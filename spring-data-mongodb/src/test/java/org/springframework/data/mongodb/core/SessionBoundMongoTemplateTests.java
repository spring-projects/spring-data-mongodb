/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.aopalliance.aop.Advice;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;
import org.springframework.aop.Advisor;
import org.springframework.aop.framework.Advised;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.ClientSessionException;
import org.springframework.data.mongodb.LazyLoadingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.SessionAwareMethodInterceptor;
import org.springframework.data.mongodb.core.MongoTemplate.SessionBoundMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.data.util.Version;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Integration tests for {@link SessionBoundMongoTemplate} operating up an active {@link ClientSession}.
 *
 * @author Christoph Strobl
 */
public class SessionBoundMongoTemplateTests {

	public static @ClassRule MongoVersionRule REQUIRES_AT_LEAST_3_6_0 = MongoVersionRule.atLeast(Version.parse("3.6.0"));
	public static @ClassRule TestRule replSet = ReplicaSet.required();

	MongoClient client;
	MongoTemplate template;
	SessionBoundMongoTemplate sessionBoundTemplate;
	ClientSession session;
	volatile List<MongoCollection<Document>> spiedCollections = new ArrayList<>();
	volatile List<MongoDatabase> spiedDatabases = new ArrayList<>();

	@Before
	public void setUp() {

		client = MongoTestUtils.replSetClient();

		MongoDbFactory factory = new SimpleMongoClientDbFactory(client, "session-bound-mongo-template-tests") {

			@Override
			public MongoDatabase getMongoDatabase() throws DataAccessException {

				MongoDatabase spiedDatabse = Mockito.spy(super.getMongoDatabase());
				spiedDatabases.add(spiedDatabse);
				return spiedDatabse;
			}
		};

		session = client.startSession(ClientSessionOptions.builder().build());

		this.template = new MongoTemplate(factory);

		this.sessionBoundTemplate = new SessionBoundMongoTemplate(session,
				new MongoTemplate(factory, getDefaultMongoConverter(factory))) {

			@Override
			protected MongoCollection<Document> prepareCollection(MongoCollection<Document> collection) {

				injectCollectionSpy(collection);

				return super.prepareCollection(collection);
			}

			@SuppressWarnings({ "ConstantConditions", "unchecked" })
			private void injectCollectionSpy(MongoCollection<Document> collection) {

				InvocationHandler handler = Proxy.getInvocationHandler(collection);

				Advised advised = (Advised) ReflectionTestUtils.getField(handler, "advised");

				for (Advisor advisor : advised.getAdvisors()) {
					Advice advice = advisor.getAdvice();
					if (advice instanceof SessionAwareMethodInterceptor) {

						MongoCollection<Document> spiedCollection = Mockito
								.spy((MongoCollection<Document>) ReflectionTestUtils.getField(advice, "target"));
						spiedCollections.add(spiedCollection);

						ReflectionTestUtils.setField(advice, "target", spiedCollection);
					}
				}
			}
		};
	}

	@After
	public void tearDown() {

		session.close();
	}

	@Test // DATAMONGO-1880
	public void findDelegatesToMethodWithSession() {

		sessionBoundTemplate.find(new Query(), Person.class);

		verify(operation(0)).find(eq(session), any(), any());
	}

	@Test // DATAMONGO-1880
	public void fluentFindDelegatesToMethodWithSession() {

		sessionBoundTemplate.query(Person.class).all();

		verify(operation(0)).find(eq(session), any(), any());
	}

	@Test // DATAMONGO-1880
	public void aggregateDelegatesToMethoddWithSession() {

		sessionBoundTemplate.aggregate(Aggregation.newAggregation(Aggregation.project("firstName")), Person.class,
				Person.class);

		verify(operation(0)).aggregate(eq(session), any(), any());
	}

	@Test // DATAMONGO-1880
	public void collectionExistsDelegatesToMethodWithSession() {

		sessionBoundTemplate.collectionExists(Person.class);

		verify(command(0)).listCollectionNames(eq(session));
	}

	@Test // DATAMONGO-1880
	public void shouldLoadDbRefWhenSessionIsActive() {

		Person person = new Person("Kylar Stern");

		template.save(person);

		WithDbRef wdr = new WithDbRef();
		wdr.id = "id-1";
		wdr.personRef = person;

		template.save(wdr);

		WithDbRef result = sessionBoundTemplate.findById(wdr.id, WithDbRef.class);

		assertThat(result.personRef).isEqualTo(person);
	}

	@Test // DATAMONGO-1880
	public void shouldErrorOnLoadDbRefWhenSessionIsClosed() {

		Person person = new Person("Kylar Stern");

		template.save(person);

		WithDbRef wdr = new WithDbRef();
		wdr.id = "id-1";
		wdr.personRef = person;

		template.save(wdr);

		session.close();

		assertThatExceptionOfType(ClientSessionException.class)
				.isThrownBy(() -> sessionBoundTemplate.findById(wdr.id, WithDbRef.class));
	}

	@Test // DATAMONGO-1880
	public void shouldLoadLazyDbRefWhenSessionIsActive() {

		Person person = new Person("Kylar Stern");

		template.save(person);

		WithLazyDbRef wdr = new WithLazyDbRef();
		wdr.id = "id-1";
		wdr.personRef = person;

		template.save(wdr);

		WithLazyDbRef result = sessionBoundTemplate.findById(wdr.id, WithLazyDbRef.class);

		assertThat(result.getPersonRef()).isEqualTo(person);
	}

	@Test // DATAMONGO-1880
	public void shouldErrorOnLoadLazyDbRefWhenSessionIsClosed() {

		Person person = new Person("Kylar Stern");

		template.save(person);

		WithLazyDbRef wdr = new WithLazyDbRef();
		wdr.id = "id-1";
		wdr.personRef = person;

		template.save(wdr);

		WithLazyDbRef result = sessionBoundTemplate.findById(wdr.id, WithLazyDbRef.class);

		session.close(); // now close the session

		assertThatExceptionOfType(LazyLoadingException.class).isThrownBy(() -> result.getPersonRef().toString());
	}

	@Test // DATAMONGO-2001
	public void countShouldWorkInTransactions() {

		if (!template.collectionExists(Person.class)) {
			template.createCollection(Person.class);
		} else {
			template.remove(Person.class).all();
		}

		ClientSession session = client.startSession();
		session.startTransaction();

		MongoTemplate sessionBound = template.withSession(session);

		sessionBound.save(new Person("Kylar Stern"));

		assertThat(sessionBound.query(Person.class).matching(query(where("firstName").is("foobar"))).count()).isZero();
		assertThat(sessionBound.query(Person.class).matching(query(where("firstName").is("Kylar Stern"))).count()).isOne();
		assertThat(sessionBound.query(Person.class).count()).isOne();

		session.commitTransaction();
		session.close();
	}

	@Test // DATAMONGO-2012
	public void countWithGeoInTransaction() {

		if (!template.collectionExists(Person.class)) {
			template.createCollection(Person.class);
			template.indexOps(Person.class).ensureIndex(new GeospatialIndex("location"));
		} else {
			template.remove(Person.class).all();
		}

		ClientSession session = client.startSession();
		session.startTransaction();

		MongoTemplate sessionBound = template.withSession(session);

		sessionBound.save(new Person("Kylar Stern"));

		assertThat(sessionBound.query(Person.class).matching(query(where("location").near(new Point(1, 0)))).count())
				.isZero();

		session.commitTransaction();
		session.close();
	}

	@Test // DATAMONGO-2001
	public void countShouldReturnIsolatedCount() throws InterruptedException {

		if (!template.collectionExists(Person.class)) {
			template.createCollection(Person.class);
		} else {
			template.remove(Person.class).all();
		}

		int nrThreads = 2;
		CountDownLatch savedInTransaction = new CountDownLatch(nrThreads);
		CountDownLatch beforeCommit = new CountDownLatch(nrThreads);
		List<Object> resultList = new CopyOnWriteArrayList<>();

		Runnable runnable = () -> {

			ClientSession session = client.startSession();
			session.startTransaction();

			try {
				MongoTemplate sessionBound = template.withSession(session);

				try {
					sessionBound.save(new Person("Kylar Stern"));
				} finally {
					savedInTransaction.countDown();
				}

				savedInTransaction.await(1, TimeUnit.SECONDS);

				try {
					resultList.add(sessionBound.query(Person.class).count());
				} finally {
					beforeCommit.countDown();
				}

				beforeCommit.await(1, TimeUnit.SECONDS);
			} catch (Exception e) {
				resultList.add(e);
			}

			session.commitTransaction();
			session.close();
		};

		List<Thread> threads = IntStream.range(0, nrThreads) //
				.mapToObj(i -> new Thread(runnable)) //
				.peek(Thread::start) //
				.collect(Collectors.toList());

		for (Thread thread : threads) {
			thread.join();
		}

		assertThat(template.query(Person.class).count()).isEqualTo(2L);
		assertThat(resultList).hasSize(nrThreads).allMatch(it -> it.equals(1L));
	}

	@Data
	static class WithDbRef {

		@Id String id;
		@DBRef Person personRef;
	}

	@Data
	static class WithLazyDbRef {

		@Id String id;
		@DBRef(lazy = true) Person personRef;

		public Person getPersonRef() {
			return personRef;
		}
	}

	// --> Just some helpers for testing

	MongoCollection<Document> operation(int index) {
		return spiedCollections.get(index);
	}

	MongoDatabase command(int index) {
		return spiedDatabases.get(index);
	}

	private MongoConverter getDefaultMongoConverter(MongoDbFactory factory) {

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
		MongoCustomConversions conversions = new MongoCustomConversions(Collections.emptyList());

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.afterPropertiesSet();

		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		converter.setCustomConversions(conversions);
		converter.setCodecRegistryProvider(factory);
		converter.afterPropertiesSet();

		return converter;
	}
}
