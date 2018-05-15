/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import lombok.Data;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.aopalliance.aop.Advice;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.mockito.Mockito;
import org.springframework.aop.Advisor;
import org.springframework.aop.framework.Advised;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
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
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.data.util.Version;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
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

	public @Rule ExpectedException exception = ExpectedException.none();

	MongoTemplate template;
	SessionBoundMongoTemplate sessionBoundTemplate;
	ClientSession session;
	volatile List<MongoCollection<Document>> spiedCollections = new ArrayList<>();
	volatile List<MongoDatabase> spiedDatabases = new ArrayList<>();

	@Before
	public void setUp() {

		MongoClient client = new MongoClient();

		MongoDbFactory factory = new SimpleMongoDbFactory(client, "session-bound-mongo-template-tests") {

			@Override
			public MongoDatabase getDb() throws DataAccessException {

				MongoDatabase spiedDatabse = Mockito.spy(super.getDb());
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

		exception.expect(ClientSessionException.class);

		Person person = new Person("Kylar Stern");

		template.save(person);

		WithDbRef wdr = new WithDbRef();
		wdr.id = "id-1";
		wdr.personRef = person;

		template.save(wdr);

		session.close();

		sessionBoundTemplate.findById(wdr.id, WithDbRef.class);
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

		exception.expect(LazyLoadingException.class);
		exception.expectMessage("Invalid session state");

		Person person = new Person("Kylar Stern");

		template.save(person);

		WithLazyDbRef wdr = new WithLazyDbRef();
		wdr.id = "id-1";
		wdr.personRef = person;

		template.save(wdr);

		WithLazyDbRef result = null;
		try {
			result = sessionBoundTemplate.findById(wdr.id, WithLazyDbRef.class);
		} catch (Exception e) {
			fail("Someting went wrong, seems the session was already closed when reading.", e);
		}

		session.close(); // now close the session

		assertThat(result.getPersonRef()).isEqualTo(person); // resolve the lazy loading proxy
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
		converter.afterPropertiesSet();

		return converter;
	}
}
