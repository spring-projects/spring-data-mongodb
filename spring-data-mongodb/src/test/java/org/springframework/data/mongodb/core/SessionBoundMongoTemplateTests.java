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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate.SessionBoundMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.session.ClientSession;

/**
 * Integration tests for {@link SessionBoundMongoTemplate} operating up an active {@link ClientSession}.
 *
 * @author Christoph Strobl
 */
public class SessionBoundMongoTemplateTests {

	SessionBoundMongoTemplate template;
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

		this.template = new SessionBoundMongoTemplate(session, new MongoTemplate(factory, getDefaultMongoConverter(factory))) {

			@Override
			protected MongoCollection<Document> prepareCollection(MongoCollection<Document> collection) {

				MongoCollection<Document> spiedCollection = Mockito.spy(collection);
				spiedCollections.add(spiedCollection);
				return super.prepareCollection(spiedCollection);
			}
		};
	}

	@After
	public void tearDown() {
		session.close();
	}

	@Test // DATAMONGO-1880
	public void findDelegatesToMethodWithSession() {

		template.find(new Query(), Person.class);

		verify(operation(0)).find(eq(session), any(), any());
	}

	@Test // DATAMONGO-1880
	public void fluentFindDelegatesToMethodWithSession() {

		template.query(Person.class).all();

		verify(operation(0)).find(eq(session), any(), any());
	}

	@Test // DATAMONGO-1880
	public void aggregateDelegatesToMethoddWithSession() {

		template.aggregate(Aggregation.newAggregation(Aggregation.project("firstName")), Person.class, Person.class);

		verify(operation(0)).aggregate(eq(session), any(), any());
	}

	@Test // DATAMONGO-1880
	public void collectionExistsDelegatesToMethodWithSession() {

		template.collectionExists(Person.class);

		verify(command(0)).listCollectionNames(eq(session));
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
