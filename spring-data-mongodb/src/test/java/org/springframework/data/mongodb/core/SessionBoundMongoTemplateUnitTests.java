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

import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.lang.reflect.Proxy;
import java.util.Collections;

import org.bson.Document;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.MongoTemplate.SessionBoundMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.UpdateOptions;

/**
 * Unit test for {@link SessionBoundMongoTemplate} making sure a proxied {@link MongoCollection} and
 * {@link MongoDatabase} is used for executing high level commands like {@link MongoOperations#find(Query, Class)}
 * provided by Spring Data. Those commands simply handing over MongoDB base types for interaction like when obtaining a
 * {@link MongoCollection} via {@link MongoOperations#getCollection(String)} shall not be proxied as the user can
 * control the behavior by using the methods dedicated for {@link ClientSession} directly.
 *
 * @author Christoph Strobl
 * @author Jens Schauder
 */
@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.Silent.class)
public class SessionBoundMongoTemplateUnitTests {

	private static final String COLLECTION_NAME = "collection-1";

	SessionBoundMongoTemplate template;

	MongoDbFactory factory;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS) MongoCollection collection;
	@Mock MongoDatabase database;
	@Mock MongoClient client;
	@Mock ClientSession clientSession;
	@Mock FindIterable findIterable;
	@Mock MongoIterable mongoIterable;
	@Mock DistinctIterable distinctIterable;
	@Mock AggregateIterable aggregateIterable;
	@Mock MapReduceIterable mapReduceIterable;
	@Mock MongoCursor cursor;
	@Mock CodecRegistry codecRegistry;

	MappingMongoConverter converter;
	MongoMappingContext mappingContext;

	@Before
	public void setUp() {

		when(client.getDatabase(anyString())).thenReturn(database);
		when(codecRegistry.get(any(Class.class))).thenReturn(new BsonValueCodec());
		when(database.getCodecRegistry()).thenReturn(codecRegistry);
		when(database.getCollection(anyString(), any())).thenReturn(collection);
		when(database.listCollectionNames(any(ClientSession.class))).thenReturn(mongoIterable);
		when(collection.find(any(ClientSession.class), any(), any())).thenReturn(findIterable);
		when(collection.aggregate(any(ClientSession.class), anyList(), any())).thenReturn(aggregateIterable);
		when(collection.distinct(any(ClientSession.class), any(), any(), any())).thenReturn(distinctIterable);
		when(collection.mapReduce(any(ClientSession.class), any(), any(), any())).thenReturn(mapReduceIterable);
		when(findIterable.iterator()).thenReturn(cursor);
		when(aggregateIterable.collation(any())).thenReturn(aggregateIterable);
		when(aggregateIterable.allowDiskUse(anyBoolean())).thenReturn(aggregateIterable);
		when(aggregateIterable.batchSize(anyInt())).thenReturn(aggregateIterable);
		when(aggregateIterable.map(any())).thenReturn(aggregateIterable);
		when(aggregateIterable.into(any())).thenReturn(Collections.emptyList());
		when(mongoIterable.iterator()).thenReturn(cursor);
		when(distinctIterable.map(any())).thenReturn(distinctIterable);
		when(distinctIterable.into(any())).thenReturn(Collections.emptyList());
		when(mapReduceIterable.sort(any())).thenReturn(mapReduceIterable);
		when(mapReduceIterable.filter(any())).thenReturn(mapReduceIterable);
		when(mapReduceIterable.map(any())).thenReturn(mapReduceIterable);
		when(mapReduceIterable.iterator()).thenReturn(cursor);
		when(cursor.hasNext()).thenReturn(false);
		when(findIterable.projection(any())).thenReturn(findIterable);

		factory = new SimpleMongoClientDbFactory(client, "foo");

		this.mappingContext = new MongoMappingContext();
		this.converter = new MappingMongoConverter(new DefaultDbRefResolver(factory), mappingContext);
		this.template = new SessionBoundMongoTemplate(clientSession, new MongoTemplate(factory, converter));
	}

	@Test // DATAMONGO-1880
	public void executeUsesProxiedCollectionInCallback() {

		template.execute("collection", MongoCollection::find);

		verify(collection, never()).find();
		verify(collection).find(eq(clientSession));
	}

	@Test // DATAMONGO-1880
	public void executeUsesProxiedDatabaseInCallback() {

		template.execute(MongoDatabase::listCollectionNames);

		verify(database, never()).listCollectionNames();
		verify(database).listCollectionNames(eq(clientSession));
	}

	@Test // DATAMONGO-1880
	public void findOneUsesProxiedCollection() {

		template.findOne(new Query(), Person.class);

		verify(collection).find(eq(clientSession), any(), any());
	}

	@Test // DATAMONGO-1880
	public void findShouldUseProxiedCollection() {

		template.find(new Query(), Person.class);

		verify(collection).find(eq(clientSession), any(), any());
	}

	@Test // DATAMONGO-1880
	public void findAllShouldUseProxiedCollection() {

		template.findAll(Person.class);

		verify(collection).find(eq(clientSession), any(), any());
	}

	@Test // DATAMONGO-1880
	public void executeCommandShouldUseProxiedDatabase() {

		template.executeCommand("{}");

		verify(database).runCommand(eq(clientSession), any(), any(Class.class));
	}

	@Test // DATAMONGO-1880
	public void removeShouldUseProxiedCollection() {

		template.remove(new Query(), Person.class);

		verify(collection).deleteMany(eq(clientSession), any(), any(DeleteOptions.class));
	}

	@Test // DATAMONGO-1880
	public void insertShouldUseProxiedCollection() {

		template.insert(new Person());

		verify(collection).insertOne(eq(clientSession), any(Document.class));
	}

	@Test // DATAMONGO-1880
	public void aggregateShouldUseProxiedCollection() {

		template.aggregate(Aggregation.newAggregation(Aggregation.project("foo")), COLLECTION_NAME, Person.class);

		verify(collection).aggregate(eq(clientSession), anyList(), eq(Document.class));
	}

	@Test // DATAMONGO-1880
	public void aggregateStreamShouldUseProxiedCollection() {

		template.aggregateStream(Aggregation.newAggregation(Aggregation.project("foo")), COLLECTION_NAME, Person.class);

		verify(collection).aggregate(eq(clientSession), anyList(), eq(Document.class));
	}

	@Test // DATAMONGO-1880
	public void collectionExistsShouldUseProxiedDatabase() {

		template.collectionExists(Person.class);

		verify(database).listCollectionNames(eq(clientSession));
	}

	@Test // DATAMONGO-1880
	public void countShouldUseProxiedCollection() {

		template.count(new Query(), Person.class);

		verify(collection).countDocuments(eq(clientSession), any(), any(CountOptions.class));
	}

	@Test // DATAMONGO-1880
	public void createCollectionShouldUseProxiedDatabase() {

		template.createCollection(Person.class);

		verify(database).createCollection(eq(clientSession), anyString(), any());
	}

	@Test // DATAMONGO-1880
	public void dropShouldUseProxiedCollection() {

		template.dropCollection(Person.class);

		verify(collection).drop(eq(clientSession));
	}

	@Test // DATAMONGO-1880
	public void findAndModifyShouldUseProxiedCollection() {

		template.findAndModify(new Query(), new Update().set("foo", "bar"), Person.class);

		verify(collection).findOneAndUpdate(eq(clientSession), any(), any(Bson.class), any(FindOneAndUpdateOptions.class));
	}

	@Test // DATAMONGO-1880
	public void findDistinctShouldUseProxiedCollection() {

		template.findDistinct(new Query(), "firstName", Person.class, String.class);

		verify(collection).distinct(eq(clientSession), anyString(), any(), any());
	}

	@Test // DATAMONGO-1880, DATAMONGO-2264
	public void geoNearShouldUseProxiedDatabase() {

		when(database.runCommand(any(ClientSession.class), any(), eq(Document.class)))
				.thenReturn(new Document("results", Collections.emptyList()));
		template.geoNear(NearQuery.near(new Point(0, 0), Metrics.NEUTRAL), Person.class);

		verify(collection).aggregate(eq(clientSession), anyList(), eq(Document.class));
	}

	@Test // DATAMONGO-1880
	public void groupShouldUseProxiedDatabase() {

		when(database.runCommand(any(ClientSession.class), any(), eq(Document.class)))
				.thenReturn(new Document("retval", Collections.emptyList()));

		template.group(COLLECTION_NAME, GroupBy.key("firstName"), Person.class);

		verify(database).runCommand(eq(clientSession), any(), eq(Document.class));
	}

	@Test // DATAMONGO-1880
	public void mapReduceShouldUseProxiedCollection() {

		template.mapReduce(COLLECTION_NAME, "foo", "bar", Person.class);

		verify(collection).mapReduce(eq(clientSession), anyString(), anyString(), eq(Document.class));
	}

	@Test // DATAMONGO-1880
	public void streamShouldUseProxiedCollection() {

		template.stream(new Query(), Person.class);

		verify(collection).find(eq(clientSession), any(), eq(Document.class));
	}

	@Test // DATAMONGO-1880
	public void updateFirstShouldUseProxiedCollection() {

		template.updateFirst(new Query(), Update.update("foo", "bar"), Person.class);

		verify(collection).updateOne(eq(clientSession), any(), any(Bson.class), any(UpdateOptions.class));
	}

	@Test // DATAMONGO-1880
	public void updateMultiShouldUseProxiedCollection() {

		template.updateMulti(new Query(), Update.update("foo", "bar"), Person.class);

		verify(collection).updateMany(eq(clientSession), any(), any(Bson.class), any(UpdateOptions.class));
	}

	@Test // DATAMONGO-1880
	public void upsertShouldUseProxiedCollection() {

		template.upsert(new Query(), Update.update("foo", "bar"), Person.class);

		verify(collection).updateOne(eq(clientSession), any(), any(Bson.class), any(UpdateOptions.class));
	}

	@Test // DATAMONGO-1880
	public void getCollectionShouldShouldJustReturnTheCollection/*No ClientSession binding*/() {
		assertThat(template.getCollection(COLLECTION_NAME)).isNotInstanceOf(Proxy.class);
	}

	@Test // DATAMONGO-1880
	public void getDbShouldJustReturnTheDatabase/*No ClientSession binding*/() {
		assertThat(template.getDb()).isNotInstanceOf(Proxy.class);
	}

	@Test // DATAMONGO-1880
	public void indexOpsShouldUseProxiedCollection() {

		template.indexOps(COLLECTION_NAME).dropIndex("index-name");

		verify(collection).dropIndex(eq(clientSession), eq("index-name"));
	}

	@Test // DATAMONGO-1880
	public void bulkOpsShouldUseProxiedCollection() {

		BulkOperations bulkOps = template.bulkOps(BulkMode.ORDERED, COLLECTION_NAME);
		bulkOps.insert(new Document());

		bulkOps.execute();

		verify(collection).bulkWrite(eq(clientSession), anyList(), any());
	}

	@Test // DATAMONGO-1880
	public void scriptOpsShouldUseProxiedDatabase() {

		when(database.runCommand(eq(clientSession), any())).thenReturn(new Document("retval", new Object()));
		template.scriptOps().call("W-O-P-R");

		verify(database).runCommand(eq(clientSession), any());
	}
}
