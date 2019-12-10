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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;

import java.lang.reflect.Proxy;

import org.bson.Document;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Publisher;

import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate.ReactiveSessionBoundMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Unit tests for {@link ReactiveSessionBoundMongoTemplate}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.Silent.class)
public class ReactiveSessionBoundMongoTemplateUnitTests {

	private static final String COLLECTION_NAME = "collection-1";

	ReactiveSessionBoundMongoTemplate template;
	MongoMappingContext mappingContext;
	MappingMongoConverter converter;

	ReactiveMongoDatabaseFactory factory;

	@Mock MongoCollection collection;
	@Mock MongoDatabase database;
	@Mock ClientSession clientSession;
	@Mock FindPublisher findPublisher;
	@Mock AggregatePublisher aggregatePublisher;
	@Mock DistinctPublisher distinctPublisher;
	@Mock Publisher resultPublisher;
	@Mock MapReducePublisher mapReducePublisher;
	@Mock MongoClient client;
	@Mock CodecRegistry codecRegistry;

	@Before
	public void setUp() {

		when(client.getDatabase(anyString())).thenReturn(database);
		when(codecRegistry.get(any(Class.class))).thenReturn(new BsonValueCodec());
		when(database.getCodecRegistry()).thenReturn(codecRegistry);
		when(database.getCollection(anyString())).thenReturn(collection);
		when(database.getCollection(anyString(), any())).thenReturn(collection);
		when(database.listCollectionNames(any(ClientSession.class))).thenReturn(findPublisher);
		when(database.createCollection(any(ClientSession.class), any(), any())).thenReturn(resultPublisher);
		when(database.runCommand(any(ClientSession.class), any(), any(Class.class))).thenReturn(resultPublisher);
		when(collection.find(any(ClientSession.class))).thenReturn(findPublisher);
		when(collection.find(any(ClientSession.class), any(Document.class))).thenReturn(findPublisher);
		when(collection.find(any(ClientSession.class), any(Class.class))).thenReturn(findPublisher);
		when(collection.find(any(ClientSession.class), any(), any())).thenReturn(findPublisher);
		when(collection.deleteMany(any(ClientSession.class), any(), any())).thenReturn(resultPublisher);
		when(collection.insertOne(any(ClientSession.class), any(Document.class))).thenReturn(resultPublisher);
		when(collection.aggregate(any(ClientSession.class), anyList(), any(Class.class))).thenReturn(aggregatePublisher);
		when(collection.countDocuments(any(ClientSession.class), any(), any(CountOptions.class))).thenReturn(resultPublisher);
		when(collection.drop(any(ClientSession.class))).thenReturn(resultPublisher);
		when(collection.findOneAndUpdate(any(ClientSession.class), any(), any(Bson.class), any()))
				.thenReturn(resultPublisher);
		when(collection.distinct(any(ClientSession.class), any(), any(Bson.class), any())).thenReturn(distinctPublisher);
		when(collection.updateOne(any(ClientSession.class), any(), any(Bson.class), any(UpdateOptions.class)))
				.thenReturn(resultPublisher);
		when(collection.updateMany(any(ClientSession.class), any(), any(Bson.class), any(UpdateOptions.class)))
				.thenReturn(resultPublisher);
		when(collection.dropIndex(any(ClientSession.class), anyString())).thenReturn(resultPublisher);
		when(collection.mapReduce(any(ClientSession.class), any(), any(), any())).thenReturn(mapReducePublisher);
		when(findPublisher.projection(any())).thenReturn(findPublisher);
		when(findPublisher.limit(anyInt())).thenReturn(findPublisher);
		when(findPublisher.collation(any())).thenReturn(findPublisher);
		when(findPublisher.first()).thenReturn(resultPublisher);
		when(aggregatePublisher.allowDiskUse(anyBoolean())).thenReturn(aggregatePublisher);

		factory = new SimpleReactiveMongoDatabaseFactory(client, "foo");

		this.mappingContext = new MongoMappingContext();
		this.converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		this.template = new ReactiveSessionBoundMongoTemplate(clientSession, new ReactiveMongoTemplate(factory, converter));
	}

	@Test // DATAMONGO-1880
	public void executeUsesProxiedCollectionInCallback() {

		template.execute("collection", MongoCollection::find).subscribe();

		verify(collection, never()).find();
		verify(collection).find(eq(clientSession));
	}

	@Test // DATAMONGO-1880
	public void executeUsesProxiedDatabaseInCallback() {

		template.execute(MongoDatabase::listCollectionNames).subscribe();

		verify(database, never()).listCollectionNames();
		verify(database).listCollectionNames(eq(clientSession));
	}

	@Test // DATAMONGO-1880
	public void findOneUsesProxiedCollection() {

		template.findOne(new Query(), Person.class).subscribe();

		verify(collection).find(eq(clientSession), any(), any());
	}

	@Test // DATAMONGO-1880
	public void findShouldUseProxiedCollection() {

		template.find(new Query(), Person.class).subscribe();

		verify(collection).find(eq(clientSession), any(Class.class));
	}

	@Test // DATAMONGO-1880
	public void findAllShouldUseProxiedCollection() {

		template.findAll(Person.class).subscribe();

		verify(collection).find(eq(clientSession), eq(Document.class));
	}

	@Test // DATAMONGO-1880
	public void executeCommandShouldUseProxiedDatabase() {

		template.executeCommand("{}").subscribe();

		verify(database).runCommand(eq(clientSession), any(), any(Class.class));
	}

	@Test // DATAMONGO-1880
	public void removeShouldUseProxiedCollection() {

		template.remove(new Query(), Person.class).subscribe();

		verify(collection).deleteMany(eq(clientSession), any(), any(DeleteOptions.class));
	}

	@Test // DATAMONGO-1880
	public void insertShouldUseProxiedCollection() {

		template.insert(new Person()).subscribe();

		verify(collection).insertOne(eq(clientSession), any(Document.class));
	}

	@Test // DATAMONGO-1880
	public void aggregateShouldUseProxiedCollection() {

		template.aggregate(Aggregation.newAggregation(Aggregation.project("foo")), COLLECTION_NAME, Person.class)
				.subscribe();

		verify(collection).aggregate(eq(clientSession), anyList(), eq(Document.class));
	}

	@Test // DATAMONGO-1880
	public void collectionExistsShouldUseProxiedDatabase() {

		template.collectionExists(Person.class).subscribe();

		verify(database).listCollectionNames(eq(clientSession));
	}

	@Test // DATAMONGO-1880
	public void countShouldUseProxiedCollection() {

		template.count(new Query(), Person.class).subscribe();

		verify(collection).countDocuments(eq(clientSession), any(), any(CountOptions.class));
	}

	@Test // DATAMONGO-1880
	public void createCollectionShouldUseProxiedDatabase() {

		template.createCollection(Person.class).subscribe();

		verify(database).createCollection(eq(clientSession), anyString(), any());
	}

	@Test // DATAMONGO-1880
	public void dropShouldUseProxiedCollection() {

		template.dropCollection(Person.class).subscribe();

		verify(collection).drop(eq(clientSession));
	}

	@Test // DATAMONGO-1880
	public void findAndModifyShouldUseProxiedCollection() {

		template.findAndModify(new Query(), new Update().set("foo", "bar"), Person.class).subscribe();

		verify(collection).findOneAndUpdate(eq(clientSession), any(), any(Bson.class), any(FindOneAndUpdateOptions.class));
	}

	@Test // DATAMONGO-1880
	public void findDistinctShouldUseProxiedCollection() {

		template.findDistinct(new Query(), "firstName", Person.class, String.class).subscribe();

		verify(collection).distinct(eq(clientSession), anyString(), any(), any());
	}

	@Test // DATAMONGO-1880, DATAMONGO-2264
	public void geoNearShouldUseProxiedDatabase() {

		template.geoNear(NearQuery.near(new Point(0, 0), Metrics.NEUTRAL), Person.class).subscribe();

		verify(collection).aggregate(eq(clientSession), anyList(), eq(Document.class));
	}

	@Test // DATAMONGO-1880, DATAMONGO-1890, DATAMONGO-257
	public void mapReduceShouldUseProxiedCollection() {

		template.mapReduce(new BasicQuery("{}"), Person.class, COLLECTION_NAME, Person.class, "foo", "bar",
				MapReduceOptions.options()).subscribe();

		verify(collection).mapReduce(eq(clientSession), anyString(), anyString(), eq(Document.class));
	}

	@Test // DATAMONGO-1880
	public void updateFirstShouldUseProxiedCollection() {

		template.updateFirst(new Query(), Update.update("foo", "bar"), Person.class).subscribe();

		verify(collection).updateOne(eq(clientSession), any(), any(Bson.class), any(UpdateOptions.class));
	}

	@Test // DATAMONGO-1880
	public void updateMultiShouldUseProxiedCollection() {

		template.updateMulti(new Query(), Update.update("foo", "bar"), Person.class).subscribe();

		verify(collection).updateMany(eq(clientSession), any(), any(Bson.class), any(UpdateOptions.class));
	}

	@Test // DATAMONGO-1880
	public void upsertShouldUseProxiedCollection() {

		template.upsert(new Query(), Update.update("foo", "bar"), Person.class).subscribe();

		verify(collection).updateOne(eq(clientSession), any(), any(Bson.class), any(UpdateOptions.class));
	}

	@Test // DATAMONGO-1880
	public void getCollectionShouldShouldJustReturnTheCollection/*No ClientSession binding*/() {
		assertThat(template.getCollection(COLLECTION_NAME)).isNotInstanceOf(Proxy.class);
	}

	@Test // DATAMONGO-1880
	public void getDbShouldJustReturnTheDatabase/*No ClientSession binding*/() {
		assertThat(template.getMongoDatabase()).isNotInstanceOf(Proxy.class);
	}

	@Test // DATAMONGO-1880
	public void indexOpsShouldUseProxiedCollection() {

		template.indexOps(COLLECTION_NAME).dropIndex("index-name").subscribe();

		verify(collection).dropIndex(eq(clientSession), eq("index-name"));
	}
}
