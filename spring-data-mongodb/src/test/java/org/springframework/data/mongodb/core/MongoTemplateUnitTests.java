/*
 * Copyright 2010-2019 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import lombok.Data;

import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreator;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.IsBsonObject;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.MapReduceAction;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * Unit tests for {@link MongoTemplate}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoTemplateUnitTests extends MongoOperationsUnitTests {

	MongoTemplate template;

	@Mock MongoDbFactory factory;
	@Mock MongoClient mongo;
	@Mock MongoDatabase db;
	@Mock MongoCollection<Document> collection;
	@Mock MongoCollection<Document> collectionWithWriteConcern;
	@Mock MongoCursor<Document> cursor;
	@Mock FindIterable<Document> findIterable;
	@Mock AggregateIterable aggregateIterable;
	@Mock MapReduceIterable mapReduceIterable;
	@Mock DistinctIterable distinctIterable;
	@Mock UpdateResult updateResult;
	@Mock DeleteResult deleteResult;

	Document commandResultDocument = new Document();

	MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	MappingMongoConverter converter;
	MongoMappingContext mappingContext;

	@Before
	public void setUp() {

		when(findIterable.iterator()).thenReturn(cursor);
		when(factory.getDb()).thenReturn(db);
		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslator);
		when(db.getCollection(any(String.class), eq(Document.class))).thenReturn(collection);
		when(db.runCommand(any(), any(Class.class))).thenReturn(commandResultDocument);
		when(collection.find(any(org.bson.Document.class), any(Class.class))).thenReturn(findIterable);
		when(collection.mapReduce(any(), any(), eq(Document.class))).thenReturn(mapReduceIterable);
		when(collection.count(any(Bson.class), any(CountOptions.class))).thenReturn(1L); // TODO: MongoDB 4 - fix me
		when(collection.getNamespace()).thenReturn(new MongoNamespace("db.mock-collection"));
		when(collection.aggregate(any(List.class), any())).thenReturn(aggregateIterable);
		when(collection.withReadPreference(any())).thenReturn(collection);
		when(collection.replaceOne(any(), any(), any(ReplaceOptions.class))).thenReturn(updateResult);
		when(collection.withWriteConcern(any())).thenReturn(collectionWithWriteConcern);
		when(collection.distinct(anyString(), any(Document.class), any())).thenReturn(distinctIterable);
		when(collectionWithWriteConcern.deleteOne(any(Bson.class), any())).thenReturn(deleteResult);
		when(findIterable.projection(any())).thenReturn(findIterable);
		when(findIterable.sort(any(org.bson.Document.class))).thenReturn(findIterable);
		when(findIterable.collation(any())).thenReturn(findIterable);
		when(findIterable.limit(anyInt())).thenReturn(findIterable);
		when(mapReduceIterable.collation(any())).thenReturn(mapReduceIterable);
		when(mapReduceIterable.sort(any())).thenReturn(mapReduceIterable);
		when(mapReduceIterable.iterator()).thenReturn(cursor);
		when(mapReduceIterable.filter(any())).thenReturn(mapReduceIterable);
		when(mapReduceIterable.collectionName(any())).thenReturn(mapReduceIterable);
		when(mapReduceIterable.databaseName(any())).thenReturn(mapReduceIterable);
		when(mapReduceIterable.action(any())).thenReturn(mapReduceIterable);
		when(aggregateIterable.collation(any())).thenReturn(aggregateIterable);
		when(aggregateIterable.allowDiskUse(any())).thenReturn(aggregateIterable);
		when(aggregateIterable.batchSize(anyInt())).thenReturn(aggregateIterable);
		when(aggregateIterable.map(any())).thenReturn(aggregateIterable);
		when(aggregateIterable.into(any())).thenReturn(Collections.emptyList());
		when(distinctIterable.collation(any())).thenReturn(distinctIterable);
		when(distinctIterable.map(any())).thenReturn(distinctIterable);
		when(distinctIterable.into(any())).thenReturn(Collections.emptyList());

		this.mappingContext = new MongoMappingContext();
		mappingContext.afterPropertiesSet();

		this.converter = spy(new MappingMongoConverter(new DefaultDbRefResolver(factory), mappingContext));
		converter.afterPropertiesSet();
		this.template = new MongoTemplate(factory, converter);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullDatabaseName() throws Exception {
		new MongoTemplate(mongo, null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1968
	public void rejectsNullMongo() {
		new MongoTemplate((MongoClient) null, "database");
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1968
	public void rejectsNullMongoClient() {
		new MongoTemplate((com.mongodb.client.MongoClient) null, "database");
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1870
	public void removeHandlesMongoExceptionProperly() throws Exception {

		MongoTemplate template = mockOutGetDb();

		template.remove(null, "collection");
	}

	@Test
	public void defaultsConverterToMappingMongoConverter() throws Exception {
		MongoTemplate template = new MongoTemplate(mongo, "database");
		assertTrue(ReflectionTestUtils.getField(template, "mongoConverter") instanceof MappingMongoConverter);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void rejectsNotFoundMapReduceResource() {

		GenericApplicationContext ctx = new GenericApplicationContext();
		ctx.refresh();
		template.setApplicationContext(ctx);
		template.mapReduce("foo", "classpath:doesNotExist.js", "function() {}", Person.class);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAMONGO-322
	public void rejectsEntityWithNullIdIfNotSupportedIdType() {

		Object entity = new NotAutogenerateableId();
		template.save(entity);
	}

	@Test // DATAMONGO-322
	public void storesEntityWithSetIdAlthoughNotAutogenerateable() {

		NotAutogenerateableId entity = new NotAutogenerateableId();
		entity.id = 1;

		template.save(entity);
	}

	@Test // DATAMONGO-322
	public void autogeneratesIdForEntityWithAutogeneratableId() {

		this.converter.afterPropertiesSet();

		MongoTemplate template = spy(this.template);
		doReturn(new ObjectId()).when(template).saveDocument(any(String.class), any(Document.class), any(Class.class));

		AutogenerateableId entity = new AutogenerateableId();
		template.save(entity);

		assertThat(entity.id, is(notNullValue()));
	}

	@Test // DATAMONGO-1912
	public void autogeneratesIdForMap() {

		MongoTemplate template = spy(this.template);
		doReturn(new ObjectId()).when(template).saveDocument(any(String.class), any(Document.class), any(Class.class));

		Map<String, String> entity = new LinkedHashMap<>();
		template.save(entity, "foo");

		assertThat(entity, hasKey("_id"));
	}

	@Test // DATAMONGO-374
	public void convertsUpdateConstraintsUsingConverters() {

		CustomConversions conversions = new MongoCustomConversions(Collections.singletonList(MyConverter.INSTANCE));
		this.converter.setCustomConversions(conversions);
		this.converter.afterPropertiesSet();

		Query query = new Query();
		Update update = new Update().set("foo", new AutogenerateableId());

		template.updateFirst(query, update, Wrapper.class);

		QueryMapper queryMapper = new QueryMapper(converter);
		Document reference = queryMapper.getMappedObject(update.getUpdateObject(), Optional.empty());

		verify(collection, times(1)).updateOne(any(org.bson.Document.class), eq(reference), any(UpdateOptions.class));
	}

	@Test // DATAMONGO-474
	public void setsUnpopulatedIdField() {

		NotAutogenerateableId entity = new NotAutogenerateableId();

		template.populateIdIfNecessary(entity, 5);
		assertThat(entity.id, is(5));
	}

	@Test // DATAMONGO-474
	public void doesNotSetAlreadyPopulatedId() {

		NotAutogenerateableId entity = new NotAutogenerateableId();
		entity.id = 5;

		template.populateIdIfNecessary(entity, 7);
		assertThat(entity.id, is(5));
	}

	@Test // DATAMONGO-868
	public void findAndModifyShouldBumpVersionByOneWhenVersionFieldNotIncludedInUpdate() {

		VersionedEntity v = new VersionedEntity();
		v.id = 1;
		v.version = 0;

		ArgumentCaptor<org.bson.Document> captor = ArgumentCaptor.forClass(org.bson.Document.class);

		template.findAndModify(new Query(), new Update().set("id", "10"), VersionedEntity.class);

		verify(collection, times(1)).findOneAndUpdate(any(org.bson.Document.class), captor.capture(),
				any(FindOneAndUpdateOptions.class));
		Assert.assertThat(captor.getValue().get("$inc"), Is.is(new org.bson.Document("version", 1L)));
	}

	@Test // DATAMONGO-868
	public void findAndModifyShouldNotBumpVersionByOneWhenVersionFieldAlreadyIncludedInUpdate() {

		VersionedEntity v = new VersionedEntity();
		v.id = 1;
		v.version = 0;

		ArgumentCaptor<org.bson.Document> captor = ArgumentCaptor.forClass(org.bson.Document.class);

		template.findAndModify(new Query(), new Update().set("version", 100), VersionedEntity.class);

		verify(collection, times(1)).findOneAndUpdate(any(org.bson.Document.class), captor.capture(),
				any(FindOneAndUpdateOptions.class));

		Assert.assertThat(captor.getValue().get("$set"), Is.is(new org.bson.Document("version", 100)));
		Assert.assertThat(captor.getValue().get("$inc"), nullValue());
	}

	@Test // DATAMONGO-533
	public void registersDefaultEntityIndexCreatorIfApplicationContextHasOneForDifferentMappingContext() {

		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.getBeanFactory().registerSingleton("foo",
				new MongoPersistentEntityIndexCreator(new MongoMappingContext(), template));
		applicationContext.refresh();

		GenericApplicationContext spy = spy(applicationContext);

		MongoTemplate mongoTemplate = new MongoTemplate(factory, converter);
		mongoTemplate.setApplicationContext(spy);

		verify(spy, times(1)).addApplicationListener(argThat(new ArgumentMatcher<MongoPersistentEntityIndexCreator>() {

			@Override
			public boolean matches(MongoPersistentEntityIndexCreator argument) {
				return argument.isIndexCreatorFor(mappingContext);
			}
		}));
	}

	@Test // DATAMONGO-566
	public void findAllAndRemoveShouldRetrieveMatchingDocumentsPriorToRemoval() {

		BasicQuery query = new BasicQuery("{'foo':'bar'}");
		template.findAllAndRemove(query, VersionedEntity.class);
		verify(collection, times(1)).find(Mockito.eq(query.getQueryObject()), any(Class.class));
	}

	@Test // DATAMONGO-566
	public void findAllAndRemoveShouldRemoveDocumentsReturedByFindQuery() {

		Mockito.when(cursor.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
		Mockito.when(cursor.next()).thenReturn(new org.bson.Document("_id", Integer.valueOf(0)))
				.thenReturn(new org.bson.Document("_id", Integer.valueOf(1)));

		ArgumentCaptor<org.bson.Document> queryCaptor = ArgumentCaptor.forClass(org.bson.Document.class);
		BasicQuery query = new BasicQuery("{'foo':'bar'}");
		template.findAllAndRemove(query, VersionedEntity.class);

		verify(collection, times(1)).deleteMany(queryCaptor.capture(), any());

		Document idField = DocumentTestUtils.getAsDocument(queryCaptor.getValue(), "_id");
		assertThat((List<Object>) idField.get("$in"),
				IsIterableContainingInOrder.<Object> contains(Integer.valueOf(0), Integer.valueOf(1)));
	}

	@Test // DATAMONGO-566
	public void findAllAndRemoveShouldNotTriggerRemoveIfFindResultIsEmpty() {

		template.findAllAndRemove(new BasicQuery("{'foo':'bar'}"), VersionedEntity.class);
		verify(collection, never()).deleteMany(any(org.bson.Document.class));
	}

	@Test // DATAMONGO-948
	public void sortShouldBeTakenAsIsWhenExecutingQueryWithoutSpecificTypeInformation() {

		Query query = Query.query(Criteria.where("foo").is("bar")).with(Sort.by("foo"));
		template.executeQuery(query, "collection1", new DocumentCallbackHandler() {

			@Override
			public void processDocument(Document document) throws MongoException, DataAccessException {
				// nothing to do - just a test
			}
		});

		ArgumentCaptor<org.bson.Document> captor = ArgumentCaptor.forClass(org.bson.Document.class);

		verify(findIterable, times(1)).sort(captor.capture());
		assertThat(captor.getValue(), equalTo(new org.bson.Document("foo", 1)));
	}

	@Test // DATAMONGO-1166, DATAMONGO-1824
	public void aggregateShouldHonorReadPreferenceWhenSet() {

		template.setReadPreference(ReadPreference.secondary());

		template.aggregate(newAggregation(Aggregation.unwind("foo")), "collection-1", Wrapper.class);

		verify(collection).withReadPreference(eq(ReadPreference.secondary()));
	}

	@Test // DATAMONGO-1166, DATAMONGO-1824
	public void aggregateShouldIgnoreReadPreferenceWhenNotSet() {

		template.aggregate(newAggregation(Aggregation.unwind("foo")), "collection-1", Wrapper.class);

		verify(collection, never()).withReadPreference(any());
	}

	@Test // DATAMONGO-1166, DATAMONGO-2264
	public void geoNearShouldHonorReadPreferenceWhenSet() {

		template.setReadPreference(ReadPreference.secondary());

		NearQuery query = NearQuery.near(new Point(1, 1));
		template.geoNear(query, Wrapper.class);

		verify(collection).withReadPreference(eq(ReadPreference.secondary()));
	}

	@Test // DATAMONGO-1166, DATAMONGO-2264
	public void geoNearShouldIgnoreReadPreferenceWhenNotSet() {

		NearQuery query = NearQuery.near(new Point(1, 1));
		template.geoNear(query, Wrapper.class);

		verify(collection, never()).withReadPreference(any());
	}

	@Test // DATAMONGO-1334
	@Ignore("TODO: mongo3 - a bit hard to tests with the immutable object stuff")
	public void mapReduceShouldUseZeroAsDefaultLimit() {

		MongoCursor cursor = mock(MongoCursor.class);
		MapReduceIterable output = mock(MapReduceIterable.class);
		when(output.limit(anyInt())).thenReturn(output);
		when(output.sort(any(Document.class))).thenReturn(output);
		when(output.filter(any(Document.class))).thenReturn(output);
		when(output.iterator()).thenReturn(cursor);
		when(cursor.hasNext()).thenReturn(false);

		when(collection.mapReduce(anyString(), anyString())).thenReturn(output);

		Query query = new BasicQuery("{'foo':'bar'}");

		template.mapReduce(query, "collection", "function(){}", "function(key,values){}", Wrapper.class);

		verify(output, times(1)).limit(1);
	}

	@Test // DATAMONGO-1334
	public void mapReduceShouldPickUpLimitFromQuery() {

		MongoCursor cursor = mock(MongoCursor.class);
		MapReduceIterable output = mock(MapReduceIterable.class);
		when(output.limit(anyInt())).thenReturn(output);
		when(output.sort(any())).thenReturn(output);
		when(output.filter(any(Document.class))).thenReturn(output);
		when(output.iterator()).thenReturn(cursor);
		when(cursor.hasNext()).thenReturn(false);

		when(collection.mapReduce(anyString(), anyString(), eq(Document.class))).thenReturn(output);

		Query query = new BasicQuery("{'foo':'bar'}");
		query.limit(100);

		template.mapReduce(query, "collection", "function(){}", "function(key,values){}", Wrapper.class);

		verify(output, times(1)).limit(100);
	}

	@Test // DATAMONGO-1334
	public void mapReduceShouldPickUpLimitFromOptions() {

		MongoCursor cursor = mock(MongoCursor.class);
		MapReduceIterable output = mock(MapReduceIterable.class);
		when(output.limit(anyInt())).thenReturn(output);
		when(output.sort(any())).thenReturn(output);
		when(output.filter(any(Document.class))).thenReturn(output);
		when(output.iterator()).thenReturn(cursor);
		when(cursor.hasNext()).thenReturn(false);

		when(collection.mapReduce(anyString(), anyString(), eq(Document.class))).thenReturn(output);

		Query query = new BasicQuery("{'foo':'bar'}");

		template.mapReduce(query, "collection", "function(){}", "function(key,values){}",
				new MapReduceOptions().limit(1000), Wrapper.class);

		verify(output, times(1)).limit(1000);
	}

	@Test // DATAMONGO-1334
	public void mapReduceShouldPickUpLimitFromOptionsWhenQueryIsNotPresent() {

		MongoCursor cursor = mock(MongoCursor.class);
		MapReduceIterable output = mock(MapReduceIterable.class);
		when(output.limit(anyInt())).thenReturn(output);
		when(output.sort(any())).thenReturn(output);
		when(output.filter(any())).thenReturn(output);
		when(output.iterator()).thenReturn(cursor);
		when(cursor.hasNext()).thenReturn(false);

		when(collection.mapReduce(anyString(), anyString(), eq(Document.class))).thenReturn(output);

		template.mapReduce("collection", "function(){}", "function(key,values){}", new MapReduceOptions().limit(1000),
				Wrapper.class);

		verify(output, times(1)).limit(1000);
	}

	@Test // DATAMONGO-1334
	public void mapReduceShouldPickUpLimitFromOptionsEvenWhenQueryDefinesItDifferently() {

		MongoCursor cursor = mock(MongoCursor.class);
		MapReduceIterable output = mock(MapReduceIterable.class);
		when(output.limit(anyInt())).thenReturn(output);
		when(output.sort(any())).thenReturn(output);
		when(output.filter(any(Document.class))).thenReturn(output);
		when(output.iterator()).thenReturn(cursor);
		when(cursor.hasNext()).thenReturn(false);

		when(collection.mapReduce(anyString(), anyString(), eq(Document.class))).thenReturn(output);

		Query query = new BasicQuery("{'foo':'bar'}");
		query.limit(100);

		template.mapReduce(query, "collection", "function(){}", "function(key,values){}",
				new MapReduceOptions().limit(1000), Wrapper.class);

		verify(output, times(1)).limit(1000);
	}

	@Test // DATAMONGO-1639
	public void beforeConvertEventForUpdateSeesNextVersion() {

		when(updateResult.getModifiedCount()).thenReturn(1L);

		final VersionedEntity entity = new VersionedEntity();
		entity.id = 1;
		entity.version = 0;

		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		context.addApplicationListener(new AbstractMongoEventListener<VersionedEntity>() {

			@Override
			public void onBeforeConvert(BeforeConvertEvent<VersionedEntity> event) {
				assertThat(event.getSource().version, is(1));
			}
		});

		template.setApplicationContext(context);

		template.save(entity);
	}

	@Test // DATAMONGO-1447
	public void shouldNotAppend$isolatedToNonMulitUpdate() {

		template.updateFirst(new Query(), new Update().isolated().set("jon", "snow"), Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateOne(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat(queryCaptor.getValue(), isBsonObject().notContaining("$isolated"));
		assertThat(updateCaptor.getValue(), isBsonObject().containing("$set.jon", "snow").notContaining("$isolated"));
	}

	@Test // DATAMONGO-1447
	public void shouldAppend$isolatedToUpdateMultiEmptyQuery() {

		template.updateMulti(new Query(), new Update().isolated().set("jon", "snow"), Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateMany(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat(queryCaptor.getValue(), isBsonObject().withSize(1).containing("$isolated", 1));
		assertThat(updateCaptor.getValue(), isBsonObject().containing("$set.jon", "snow").notContaining("$isolated"));
	}

	@Test // DATAMONGO-1447
	public void shouldAppend$isolatedToUpdateMultiQueryIfNotPresentAndUpdateSetsValue() {

		Update update = new Update().isolated().set("jon", "snow");
		Query query = new BasicQuery("{'eddard':'stark'}");

		template.updateMulti(query, update, Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateMany(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat(queryCaptor.getValue(), isBsonObject().containing("$isolated", 1).containing("eddard", "stark"));
		assertThat(updateCaptor.getValue(), isBsonObject().containing("$set.jon", "snow").notContaining("$isolated"));
	}

	@Test // DATAMONGO-1447
	public void shouldNotAppend$isolatedToUpdateMultiQueryIfNotPresentAndUpdateDoesNotSetValue() {

		Update update = new Update().set("jon", "snow");
		Query query = new BasicQuery("{'eddard':'stark'}");

		template.updateMulti(query, update, Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateMany(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat(queryCaptor.getValue(), isBsonObject().notContaining("$isolated").containing("eddard", "stark"));
		assertThat(updateCaptor.getValue(), isBsonObject().containing("$set.jon", "snow").notContaining("$isolated"));
	}

	@Test // DATAMONGO-1447
	public void shouldNotOverwrite$isolatedToUpdateMultiQueryIfPresentAndUpdateDoesNotSetValue() {

		Update update = new Update().set("jon", "snow");
		Query query = new BasicQuery("{'eddard':'stark', '$isolated' : 1}");

		template.updateMulti(query, update, Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateMany(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat(queryCaptor.getValue(), isBsonObject().containing("$isolated", 1).containing("eddard", "stark"));
		assertThat(updateCaptor.getValue(), isBsonObject().containing("$set.jon", "snow").notContaining("$isolated"));
	}

	@Test // DATAMONGO-1447
	public void shouldNotOverwrite$isolatedToUpdateMultiQueryIfPresentAndUpdateSetsValue() {

		Update update = new Update().isolated().set("jon", "snow");
		Query query = new BasicQuery("{'eddard':'stark', '$isolated' : 0}");

		template.updateMulti(query, update, Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateMany(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat(queryCaptor.getValue(), isBsonObject().containing("$isolated", 0).containing("eddard", "stark"));
		assertThat(updateCaptor.getValue(), isBsonObject().containing("$set.jon", "snow").notContaining("$isolated"));
	}

	@Test // DATAMONGO-1311
	public void executeQueryShouldUseBatchSizeWhenPresent() {

		when(findIterable.batchSize(anyInt())).thenReturn(findIterable);

		Query query = new Query().cursorBatchSize(1234);
		template.find(query, Person.class);

		verify(findIterable).batchSize(1234);
	}

	@Test // DATAMONGO-1518
	public void executeQueryShouldUseCollationWhenPresent() {

		template.executeQuery(new BasicQuery("{}").collation(Collation.of("fr")), "collection-1", val -> {});

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	public void streamQueryShouldUseCollationWhenPresent() {

		template.stream(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class).next();

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	public void findShouldUseCollationWhenPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}").collation(Collation.of("fr")), AutogenerateableId.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	public void findOneShouldUseCollationWhenPresent() {

		template.findOne(new BasicQuery("{'foo' : 'bar'}").collation(Collation.of("fr")), AutogenerateableId.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	public void existsShouldUseCollationWhenPresent() {

		template.exists(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).count(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(equalTo(com.mongodb.client.model.Collation.builder().locale("fr").build())));
	}

	@Test // DATAMONGO-1518
	public void findAndModfiyShoudUseCollationWhenPresent() {

		template.findAndModify(new BasicQuery("{}").collation(Collation.of("fr")), new Update(), AutogenerateableId.class);

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-1518
	public void findAndRemoveShouldUseCollationWhenPresent() {

		template.findAndRemove(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class);

		ArgumentCaptor<FindOneAndDeleteOptions> options = ArgumentCaptor.forClass(FindOneAndDeleteOptions.class);
		verify(collection).findOneAndDelete(any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-2196
	public void removeShouldApplyWriteConcern() {

		Person person = new Person();
		person.id = "id-1";

		template.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
		template.remove(person);

		verify(collection).withWriteConcern(eq(WriteConcern.UNACKNOWLEDGED));
		verify(collectionWithWriteConcern).deleteOne(any(Bson.class), any());
	}

	@Test // DATAMONGO-1518
	public void findAndRemoveManyShouldUseCollationWhenPresent() {

		template.doRemove("collection-1", new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class,
				true);

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-1518
	public void updateOneShouldUseCollationWhenPresent() {

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), new Update().set("foo", "bar"),
				AutogenerateableId.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-1518
	public void updateManyShouldUseCollationWhenPresent() {

		template.updateMulti(new BasicQuery("{}").collation(Collation.of("fr")), new Update().set("foo", "bar"),
				AutogenerateableId.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));

	}

	@Test // DATAMONGO-1518
	public void replaceOneShouldUseCollationWhenPresent() {

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), new Update(), AutogenerateableId.class);

		ArgumentCaptor<ReplaceOptions> options = ArgumentCaptor.forClass(ReplaceOptions.class);
		verify(collection).replaceOne(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-1518, DATAMONGO-1824
	public void aggregateShouldUseCollationWhenPresent() {

		Aggregation aggregation = newAggregation(project("id"))
				.withOptions(newAggregationOptions().collation(Collation.of("fr")).build());
		template.aggregate(aggregation, AutogenerateableId.class, Document.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1824
	public void aggregateShouldUseBatchSizeWhenPresent() {

		Aggregation aggregation = newAggregation(project("id"))
				.withOptions(newAggregationOptions().collation(Collation.of("fr")).cursorBatchSize(100).build());
		template.aggregate(aggregation, AutogenerateableId.class, Document.class);

		verify(aggregateIterable).batchSize(100);
	}

	@Test // DATAMONGO-1518
	public void mapReduceShouldUseCollationWhenPresent() {

		template.mapReduce("", "", "", MapReduceOptions.options().collation(Collation.of("fr")), AutogenerateableId.class);

		verify(mapReduceIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-2027
	public void mapReduceShouldUseOutputCollectionWhenPresent() {

		template.mapReduce("", "", "", MapReduceOptions.options().outputCollection("out-collection"),
				AutogenerateableId.class);

		verify(mapReduceIterable).collectionName(eq("out-collection"));
	}

	@Test // DATAMONGO-2027
	public void mapReduceShouldNotUseOutputCollectionForInline() {

		template.mapReduce("", "", "", MapReduceOptions.options().outputCollection("out-collection").outputTypeInline(),
				AutogenerateableId.class);

		verify(mapReduceIterable, never()).collectionName(any());
	}

	@Test // DATAMONGO-2027
	public void mapReduceShouldUseOutputActionWhenPresent() {

		template.mapReduce("", "", "", MapReduceOptions.options().outputCollection("out-collection").outputTypeMerge(),
				AutogenerateableId.class);

		verify(mapReduceIterable).action(eq(MapReduceAction.MERGE));
	}

	@Test // DATAMONGO-2027
	public void mapReduceShouldUseOutputDatabaseWhenPresent() {

		template.mapReduce("", "", "",
				MapReduceOptions.options().outputDatabase("out-database").outputCollection("out-collection").outputTypeMerge(),
				AutogenerateableId.class);

		verify(mapReduceIterable).databaseName(eq("out-database"));
	}

	@Test // DATAMONGO-2027
	public void mapReduceShouldNotUseOutputDatabaseForInline() {

		template.mapReduce("", "", "", MapReduceOptions.options().outputDatabase("out-database").outputTypeInline(),
				AutogenerateableId.class);

		verify(mapReduceIterable, never()).databaseName(any());
	}

	@Test // DATAMONGO-1518, DATAMONGO-2264
	public void geoNearShouldUseCollationWhenPresent() {

		NearQuery query = NearQuery.near(0D, 0D).query(new BasicQuery("{}").collation(Collation.of("fr")));
		template.geoNear(query, AutogenerateableId.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	public void groupShouldUseCollationWhenPresent() {

		commandResultDocument.append("retval", Collections.emptySet());
		template.group("collection-1", GroupBy.key("id").reduceFunction("bar").collation(Collation.of("fr")),
				AutogenerateableId.class);

		ArgumentCaptor<Document> cmd = ArgumentCaptor.forClass(Document.class);
		verify(db).runCommand(cmd.capture(), any(Class.class));

		assertThat(cmd.getValue().get("group", Document.class).get("collation", Document.class),
				equalTo(new Document("locale", "fr")));
	}

	@Test // DATAMONGO-1880
	public void countShouldUseCollationWhenPresent() {

		template.count(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).count(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(equalTo(com.mongodb.client.model.Collation.builder().locale("fr").build())));
	}

	@Test // DATAMONGO-1733
	public void appliesFieldsWhenInterfaceProjectionIsClosedAndQueryDoesNotDefineFields() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, PersonProjection.class, null);

		verify(findIterable).projection(eq(new Document("firstname", 1)));
	}

	@Test // DATAMONGO-1733
	public void doesNotApplyFieldsWhenInterfaceProjectionIsClosedAndQueryDefinesFields() {

		template.doFind("star-wars", new Document(), new Document("bar", 1), Person.class, PersonProjection.class, null);

		verify(findIterable).projection(eq(new Document("bar", 1)));
	}

	@Test // DATAMONGO-1733
	public void doesNotApplyFieldsWhenInterfaceProjectionIsOpen() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, PersonSpELProjection.class, null);

		verify(findIterable).projection(eq(new Document()));
	}

	@Test // DATAMONGO-1733, DATAMONGO-2041
	public void appliesFieldsToDtoProjection() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, Jedi.class, null);

		verify(findIterable).projection(eq(new Document("firstname", 1)));
	}

	@Test // DATAMONGO-1733
	public void doesNotApplyFieldsToDtoProjectionWhenQueryDefinesFields() {

		template.doFind("star-wars", new Document(), new Document("bar", 1), Person.class, Jedi.class, null);

		verify(findIterable).projection(eq(new Document("bar", 1)));
	}

	@Test // DATAMONGO-1733
	public void doesNotApplyFieldsWhenTargetIsNotAProjection() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, Person.class, null);

		verify(findIterable).projection(eq(new Document()));
	}

	@Test // DATAMONGO-1733
	public void doesNotApplyFieldsWhenTargetExtendsDomainType() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, PersonExtended.class, null);

		verify(findIterable).projection(eq(new Document()));
	}

	@Test // DATAMONGO-1348, DATAMONGO-2264
	public void geoNearShouldMapQueryCorrectly() {

		NearQuery query = NearQuery.near(new Point(1, 1));
		query.query(Query.query(Criteria.where("customName").is("rand al'thor")));

		template.geoNear(query, WithNamedFields.class);

		ArgumentCaptor<List<Document>> capture = ArgumentCaptor.forClass(List.class);

		verify(collection).aggregate(capture.capture(), eq(Document.class));
		Document $geoNear = capture.getValue().iterator().next();

		assertThat($geoNear, IsBsonObject.isBsonObject().containing("$geoNear.query.custom-named-field", "rand al'thor")
				.notContaining("query.customName"));
	}

	@Test // DATAMONGO-1348, DATAMONGO-2264
	public void geoNearShouldMapGeoJsonPointCorrectly() {

		NearQuery query = NearQuery.near(new GeoJsonPoint(1, 2));
		query.query(Query.query(Criteria.where("customName").is("rand al'thor")));

		template.geoNear(query, WithNamedFields.class);

		ArgumentCaptor<List<Document>> capture = ArgumentCaptor.forClass(List.class);

		verify(collection).aggregate(capture.capture(), eq(Document.class));
		Document $geoNear = capture.getValue().iterator().next();

		assertThat($geoNear, IsBsonObject.isBsonObject().containing("$geoNear.near.type", "Point")
				.containing("$geoNear.near.coordinates.[0]", 1D).containing("$geoNear.near.coordinates.[1]", 2D));
	}

	@Test // DATAMONGO-2155
	public void saveVersionedEntityShouldCallUpdateCorrectly() {

		when(updateResult.getModifiedCount()).thenReturn(1L);

		VersionedEntity entity = new VersionedEntity();
		entity.id = 1;
		entity.version = 10;

		ArgumentCaptor<org.bson.Document> queryCaptor = ArgumentCaptor.forClass(org.bson.Document.class);
		ArgumentCaptor<org.bson.Document> updateCaptor = ArgumentCaptor.forClass(org.bson.Document.class);

		template.save(entity);

		verify(collection, times(1)).replaceOne(queryCaptor.capture(), updateCaptor.capture(), any(ReplaceOptions.class));

		assertThat(queryCaptor.getValue(), is(equalTo(new Document("_id", 1).append("version", 10))));
		assertThat(updateCaptor.getValue(),
				is(equalTo(new Document("version", 11).append("_class", VersionedEntity.class.getName()))));
	}

	@Test // DATAMONGO-1783
	public void usesQueryOffsetForCountOperation() {

		template.count(new BasicQuery("{}").skip(100), AutogenerateableId.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).count(any(), options.capture());

		assertThat(options.getValue().getSkip(), is(equalTo(100)));
	}

	@Test // DATAMONGO-1783
	public void usesQueryLimitForCountOperation() {

		template.count(new BasicQuery("{}").limit(10), AutogenerateableId.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).count(any(), options.capture());

		assertThat(options.getValue().getLimit(), is(equalTo(10)));
	}

	@Test // DATAMONGO-2215
	public void updateShouldApplyArrayFilters() {

		template.updateFirst(new BasicQuery("{}"),
				new Update().set("grades.$[element]", 100).filterArray(Criteria.where("element").gte(100)),
				EntityWithListOfSimple.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(), options.capture());

		Assertions.assertThat((List<Bson>) options.getValue().getArrayFilters())
				.contains(new org.bson.Document("element", new Document("$gte", 100)));
	}

	@Test // DATAMONGO-2215
	public void findAndModifyShouldApplyArrayFilters() {

		template.findAndModify(new BasicQuery("{}"),
				new Update().set("grades.$[element]", 100).filterArray(Criteria.where("element").gte(100)),
				EntityWithListOfSimple.class);

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(), options.capture());

		Assertions.assertThat((List<Bson>) options.getValue().getArrayFilters())
				.contains(new org.bson.Document("element", new Document("$gte", 100)));
	}

	@Test // DATAMONGO-1854
	public void streamQueryShouldUseDefaultCollationWhenPresent() {

		template.stream(new BasicQuery("{}"), Sith.class).next();

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void findShouldNotUseCollationWhenNoDefaultPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}"), Jedi.class);

		verify(findIterable, never()).collation(any());
	}

	@Test // DATAMONGO-1854
	public void findShouldUseDefaultCollationWhenPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}"), Sith.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void findOneShouldUseDefaultCollationWhenPresent() {

		template.findOne(new BasicQuery("{'foo' : 'bar'}"), Sith.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void existsShouldUseDefaultCollationWhenPresent() {

		template.exists(new BasicQuery("{}"), Sith.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).count(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(equalTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build())));
	}

	@Test // DATAMONGO-1854
	public void findAndModfiyShoudUseDefaultCollationWhenPresent() {

		template.findAndModify(new BasicQuery("{}"), new Update(), Sith.class);

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void findAndRemoveShouldUseDefaultCollationWhenPresent() {

		template.findAndRemove(new BasicQuery("{}"), Sith.class);

		ArgumentCaptor<FindOneAndDeleteOptions> options = ArgumentCaptor.forClass(FindOneAndDeleteOptions.class);
		verify(collection).findOneAndDelete(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void createCollectionShouldNotCollationIfNotPresent() {

		template.createCollection(AutogenerateableId.class);

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		Assertions.assertThat(options.getValue().getCollation()).isNull();
	}

	@Test // DATAMONGO-1854
	public void createCollectionShouldApplyDefaultCollation() {

		template.createCollection(Sith.class);

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void createCollectionShouldFavorExplicitOptionsOverDefaultCollation() {

		template.createCollection(Sith.class, CollectionOptions.just(Collation.of("en_US")));

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("en_US").build()));
	}

	@Test // DATAMONGO-1854
	public void createCollectionShouldUseDefaultCollationIfCollectionOptionsAreNull() {

		template.createCollection(Sith.class, null);

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void aggreateShouldUseDefaultCollationIfPresent() {

		template.aggregate(newAggregation(Sith.class, project("id")), AutogenerateableId.class, Document.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void aggreateShouldUseCollationFromOptionsEvenIfDefaultCollationIsPresent() {

		template.aggregateStream(newAggregation(Sith.class, project("id")).withOptions(
				newAggregationOptions().collation(Collation.of("fr")).build()), AutogenerateableId.class, Document.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	public void aggreateStreamShouldUseDefaultCollationIfPresent() {

		template.aggregate(newAggregation(Sith.class, project("id")), AutogenerateableId.class, Document.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void aggreateStreamShouldUseCollationFromOptionsEvenIfDefaultCollationIsPresent() {

		template.aggregateStream(newAggregation(Sith.class, project("id")).withOptions(
				newAggregationOptions().collation(Collation.of("fr")).build()), AutogenerateableId.class, Document.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	public void findAndReplaceShouldUseCollationWhenPresent() {

		template.findAndReplace(new BasicQuery("{}").collation(Collation.of("fr")), new AutogenerateableId());

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-1854
	public void findOneWithSortShouldUseCollationWhenPresent() {

		template.findOne(new BasicQuery("{}").collation(Collation.of("fr")).with(Sort.by("id")), Sith.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	public void findOneWithSortShouldUseDefaultCollationWhenPresent() {

		template.findOne(new BasicQuery("{}").with(Sort.by("id")), Sith.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void findAndReplaceShouldUseDefaultCollationWhenPresent() {

		template.findAndReplace(new BasicQuery("{}"), new Sith());

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("de_AT"));
	}

	@Test // DATAMONGO-18545
	public void findAndReplaceShouldUseCollationEvenIfDefaultCollationIsPresent() {

		template.findAndReplace(new BasicQuery("{}").collation(Collation.of("fr")), new Sith());

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-1854
	public void findDistinctShouldUseDefaultCollationWhenPresent() {

		template.findDistinct(new BasicQuery("{}"), "name", Sith.class, String.class);

		verify(distinctIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void findDistinctPreferCollationFromQueryOverDefaultCollation() {

		template.findDistinct(new BasicQuery("{}").collation(Collation.of("fr")), "name", Sith.class, String.class);

		verify(distinctIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	public void updateFirstShouldUseDefaultCollationWhenPresent() {

		template.updateFirst(new BasicQuery("{}"), Update.update("foo", "bar"), Sith.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void updateFirstShouldPreferExplicitCollationOverDefaultCollation() {

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), Update.update("foo", "bar"), Sith.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	public void updateMultiShouldUseDefaultCollationWhenPresent() {

		template.updateMulti(new BasicQuery("{}"), Update.update("foo", "bar"), Sith.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void updateMultiShouldPreferExplicitCollationOverDefaultCollation() {

		template.updateMulti(new BasicQuery("{}").collation(Collation.of("fr")), Update.update("foo", "bar"), Sith.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	public void removeShouldUseDefaultCollationWhenPresent() {

		template.remove(new BasicQuery("{}"), Sith.class);

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void removeShouldPreferExplicitCollationOverDefaultCollation() {

		template.remove(new BasicQuery("{}").collation(Collation.of("fr")), Sith.class);

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	public void mapReduceShouldUseDefaultCollationWhenPresent() {

		template.mapReduce("", "", "", MapReduceOptions.options(), Sith.class);

		verify(mapReduceIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void mapReduceShouldPreferExplicitCollationOverDefaultCollation() {

		template.mapReduce("", "", "", MapReduceOptions.options().collation(Collation.of("fr")), Sith.class);

		verify(mapReduceIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	class AutogenerateableId {

		@Id BigInteger id;
	}

	class NotAutogenerateableId {

		@Id Integer id;

		public Pattern getId() {
			return Pattern.compile(".");
		}
	}

	static class VersionedEntity {

		@Id Integer id;
		@Version Integer version;
	}

	enum MyConverter implements Converter<AutogenerateableId, String> {

		INSTANCE;

		public String convert(AutogenerateableId source) {
			return source.toString();
		}
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = "star-wars")
	static class Person {

		@Id String id;
		String firstname;
	}

	static class PersonExtended extends Person {

		String lastname;
	}

	interface PersonProjection {
		String getFirstname();
	}

	public interface PersonSpELProjection {

		@Value("#{target.firstname}")
		String getName();
	}

	@Data
	static class Human {
		@Id String id;
	}

	@Data
	static class Jedi {

		@Field("firstname") String name;
	}

	class Wrapper {

		AutogenerateableId foo;
	}

	static class EntityWithListOfSimple {
		List<Integer> grades;
	}

	static class WithNamedFields {

		@Id String id;

		String name;
		@Field("custom-named-field") String customName;
	}

	@org.springframework.data.mongodb.core.mapping.Document(collation = "de_AT")
	static class Sith {

		@Field("firstname") String name;
	}

	/**
	 * Mocks out the {@link MongoTemplate#getDb()} method to return the {@link DB} mock instead of executing the actual
	 * behaviour.
	 *
	 * @return
	 */
	private MongoTemplate mockOutGetDb() {

		MongoTemplate template = spy(this.template);
		when(template.getDb()).thenReturn(db);
		return template;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.core.MongoOperationsUnitTests#getOperations()
	 */
	@Override
	protected MongoOperations getOperationsForExceptionHandling() {
		MongoTemplate template = spy(this.template);
		when(template.getDb()).thenThrow(new MongoException("Error!"));
		return template;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.core.MongoOperationsUnitTests#getOperations()
	 */
	@Override
	protected MongoOperations getOperations() {
		return this.template;
	}
}
