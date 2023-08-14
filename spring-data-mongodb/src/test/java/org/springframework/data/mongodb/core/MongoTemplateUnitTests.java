/*
 * Copyright 2010-2023 the original author or authors.
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
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.annotation.Version;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Point;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.context.InvalidPersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.aggregation.ComparisonOperators.Gte;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Switch.CaseOperator;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreator;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.Sharded;
import org.springframework.data.mongodb.core.mapping.TimeSeries;
import org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener;
import org.springframework.data.mongodb.core.mapping.event.AfterConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.timeseries.Granularity;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.lang.Nullable;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.CollectionUtils;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * Unit tests for {@link MongoTemplate}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Michael J. Simons
 * @author Roman Puchkovskiy
 * @author Yadhukrishna S Pai
 * @author Jakub Zurawa
 */
@MockitoSettings(strictness = Strictness.LENIENT)
public class MongoTemplateUnitTests extends MongoOperationsUnitTests {

	private MongoTemplate template;

	@Mock MongoDatabaseFactory factory;
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

	private Document commandResultDocument = new Document();

	private MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	private MappingMongoConverter converter;
	private MongoMappingContext mappingContext;

	@BeforeEach
	void beforeEach() {

		when(findIterable.iterator()).thenReturn(cursor);
		when(factory.getMongoDatabase()).thenReturn(db);
		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslator);
		when(factory.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
		when(db.getCollection(any(String.class), eq(Document.class))).thenReturn(collection);
		when(db.runCommand(any(), any(Class.class))).thenReturn(commandResultDocument);
		when(collection.find(any(org.bson.Document.class), any(Class.class))).thenReturn(findIterable);
		when(collection.mapReduce(any(), any(), eq(Document.class))).thenReturn(mapReduceIterable);
		when(collection.countDocuments(any(Bson.class), any(CountOptions.class))).thenReturn(1L);
		when(collection.estimatedDocumentCount(any())).thenReturn(1L);
		when(collection.getNamespace()).thenReturn(new MongoNamespace("db.mock-collection"));
		when(collection.aggregate(any(List.class), any())).thenReturn(aggregateIterable);
		when(collection.withReadConcern(any())).thenReturn(collection);
		when(collection.withReadPreference(any())).thenReturn(collection);
		when(collection.replaceOne(any(), any(), any(com.mongodb.client.model.ReplaceOptions.class))).thenReturn(updateResult);
		when(collection.withWriteConcern(any())).thenReturn(collectionWithWriteConcern);
		when(collection.distinct(anyString(), any(Document.class), any())).thenReturn(distinctIterable);
		when(collectionWithWriteConcern.deleteOne(any(Bson.class), any())).thenReturn(deleteResult);
		when(collectionWithWriteConcern.replaceOne(any(), any(), any(com.mongodb.client.model.ReplaceOptions.class))).thenReturn(updateResult);
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
		when(aggregateIterable.maxTime(anyLong(), any())).thenReturn(aggregateIterable);
		when(aggregateIterable.into(any())).thenReturn(Collections.emptyList());
		when(aggregateIterable.hint(any())).thenReturn(aggregateIterable);
		when(aggregateIterable.hintString(any())).thenReturn(aggregateIterable);
		when(distinctIterable.collation(any())).thenReturn(distinctIterable);
		when(distinctIterable.map(any())).thenReturn(distinctIterable);
		when(distinctIterable.into(any())).thenReturn(Collections.emptyList());

		this.mappingContext = new MongoMappingContext();
		mappingContext.setAutoIndexCreation(true);
		mappingContext.setSimpleTypeHolder(new MongoCustomConversions(Collections.emptyList()).getSimpleTypeHolder());
		mappingContext.afterPropertiesSet();

		this.converter = spy(new MappingMongoConverter(new DefaultDbRefResolver(factory), mappingContext));
		converter.afterPropertiesSet();
		this.template = new MongoTemplate(factory, converter);
	}

	@Test
	void rejectsNullDatabaseName() {
		assertThatIllegalArgumentException().isThrownBy(() -> new MongoTemplate(mongo, null));
	}

	@Test // DATAMONGO-1968
	void rejectsNullMongo() {
		assertThatIllegalArgumentException().isThrownBy(() -> new MongoTemplate((MongoClient) null, "database"));
	}

	@Test // DATAMONGO-1968
	void rejectsNullMongoClient() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new MongoTemplate((com.mongodb.client.MongoClient) null, "database"));
	}

	@Test // DATAMONGO-1870
	void removeHandlesMongoExceptionProperly() {

		MongoTemplate template = mockOutGetDb();

		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> template.remove(null, "collection"));
	}

	@Test
	void defaultsConverterToMappingMongoConverter() {
		MongoTemplate template = new MongoTemplate(mongo, "database");
		assertThat(ReflectionTestUtils.getField(template, "mongoConverter") instanceof MappingMongoConverter).isTrue();
	}

	@Test
	void rejectsNotFoundMapReduceResource() {

		GenericApplicationContext ctx = new GenericApplicationContext();
		ctx.refresh();
		template.setApplicationContext(ctx);

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> template.mapReduce("foo", "classpath:doesNotExist.js", "function() {}", Person.class));
	}

	@Test // DATAMONGO-322
	void rejectsEntityWithNullIdIfNotSupportedIdType() {

		Object entity = new NotAutogenerateableId();
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> template.save(entity));
	}

	@Test // DATAMONGO-322
	void storesEntityWithSetIdAlthoughNotAutogenerateable() {

		NotAutogenerateableId entity = new NotAutogenerateableId();
		entity.id = 1;

		template.save(entity);
	}

	@Test // DATAMONGO-322
	void autogeneratesIdForEntityWithAutogeneratableId() {

		this.converter.afterPropertiesSet();

		MongoTemplate template = spy(this.template);
		doReturn(new ObjectId()).when(template).saveDocument(any(String.class), any(Document.class), any(Class.class));

		AutogenerateableId entity = new AutogenerateableId();
		template.save(entity);

		assertThat(entity.id).isNotNull();
	}

	@Test // DATAMONGO-1912
	void autogeneratesIdForMap() {

		MongoTemplate template = spy(this.template);
		doReturn(new ObjectId()).when(template).saveDocument(any(String.class), any(Document.class), any(Class.class));

		Map<String, String> entity = new LinkedHashMap<>();
		template.save(entity, "foo");

		assertThat(entity).containsKey("_id");
	}

	@Test // DATAMONGO-374
	void convertsUpdateConstraintsUsingConverters() {

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
	void setsUnpopulatedIdField() {

		NotAutogenerateableId entity = new NotAutogenerateableId();

		template.populateIdIfNecessary(entity, 5);
		assertThat(entity.id).isEqualTo(5);
	}

	@Test // DATAMONGO-474
	void doesNotSetAlreadyPopulatedId() {

		NotAutogenerateableId entity = new NotAutogenerateableId();
		entity.id = 5;

		template.populateIdIfNecessary(entity, 7);
		assertThat(entity.id).isEqualTo(5);
	}

	@Test // DATAMONGO-868
	void findAndModifyShouldBumpVersionByOneWhenVersionFieldNotIncludedInUpdate() {

		VersionedEntity v = new VersionedEntity();
		v.id = 1;
		v.version = 0;

		ArgumentCaptor<org.bson.Document> captor = ArgumentCaptor.forClass(org.bson.Document.class);

		template.findAndModify(new Query(), new Update().set("id", "10"), VersionedEntity.class);

		verify(collection, times(1)).findOneAndUpdate(any(org.bson.Document.class), captor.capture(),
				any(FindOneAndUpdateOptions.class));
		assertThat(captor.getValue().get("$inc")).isEqualTo(new Document("version", 1L));
	}

	@Test // DATAMONGO-868
	void findAndModifyShouldNotBumpVersionByOneWhenVersionFieldAlreadyIncludedInUpdate() {

		VersionedEntity v = new VersionedEntity();
		v.id = 1;
		v.version = 0;

		ArgumentCaptor<org.bson.Document> captor = ArgumentCaptor.forClass(org.bson.Document.class);

		template.findAndModify(new Query(), new Update().set("version", 100), VersionedEntity.class);

		verify(collection, times(1)).findOneAndUpdate(any(org.bson.Document.class), captor.capture(),
				any(FindOneAndUpdateOptions.class));

		assertThat(captor.getValue().get("$set")).isEqualTo(new Document("version", 100));
		assertThat(captor.getValue().get("$inc")).isNull();
	}

	@Test // DATAMONGO-533
	void registersDefaultEntityIndexCreatorIfApplicationContextHasOneForDifferentMappingContext() {

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
	void findAllAndRemoveShouldRetrieveMatchingDocumentsPriorToRemoval() {

		BasicQuery query = new BasicQuery("{'foo':'bar'}");
		template.findAllAndRemove(query, VersionedEntity.class);
		verify(collection, times(1)).find(Mockito.eq(query.getQueryObject()), any(Class.class));
	}

	@Test // GH-3648
	void shouldThrowExceptionIfEntityReaderReturnsNull() {

		when(cursor.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
		when(cursor.next()).thenReturn(new org.bson.Document("_id", Integer.valueOf(0)));
		MappingMongoConverter converter = mock(MappingMongoConverter.class);
		when(converter.getMappingContext()).thenReturn((MappingContext) mappingContext);
		when(converter.getProjectionFactory()).thenReturn(new SpelAwareProxyProjectionFactory());
		template = new MongoTemplate(factory, converter);

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> template.findAll(Person.class))
				.withMessageContaining("returned null");
	}

	@Test // DATAMONGO-566
	void findAllAndRemoveShouldRemoveDocumentsReturedByFindQuery() {

		when(cursor.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
		when(cursor.next()).thenReturn(new org.bson.Document("_id", Integer.valueOf(0)))
				.thenReturn(new org.bson.Document("_id", Integer.valueOf(1)));

		ArgumentCaptor<org.bson.Document> queryCaptor = ArgumentCaptor.forClass(org.bson.Document.class);
		BasicQuery query = new BasicQuery("{'foo':'bar'}");
		template.findAllAndRemove(query, VersionedEntity.class);

		verify(collection, times(1)).deleteMany(queryCaptor.capture(), any());

		Document idField = DocumentTestUtils.getAsDocument(queryCaptor.getValue(), "_id");
		assertThat((List<Object>) idField.get("$in")).containsExactly(Integer.valueOf(0), Integer.valueOf(1));
	}

	@Test // DATAMONGO-566
	void findAllAndRemoveShouldNotTriggerRemoveIfFindResultIsEmpty() {

		template.findAllAndRemove(new BasicQuery("{'foo':'bar'}"), VersionedEntity.class);
		verify(collection, never()).deleteMany(any(org.bson.Document.class));
	}

	@Test // DATAMONGO-948
	void sortShouldBeTakenAsIsWhenExecutingQueryWithoutSpecificTypeInformation() {

		Query query = Query.query(Criteria.where("foo").is("bar")).with(Sort.by("foo"));
		template.executeQuery(query, "collection1", new DocumentCallbackHandler() {

			@Override
			public void processDocument(Document document) throws MongoException, DataAccessException {
				// nothing to do - just a test
			}
		});

		ArgumentCaptor<org.bson.Document> captor = ArgumentCaptor.forClass(org.bson.Document.class);

		verify(findIterable, times(1)).sort(captor.capture());
		assertThat(captor.getValue()).isEqualTo(new Document("foo", 1));
	}

	@Test // DATAMONGO-1166, DATAMONGO-1824
	void aggregateShouldHonorReadPreferenceWhenSet() {

		template.setReadPreference(ReadPreference.secondary());

		template.aggregate(newAggregation(Aggregation.unwind("foo")), "collection-1", Wrapper.class);

		verify(collection).withReadPreference(eq(ReadPreference.secondary()));
	}

	@Test // DATAMONGO-1166, DATAMONGO-1824
	void aggregateShouldIgnoreReadPreferenceWhenNotSet() {

		template.aggregate(newAggregation(Aggregation.unwind("foo")), "collection-1", Wrapper.class);

		verify(collection, never()).withReadPreference(any());
	}

	@Test // GH-4277
	void aggregateShouldHonorOptionsReadConcernWhenSet() {

		AggregationOptions options = AggregationOptions.builder().readConcern(ReadConcern.SNAPSHOT).build();
		template.aggregate(newAggregation(Aggregation.unwind("foo")).withOptions(options), "collection-1", Wrapper.class);

		verify(collection).withReadConcern(ReadConcern.SNAPSHOT);
	}

	@Test // GH-4277
	void aggregateShouldHonorOptionsReadPreferenceWhenSet() {

		AggregationOptions options = AggregationOptions.builder().readPreference(ReadPreference.secondary()).build();
		template.aggregate(newAggregation(Aggregation.unwind("foo")).withOptions(options), "collection-1", Wrapper.class);

		verify(collection).withReadPreference(ReadPreference.secondary());
	}

	@Test // GH-4277
	void aggregateStreamShouldHonorOptionsReadPreferenceWhenSet() {

		AggregationOptions options = AggregationOptions.builder().readPreference(ReadPreference.secondary()).build();
		template.aggregateStream(newAggregation(Aggregation.unwind("foo")).withOptions(options), "collection-1",
				Wrapper.class);

		verify(collection).withReadPreference(ReadPreference.secondary());
	}

	@Test // DATAMONGO-2153
	void aggregateShouldHonorOptionsComment() {

		AggregationOptions options = AggregationOptions.builder().comment("expensive").build();

		template.aggregate(newAggregation(Aggregation.unwind("foo")).withOptions(options), "collection-1", Wrapper.class);

		verify(aggregateIterable).comment("expensive");
	}

	@Test // DATAMONGO-1836
	void aggregateShouldHonorOptionsHint() {

		Document hint = new Document("dummyField", 1);
		AggregationOptions options = AggregationOptions.builder().hint(hint).build();

		template.aggregate(newAggregation(Aggregation.unwind("foo")).withOptions(options), "collection-1", Wrapper.class);

		verify(aggregateIterable).hint(hint);
	}

	@Test // GH-4238
	void aggregateShouldHonorOptionsHintString() {

		AggregationOptions options = AggregationOptions.builder().hint("index-1").build();

		template.aggregate(newAggregation(Aggregation.unwind("foo")).withOptions(options), "collection-1", Wrapper.class);

		verify(aggregateIterable).hintString("index-1");
	}

	@Test // GH-3542
	void aggregateShouldUseRelaxedMappingByDefault() {

		MongoTemplate template = new MongoTemplate(factory, converter) {

			@Override
			protected <O> AggregationResults<O> doAggregate(Aggregation aggregation, String collectionName,
					Class<O> outputType, AggregationOperationContext context) {

				assertThat(context).isInstanceOf(RelaxedTypeBasedAggregationOperationContext.class);
				return super.doAggregate(aggregation, collectionName, outputType, context);
			}
		};

		template.aggregate(
				newAggregation(Jedi.class, Aggregation.unwind("foo")).withOptions(AggregationOptions.builder().build()),
				Jedi.class);
	}

	@Test // GH-3542
	void aggregateShouldUseStrictMappingIfOptionsIndicate() {

		MongoTemplate template = new MongoTemplate(factory, converter) {

			@Override
			protected <O> AggregationResults<O> doAggregate(Aggregation aggregation, String collectionName,
					Class<O> outputType, AggregationOperationContext context) {

				assertThat(context).isInstanceOf(TypeBasedAggregationOperationContext.class);
				return super.doAggregate(aggregation, collectionName, outputType, context);
			}
		};

		assertThatExceptionOfType(InvalidPersistentPropertyPath.class)
				.isThrownBy(() -> template.aggregate(newAggregation(Jedi.class, Aggregation.unwind("foo"))
						.withOptions(AggregationOptions.builder().strictMapping().build()), Jedi.class));
	}

	@Test // DATAMONGO-1166, DATAMONGO-2264
	void geoNearShouldHonorReadPreferenceWhenSet() {

		template.setReadPreference(ReadPreference.secondary());

		NearQuery query = NearQuery.near(new Point(1, 1));
		template.geoNear(query, Wrapper.class);

		verify(collection).withReadPreference(eq(ReadPreference.secondary()));
	}

	@Test // GH-4277
	void geoNearShouldHonorReadPreferenceFromQuery() {

		NearQuery query = NearQuery.near(new Point(1, 1));
		query.withReadPreference(ReadPreference.secondary());

		template.geoNear(query, Wrapper.class);

		verify(collection).withReadPreference(eq(ReadPreference.secondary()));
	}

	@Test // GH-4277
	void geoNearShouldHonorReadConcernFromQuery() {

		NearQuery query = NearQuery.near(new Point(1, 1));
		query.withReadConcern(ReadConcern.SNAPSHOT);

		template.geoNear(query, Wrapper.class);

		verify(collection).withReadConcern(eq(ReadConcern.SNAPSHOT));
	}

	@Test // DATAMONGO-1166, DATAMONGO-2264
	void geoNearShouldIgnoreReadPreferenceWhenNotSet() {

		NearQuery query = NearQuery.near(new Point(1, 1));
		template.geoNear(query, Wrapper.class);

		verify(collection, never()).withReadPreference(any());
	}

	@Test // DATAMONGO-1334
	@Disabled("TODO: mongo3 - a bit hard to tests with the immutable object stuff")
	void mapReduceShouldUseZeroAsDefaultLimit() {

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
	void mapReduceShouldPickUpLimitFromQuery() {

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
	void mapReduceShouldPickUpLimitFromOptions() {

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
	void mapReduceShouldPickUpLimitFromOptionsWhenQueryIsNotPresent() {

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
	void mapReduceShouldPickUpLimitFromOptionsEvenWhenQueryDefinesItDifferently() {

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
	void beforeConvertEventForUpdateSeesNextVersion() {

		when(updateResult.getModifiedCount()).thenReturn(1L);

		final VersionedEntity entity = new VersionedEntity();
		entity.id = 1;
		entity.version = 0;

		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		context.addApplicationListener(new AbstractMongoEventListener<VersionedEntity>() {

			@Override
			public void onBeforeConvert(BeforeConvertEvent<VersionedEntity> event) {
				assertThat(event.getSource().version).isEqualTo(1);
			}
		});

		template.setApplicationContext(context);

		template.save(entity);
	}

	@Test // DATAMONGO-1447
	void shouldNotAppend$isolatedToNonMulitUpdate() {

		template.updateFirst(new Query(), new Update().isolated().set("jon", "snow"), Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateOne(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat((Document) queryCaptor.getValue()).doesNotContainKey("$isolated");
		assertThat((Document) updateCaptor.getValue()).containsEntry("$set.jon", "snow").doesNotContainKey("$isolated");
	}

	@Test // DATAMONGO-1447
	void shouldAppend$isolatedToUpdateMultiEmptyQuery() {

		template.updateMulti(new Query(), new Update().isolated().set("jon", "snow"), Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateMany(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat((Document) queryCaptor.getValue()).hasSize(1).containsEntry("$isolated", 1);
		assertThat((Document) updateCaptor.getValue()).containsEntry("$set.jon", "snow").doesNotContainKey("$isolated");
	}

	@Test // DATAMONGO-1447
	void shouldAppend$isolatedToUpdateMultiQueryIfNotPresentAndUpdateSetsValue() {

		Update update = new Update().isolated().set("jon", "snow");
		Query query = new BasicQuery("{'eddard':'stark'}");

		template.updateMulti(query, update, Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateMany(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat((Document) queryCaptor.getValue()).containsEntry("$isolated", 1).containsEntry("eddard", "stark");
		assertThat((Document) updateCaptor.getValue()).containsEntry("$set.jon", "snow").doesNotContainKey("$isolated");
	}

	@Test // DATAMONGO-1447
	void shouldNotAppend$isolatedToUpdateMultiQueryIfNotPresentAndUpdateDoesNotSetValue() {

		Update update = new Update().set("jon", "snow");
		Query query = new BasicQuery("{'eddard':'stark'}");

		template.updateMulti(query, update, Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateMany(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat((Document) queryCaptor.getValue()).doesNotContainKey("$isolated").containsEntry("eddard", "stark");
		assertThat((Document) updateCaptor.getValue()).containsEntry("$set.jon", "snow").doesNotContainKey("$isolated");
	}

	@Test // DATAMONGO-1447
	void shouldNotOverwrite$isolatedToUpdateMultiQueryIfPresentAndUpdateDoesNotSetValue() {

		Update update = new Update().set("jon", "snow");
		Query query = new BasicQuery("{'eddard':'stark', '$isolated' : 1}");

		template.updateMulti(query, update, Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateMany(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat((Document) queryCaptor.getValue()).containsEntry("$isolated", 1).containsEntry("eddard", "stark");
		assertThat((Document) updateCaptor.getValue()).containsEntry("$set.jon", "snow").doesNotContainKey("$isolated");
	}

	@Test // DATAMONGO-1447
	void shouldNotOverwrite$isolatedToUpdateMultiQueryIfPresentAndUpdateSetsValue() {

		Update update = new Update().isolated().set("jon", "snow");
		Query query = new BasicQuery("{'eddard':'stark', '$isolated' : 0}");

		template.updateMulti(query, update, Wrapper.class);

		ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);

		verify(collection).updateMany(queryCaptor.capture(), updateCaptor.capture(), any());

		assertThat((Document) queryCaptor.getValue()).containsEntry("$isolated", 0).containsEntry("eddard", "stark");
		assertThat((Document) updateCaptor.getValue()).containsEntry("$set.jon", "snow").doesNotContainKey("$isolated");
	}

	@Test // DATAMONGO-1311
	void executeQueryShouldUseBatchSizeWhenPresent() {

		when(findIterable.batchSize(anyInt())).thenReturn(findIterable);

		Query query = new Query().cursorBatchSize(1234);
		template.find(query, Person.class);

		verify(findIterable).batchSize(1234);
	}

	@Test // GH-4277
	void findShouldUseReadConcernWhenPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}").withReadConcern(ReadConcern.SNAPSHOT), AutogenerateableId.class);

		verify(collection).withReadConcern(ReadConcern.SNAPSHOT);
	}

	@Test // GH-4277
	void findShouldUseReadPreferenceWhenPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}").withReadPreference(ReadPreference.secondary()),
				AutogenerateableId.class);

		verify(collection).withReadPreference(ReadPreference.secondary());
	}

	@Test // DATAMONGO-1518
	void executeQueryShouldUseCollationWhenPresent() {

		template.executeQuery(new BasicQuery("{}").collation(Collation.of("fr")), "collection-1", val -> {});

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	void streamQueryShouldUseCollationWhenPresent() {

		template.stream(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	void findShouldUseCollationWhenPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}").collation(Collation.of("fr")), AutogenerateableId.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	void findOneShouldUseCollationWhenPresent() {

		template.findOne(new BasicQuery("{'foo' : 'bar'}").collation(Collation.of("fr")), AutogenerateableId.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	void existsShouldUseCollationWhenPresent() {

		template.exists(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("fr").build());
	}

	@Test // DATAMONGO-1518
	void findAndModfiyShoudUseCollationWhenPresent() {

		template.findAndModify(new BasicQuery("{}").collation(Collation.of("fr")), new Update(), AutogenerateableId.class);

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518
	void findAndRemoveShouldUseCollationWhenPresent() {

		template.findAndRemove(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class);

		ArgumentCaptor<FindOneAndDeleteOptions> options = ArgumentCaptor.forClass(FindOneAndDeleteOptions.class);
		verify(collection).findOneAndDelete(any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-2196
	void removeShouldApplyWriteConcern() {

		Person person = new Person();
		person.id = "id-1";

		template.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
		template.remove(person);

		verify(collection).withWriteConcern(eq(WriteConcern.UNACKNOWLEDGED));
		verify(collectionWithWriteConcern).deleteOne(any(Bson.class), any());
	}

	@Test // DATAMONGO-1518
	void findAndRemoveManyShouldUseCollationWhenPresent() {

		template.doRemove("collection-1", new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class,
				true);

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518
	void updateOneShouldUseCollationWhenPresent() {

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), new Update().set("foo", "bar"),
				AutogenerateableId.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518
	void updateManyShouldUseCollationWhenPresent() {

		template.updateMulti(new BasicQuery("{}").collation(Collation.of("fr")), new Update().set("foo", "bar"),
				AutogenerateableId.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // GH-3218
	void updateUsesHintStringFromQuery() {

		template.updateFirst(new Query().withHint("index-1"), new Update().set("spring", "data"), Human.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(Bson.class), any(Bson.class), options.capture());

		assertThat(options.getValue().getHintString()).isEqualTo("index-1");
	}

	@Test // GH-3218
	void updateUsesHintDocumentFromQuery() {

		template.updateFirst(new Query().withHint("{ name : 1 }"), new Update().set("spring", "data"), Human.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(Bson.class), any(Bson.class), options.capture());

		assertThat(options.getValue().getHint()).isEqualTo(new Document("name", 1));
	}

	@Test // DATAMONGO-1518
	void replaceOneShouldUseCollationWhenPresent() {

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), new Update(), AutogenerateableId.class);

		ArgumentCaptor<com.mongodb.client.model.ReplaceOptions> options = ArgumentCaptor
				.forClass(com.mongodb.client.model.ReplaceOptions.class);
		verify(collection).replaceOne(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518, DATAMONGO-1824
	void aggregateShouldUseCollationWhenPresent() {

		Aggregation aggregation = newAggregation(project("id"))
				.withOptions(newAggregationOptions().collation(Collation.of("fr")).build());
		template.aggregate(aggregation, AutogenerateableId.class, Document.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1824
	void aggregateShouldUseBatchSizeWhenPresent() {

		Aggregation aggregation = newAggregation(project("id"))
				.withOptions(newAggregationOptions().collation(Collation.of("fr")).cursorBatchSize(100).build());
		template.aggregate(aggregation, AutogenerateableId.class, Document.class);

		verify(aggregateIterable).batchSize(100);
	}

	@Test // DATAMONGO-1518
	void mapReduceShouldUseCollationWhenPresent() {

		template.mapReduce("", "", "", MapReduceOptions.options().collation(Collation.of("fr")), AutogenerateableId.class);

		verify(mapReduceIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-2027
	void mapReduceShouldUseOutputCollectionWhenPresent() {

		template.mapReduce("", "", "", MapReduceOptions.options().outputCollection("out-collection"),
				AutogenerateableId.class);

		verify(mapReduceIterable).collectionName(eq("out-collection"));
	}

	@Test // DATAMONGO-2027
	void mapReduceShouldNotUseOutputCollectionForInline() {

		template.mapReduce("", "", "", MapReduceOptions.options().actionInline().outputCollection("out-collection"),
				AutogenerateableId.class);

		verify(mapReduceIterable, never()).collectionName(any());
	}

	@Test // DATAMONGO-2027
	void mapReduceShouldUseOutputActionWhenPresent() {

		template.mapReduce("", "", "", MapReduceOptions.options().actionMerge().outputCollection("out-collection"),
				AutogenerateableId.class);

		verify(mapReduceIterable).action(eq(MapReduceAction.MERGE));
	}

	@Test // DATAMONGO-2027
	void mapReduceShouldUseOutputDatabaseWhenPresent() {

		template.mapReduce("", "", "",
				MapReduceOptions.options().outputDatabase("out-database").outputCollection("out-collection"),
				AutogenerateableId.class);

		verify(mapReduceIterable).databaseName(eq("out-database"));
	}

	@Test // DATAMONGO-2027
	void mapReduceShouldNotUseOutputDatabaseForInline() {

		template.mapReduce("", "", "", MapReduceOptions.options().outputDatabase("out-database"), AutogenerateableId.class);

		verify(mapReduceIterable, never()).databaseName(any());
	}

	@Test // DATAMONGO-1518, DATAMONGO-2264
	void geoNearShouldUseCollationWhenPresent() {

		NearQuery query = NearQuery.near(0D, 0D).query(new BasicQuery("{}").collation(Collation.of("fr")));
		template.geoNear(query, AutogenerateableId.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1880
	void countShouldUseCollationWhenPresent() {

		template.count(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("fr").build());
	}

	@Test // DATAMONGO-2360
	void countShouldApplyQueryHintIfPresent() {

		Document queryHint = new Document("age", 1);
		template.count(new BasicQuery("{}").withHint(queryHint), AutogenerateableId.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getHint()).isEqualTo(queryHint);
	}

	@Test // DATAMONGO-2365
	void countShouldApplyQueryHintAsIndexNameIfPresent() {

		template.count(new BasicQuery("{}").withHint("idx-1"), AutogenerateableId.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getHintString()).isEqualTo("idx-1");
	}

	@Test // DATAMONGO-1733
	void appliesFieldsWhenInterfaceProjectionIsClosedAndQueryDoesNotDefineFields() {

		template.doFind(CollectionPreparer.identity(), "star-wars", new Document(), new Document(), Person.class,
				PersonProjection.class, CursorPreparer.NO_OP_PREPARER);

		verify(findIterable).projection(eq(new Document("firstname", 1)));
	}

	@Test // DATAMONGO-1733
	void doesNotApplyFieldsWhenInterfaceProjectionIsClosedAndQueryDefinesFields() {

		template.doFind(CollectionPreparer.identity(), "star-wars", new Document(), new Document("bar", 1), Person.class,
				PersonProjection.class, CursorPreparer.NO_OP_PREPARER);

		verify(findIterable).projection(eq(new Document("bar", 1)));
	}

	@Test // DATAMONGO-1733
	void doesNotApplyFieldsWhenInterfaceProjectionIsOpen() {

		template.doFind(CollectionPreparer.identity(), "star-wars", new Document(), new Document(), Person.class,
				PersonSpELProjection.class, CursorPreparer.NO_OP_PREPARER);

		verify(findIterable).projection(eq(BsonUtils.EMPTY_DOCUMENT));
	}

	@Test // DATAMONGO-1733, DATAMONGO-2041
	void appliesFieldsToDtoProjection() {

		template.doFind(CollectionPreparer.identity(), "star-wars", new Document(), new Document(), Person.class,
				Jedi.class, CursorPreparer.NO_OP_PREPARER);

		verify(findIterable).projection(eq(new Document("firstname", 1)));
	}

	@Test // DATAMONGO-1733
	void doesNotApplyFieldsToDtoProjectionWhenQueryDefinesFields() {

		template.doFind(CollectionPreparer.identity(), "star-wars", new Document(), new Document("bar", 1), Person.class,
				Jedi.class, CursorPreparer.NO_OP_PREPARER);

		verify(findIterable).projection(eq(new Document("bar", 1)));
	}

	@Test // DATAMONGO-1733
	void doesNotApplyFieldsWhenTargetIsNotAProjection() {

		template.doFind(CollectionPreparer.identity(), "star-wars", new Document(), new Document(), Person.class,
				Person.class, CursorPreparer.NO_OP_PREPARER);

		verify(findIterable).projection(eq(BsonUtils.EMPTY_DOCUMENT));
	}

	@Test // DATAMONGO-1733
	void doesNotApplyFieldsWhenTargetExtendsDomainType() {

		template.doFind(CollectionPreparer.identity(), "star-wars", new Document(), new Document(), Person.class,
				PersonExtended.class, CursorPreparer.NO_OP_PREPARER);

		verify(findIterable).projection(eq(BsonUtils.EMPTY_DOCUMENT));
	}

	@Test // DATAMONGO-1348, DATAMONGO-2264
	void geoNearShouldMapQueryCorrectly() {

		NearQuery query = NearQuery.near(new Point(1, 1));
		query.query(Query.query(Criteria.where("customName").is("rand al'thor")));

		template.geoNear(query, WithNamedFields.class);

		ArgumentCaptor<List<Document>> capture = ArgumentCaptor.forClass(List.class);

		verify(collection).aggregate(capture.capture(), eq(Document.class));
		Document $geoNear = capture.getValue().iterator().next();

		assertThat($geoNear).containsEntry("$geoNear.query.custom-named-field", "rand al'thor")
				.doesNotContainKey("query.customName");
	}

	@Test // DATAMONGO-1348, DATAMONGO-2264
	void geoNearShouldMapGeoJsonPointCorrectly() {

		NearQuery query = NearQuery.near(new GeoJsonPoint(1, 2));
		query.query(Query.query(Criteria.where("customName").is("rand al'thor")));

		template.geoNear(query, WithNamedFields.class);

		ArgumentCaptor<List<Document>> capture = ArgumentCaptor.forClass(List.class);

		verify(collection).aggregate(capture.capture(), eq(Document.class));
		Document $geoNear = capture.getValue().iterator().next();

		assertThat($geoNear).containsEntry("$geoNear.near.type", "Point").containsEntry("$geoNear.near.coordinates.[0]", 1D)
				.containsEntry("$geoNear.near.coordinates.[1]", 2D);
	}

	@Test // DATAMONGO-2155, GH-3407
	void saveVersionedEntityShouldCallUpdateCorrectly() {

		when(updateResult.getModifiedCount()).thenReturn(1L);

		VersionedEntity entity = new VersionedEntity();
		entity.id = 1;
		entity.version = 10;

		ArgumentCaptor<org.bson.Document> queryCaptor = ArgumentCaptor.forClass(org.bson.Document.class);
		ArgumentCaptor<org.bson.Document> updateCaptor = ArgumentCaptor.forClass(org.bson.Document.class);

		template.save(entity);

		verify(collection, times(1)).replaceOne(queryCaptor.capture(), updateCaptor.capture(), any(com.mongodb.client.model.ReplaceOptions.class));

		assertThat(queryCaptor.getValue()).isEqualTo(new Document("_id", 1).append("version", 10));
		assertThat(updateCaptor.getValue())
				.isEqualTo(new Document("version", 11).append("_class", VersionedEntity.class.getName()).append("name", null));
	}

	@Test // DATAMONGO-1783
	void usesQueryOffsetForCountOperation() {

		template.count(new BasicQuery("{}").skip(100), AutogenerateableId.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getSkip()).isEqualTo(100);
	}

	@Test // DATAMONGO-1783
	void usesQueryLimitForCountOperation() {

		template.count(new BasicQuery("{}").limit(10), AutogenerateableId.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getLimit()).isEqualTo(10);
	}

	@Test // DATAMONGO-2215
	void updateShouldApplyArrayFilters() {

		template.updateFirst(new BasicQuery("{}"),
				new Update().set("grades.$[element]", 100).filterArray(Criteria.where("element").gte(100)),
				EntityWithListOfSimple.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		Assertions.assertThat((List<Bson>) options.getValue().getArrayFilters())
				.contains(new org.bson.Document("element", new Document("$gte", 100)));
	}

	@Test // DATAMONGO-2215
	void findAndModifyShouldApplyArrayFilters() {

		template.findAndModify(new BasicQuery("{}"),
				new Update().set("grades.$[element]", 100).filterArray(Criteria.where("element").gte(100)),
				EntityWithListOfSimple.class);

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(Bson.class), options.capture());

		Assertions.assertThat((List<Bson>) options.getValue().getArrayFilters())
				.contains(new org.bson.Document("element", new Document("$gte", 100)));
	}

	@Test // DATAMONGO-1854
	void streamQueryShouldUseDefaultCollationWhenPresent() {

		template.stream(new BasicQuery("{}"), Sith.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void findShouldNotUseCollationWhenNoDefaultPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}"), Jedi.class);

		verify(findIterable, never()).collation(any());
	}

	@Test // DATAMONGO-1854
	void findShouldUseDefaultCollationWhenPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}"), Sith.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void findOneShouldUseDefaultCollationWhenPresent() {

		template.findOne(new BasicQuery("{'foo' : 'bar'}"), Sith.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void existsShouldUseDefaultCollationWhenPresent() {

		template.exists(new BasicQuery("{}"), Sith.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void findAndModfiyShoudUseDefaultCollationWhenPresent() {

		template.findAndModify(new BasicQuery("{}"), new Update(), Sith.class);

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void findAndRemoveShouldUseDefaultCollationWhenPresent() {

		template.findAndRemove(new BasicQuery("{}"), Sith.class);

		ArgumentCaptor<FindOneAndDeleteOptions> options = ArgumentCaptor.forClass(FindOneAndDeleteOptions.class);
		verify(collection).findOneAndDelete(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void createCollectionShouldNotCollationIfNotPresent() {

		template.createCollection(AutogenerateableId.class);

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		Assertions.assertThat(options.getValue().getCollation()).isNull();
	}

	@Test // DATAMONGO-1854
	void createCollectionShouldApplyDefaultCollation() {

		template.createCollection(Sith.class);

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void createCollectionShouldFavorExplicitOptionsOverDefaultCollation() {

		template.createCollection(Sith.class, CollectionOptions.just(Collation.of("en_US")));

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("en_US").build());
	}

	@Test // DATAMONGO-1854
	void createCollectionShouldUseDefaultCollationIfCollectionOptionsAreNull() {

		template.createCollection(Sith.class, null);

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void aggreateShouldUseDefaultCollationIfPresent() {

		template.aggregate(newAggregation(Sith.class, project("id")), AutogenerateableId.class, Document.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void aggreateShouldUseCollationFromOptionsEvenIfDefaultCollationIsPresent() {

		template.aggregateStream(newAggregation(Sith.class, project("id")).withOptions(
				newAggregationOptions().collation(Collation.of("fr")).build()), AutogenerateableId.class, Document.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	void aggreateStreamShouldUseDefaultCollationIfPresent() {

		template.aggregate(newAggregation(Sith.class, project("id")), AutogenerateableId.class, Document.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void aggreateStreamShouldUseCollationFromOptionsEvenIfDefaultCollationIsPresent() {

		template.aggregateStream(newAggregation(Sith.class, project("id")).withOptions(
				newAggregationOptions().collation(Collation.of("fr")).build()), AutogenerateableId.class, Document.class);

		verify(aggregateIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-2390
	void aggregateShouldNoApplyZeroOrNegativeMaxTime() {

		template.aggregate(
				newAggregation(Sith.class, project("id")).withOptions(newAggregationOptions().maxTime(Duration.ZERO).build()),
				AutogenerateableId.class, Document.class);
		template.aggregate(newAggregation(Sith.class, project("id")).withOptions(
				newAggregationOptions().maxTime(Duration.ofSeconds(-1)).build()), AutogenerateableId.class, Document.class);

		verify(aggregateIterable, never()).maxTime(anyLong(), any());
	}

	@Test // DATAMONGO-2390
	void aggregateShouldApplyMaxTimeIfSet() {

		template.aggregate(newAggregation(Sith.class, project("id")).withOptions(
				newAggregationOptions().maxTime(Duration.ofSeconds(10)).build()), AutogenerateableId.class, Document.class);

		verify(aggregateIterable).maxTime(eq(10000L), eq(TimeUnit.MILLISECONDS));
	}

	@Test // DATAMONGO-1854
	void findAndReplaceShouldUseCollationWhenPresent() {

		template.findAndReplace(new BasicQuery("{}").collation(Collation.of("fr")), new AutogenerateableId());

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1854
	void findOneWithSortShouldUseCollationWhenPresent() {

		template.findOne(new BasicQuery("{}").collation(Collation.of("fr")).with(Sort.by("id")), Sith.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	void findOneWithSortShouldUseDefaultCollationWhenPresent() {

		template.findOne(new BasicQuery("{}").with(Sort.by("id")), Sith.class);

		verify(findIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void findAndReplaceShouldUseDefaultCollationWhenPresent() {

		template.findAndReplace(new BasicQuery("{}"), new Sith());

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("de_AT");
	}

	@Test // DATAMONGO-1854
	void findAndReplaceShouldUseCollationEvenIfDefaultCollationIsPresent() {

		template.findAndReplace(new BasicQuery("{}").collation(Collation.of("fr")), new Sith());

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1854
	void findDistinctShouldUseDefaultCollationWhenPresent() {

		template.findDistinct(new BasicQuery("{}"), "name", Sith.class, String.class);

		verify(distinctIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void findDistinctPreferCollationFromQueryOverDefaultCollation() {

		template.findDistinct(new BasicQuery("{}").collation(Collation.of("fr")), "name", Sith.class, String.class);

		verify(distinctIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	void updateFirstShouldUseDefaultCollationWhenPresent() {

		template.updateFirst(new BasicQuery("{}"), Update.update("foo", "bar"), Sith.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void updateFirstShouldPreferExplicitCollationOverDefaultCollation() {

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), Update.update("foo", "bar"), Sith.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("fr").build());
	}

	@Test // DATAMONGO-1854
	void updateMultiShouldUseDefaultCollationWhenPresent() {

		template.updateMulti(new BasicQuery("{}"), Update.update("foo", "bar"), Sith.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void updateMultiShouldPreferExplicitCollationOverDefaultCollation() {

		template.updateMulti(new BasicQuery("{}").collation(Collation.of("fr")), Update.update("foo", "bar"), Sith.class);

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("fr").build());
	}

	@Test // DATAMONGO-1854
	void removeShouldUseDefaultCollationWhenPresent() {

		template.remove(new BasicQuery("{}"), Sith.class);

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void removeShouldPreferExplicitCollationOverDefaultCollation() {

		template.remove(new BasicQuery("{}").collation(Collation.of("fr")), Sith.class);

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("fr").build());
	}

	@Test // DATAMONGO-1854
	void mapReduceShouldUseDefaultCollationWhenPresent() {

		template.mapReduce("", "", "", MapReduceOptions.options(), Sith.class);

		verify(mapReduceIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void mapReduceShouldPreferExplicitCollationOverDefaultCollation() {

		template.mapReduce("", "", "", MapReduceOptions.options().collation(Collation.of("fr")), Sith.class);

		verify(mapReduceIterable).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-2261
	void saveShouldInvokeCallbacks() {

		ValueCapturingBeforeConvertCallback beforeConvertCallback = spy(new ValueCapturingBeforeConvertCallback());
		ValueCapturingBeforeSaveCallback beforeSaveCallback = spy(new ValueCapturingBeforeSaveCallback());

		template.setEntityCallbacks(EntityCallbacks.create(beforeConvertCallback, beforeSaveCallback));

		Person entity = new Person();
		entity.id = "init";
		entity.firstname = "luke";

		template.save(entity);

		verify(beforeConvertCallback).onBeforeConvert(eq(entity), anyString());
		verify(beforeSaveCallback).onBeforeSave(eq(entity), any(), anyString());
	}

	@Test // DATAMONGO-2261
	void insertShouldInvokeCallbacks() {

		ValueCapturingBeforeConvertCallback beforeConvertCallback = spy(new ValueCapturingBeforeConvertCallback());
		ValueCapturingBeforeSaveCallback beforeSaveCallback = spy(new ValueCapturingBeforeSaveCallback());

		template.setEntityCallbacks(EntityCallbacks.create(beforeConvertCallback, beforeSaveCallback));

		Person entity = new Person();
		entity.id = "init";
		entity.firstname = "luke";

		template.insert(entity);

		verify(beforeConvertCallback).onBeforeConvert(eq(entity), anyString());
		verify(beforeSaveCallback).onBeforeSave(eq(entity), any(), anyString());
	}

	@Test // DATAMONGO-2261
	void insertAllShouldInvokeCallbacks() {

		ValueCapturingBeforeConvertCallback beforeConvertCallback = spy(new ValueCapturingBeforeConvertCallback());
		ValueCapturingBeforeSaveCallback beforeSaveCallback = spy(new ValueCapturingBeforeSaveCallback());

		template.setEntityCallbacks(EntityCallbacks.create(beforeConvertCallback, beforeSaveCallback));

		Person entity1 = new Person();
		entity1.id = "1";
		entity1.firstname = "luke";

		Person entity2 = new Person();
		entity1.id = "2";
		entity1.firstname = "luke";

		template.insertAll(Arrays.asList(entity1, entity2));

		verify(beforeConvertCallback, times(2)).onBeforeConvert(any(), anyString());
		verify(beforeSaveCallback, times(2)).onBeforeSave(any(), any(), anyString());
	}

	@Test // DATAMONGO-2261
	void findAndReplaceShouldInvokeCallbacks() {

		ValueCapturingBeforeConvertCallback beforeConvertCallback = spy(new ValueCapturingBeforeConvertCallback());
		ValueCapturingBeforeSaveCallback beforeSaveCallback = spy(new ValueCapturingBeforeSaveCallback());

		template.setEntityCallbacks(EntityCallbacks.create(beforeConvertCallback, beforeSaveCallback));

		Person entity = new Person();
		entity.id = "init";
		entity.firstname = "luke";

		template.findAndReplace(new Query(), entity);

		verify(beforeConvertCallback).onBeforeConvert(eq(entity), anyString());
		verify(beforeSaveCallback).onBeforeSave(eq(entity), any(), anyString());
	}

	@Test // DATAMONGO-2261
	void publishesEventsAndEntityCallbacksInOrder() {

		BeforeConvertCallback<Person> beforeConvertCallback = new BeforeConvertCallback<Person>() {

			@Override
			public Person onBeforeConvert(Person entity, String collection) {

				assertThat(entity.id).isEqualTo("before-convert-event");
				entity.id = "before-convert-callback";
				return entity;
			}
		};

		BeforeSaveCallback<Person> beforeSaveCallback = new BeforeSaveCallback<Person>() {

			@Override
			public Person onBeforeSave(Person entity, Document document, String collection) {

				assertThat(entity.id).isEqualTo("before-save-event");
				entity.id = "before-save-callback";
				return entity;
			}
		};

		AbstractMongoEventListener<Person> eventListener = new AbstractMongoEventListener<Person>() {

			@Override
			public void onBeforeConvert(BeforeConvertEvent<Person> event) {

				assertThat(event.getSource().id).isEqualTo("init");
				event.getSource().id = "before-convert-event";
			}

			@Override
			public void onBeforeSave(BeforeSaveEvent<Person> event) {

				assertThat(event.getSource().id).isEqualTo("before-convert-callback");
				event.getSource().id = "before-save-event";
			}
		};

		StaticApplicationContext ctx = new StaticApplicationContext();
		ctx.registerBean(ApplicationListener.class, () -> eventListener);
		ctx.registerBean(BeforeConvertCallback.class, () -> beforeConvertCallback);
		ctx.registerBean(BeforeSaveCallback.class, () -> beforeSaveCallback);
		ctx.refresh();

		template.setApplicationContext(ctx);

		Person entity = new Person();
		entity.id = "init";
		entity.firstname = "luke";

		Person saved = template.save(entity);

		assertThat(saved.id).isEqualTo("before-save-callback");
	}

	@Test // DATAMONGO-2261
	void beforeSaveCallbackAllowsTargetDocumentModifications() {

		BeforeSaveCallback<Person> beforeSaveCallback = new BeforeSaveCallback<Person>() {

			@Override
			public Person onBeforeSave(Person entity, Document document, String collection) {

				document.append("added-by", "callback");
				return entity;
			}
		};

		StaticApplicationContext ctx = new StaticApplicationContext();
		ctx.registerBean(BeforeSaveCallback.class, () -> beforeSaveCallback);
		ctx.refresh();

		template.setApplicationContext(ctx);

		Person entity = new Person();
		entity.id = "luke-skywalker";
		entity.firstname = "luke";

		template.save(entity);

		ArgumentCaptor<Document> captor = ArgumentCaptor.forClass(Document.class);

		verify(collection).replaceOne(any(), captor.capture(), any(com.mongodb.client.model.ReplaceOptions.class));
		assertThat(captor.getValue()).containsEntry("added-by", "callback");
	}

	@Test // DATAMONGO-2307
	void beforeSaveCallbackAllowsTargetEntityModificationsUsingSave() {

		StaticApplicationContext ctx = new StaticApplicationContext();
		ctx.registerBean(BeforeSaveCallback.class, this::beforeSaveCallbackReturningNewPersonWithTransientAttribute);
		ctx.refresh();

		template.setApplicationContext(ctx);

		PersonWithTransientAttribute entity = new PersonWithTransientAttribute();
		entity.id = "luke-skywalker";
		entity.firstname = "luke";
		entity.isNew = true;

		PersonWithTransientAttribute savedPerson = template.save(entity);
		assertThat(savedPerson.isNew).isFalse();
	}

	@Test // DATAMONGO-2307
	void beforeSaveCallbackAllowsTargetEntityModificationsUsingInsert() {

		StaticApplicationContext ctx = new StaticApplicationContext();
		ctx.registerBean(BeforeSaveCallback.class, this::beforeSaveCallbackReturningNewPersonWithTransientAttribute);
		ctx.refresh();

		template.setApplicationContext(ctx);

		PersonWithTransientAttribute entity = new PersonWithTransientAttribute();
		entity.id = "luke-skywalker";
		entity.firstname = "luke";
		entity.isNew = true;

		PersonWithTransientAttribute savedPerson = template.insert(entity);
		assertThat(savedPerson.isNew).isFalse();
	}

	// TODO: additional tests for what is when saved.

	@Test // DATAMONGO-2261
	void entityCallbacksAreNotSetByDefault() {
		Assertions.assertThat(ReflectionTestUtils.getField(template, "entityCallbacks")).isNull();
	}

	@Test // DATAMONGO-2261
	void entityCallbacksShouldBeInitiatedOnSettingApplicationContext() {

		ApplicationContext ctx = new StaticApplicationContext();
		template.setApplicationContext(ctx);

		Assertions.assertThat(ReflectionTestUtils.getField(template, "entityCallbacks")).isNotNull();
	}

	@Test // DATAMONGO-2261
	void setterForEntityCallbackOverridesContextInitializedOnes() {

		ApplicationContext ctx = new StaticApplicationContext();
		template.setApplicationContext(ctx);

		EntityCallbacks callbacks = EntityCallbacks.create();
		template.setEntityCallbacks(callbacks);

		Assertions.assertThat(ReflectionTestUtils.getField(template, "entityCallbacks")).isSameAs(callbacks);
	}

	@Test // DATAMONGO-2261
	void setterForApplicationContextShouldNotOverrideAlreadySetEntityCallbacks() {

		EntityCallbacks callbacks = EntityCallbacks.create();
		ApplicationContext ctx = new StaticApplicationContext();

		template.setEntityCallbacks(callbacks);
		template.setApplicationContext(ctx);

		Assertions.assertThat(ReflectionTestUtils.getField(template, "entityCallbacks")).isSameAs(callbacks);
	}

	@Test // DATAMONGO-2344, DATAMONGO-2572
	void allowSecondaryReadsQueryOptionShouldApplyPrimaryPreferredReadPreferenceForFind() {

		template.find(new Query().allowSecondaryReads(), AutogenerateableId.class);

		verify(collection).withReadPreference(eq(ReadPreference.primaryPreferred()));
	}

	@Test // DATAMONGO-2344, DATAMONGO-2572
	void allowSecondaryReadsQueryOptionShouldApplyPrimaryPreferredReadPreferenceForFindOne() {

		template.findOne(new Query().allowSecondaryReads(), AutogenerateableId.class);

		verify(collection).withReadPreference(eq(ReadPreference.primaryPreferred()));
	}

	@Test // DATAMONGO-2344, DATAMONGO-2572
	void allowSecondaryReadsQueryOptionShouldApplyPrimaryPreferredReadPreferenceForFindDistinct() {

		template.findDistinct(new Query().allowSecondaryReads(), "name", AutogenerateableId.class, String.class);

		verify(collection).withReadPreference(eq(ReadPreference.primaryPreferred()));
	}

	@Test // DATAMONGO-2344, DATAMONGO-2572
	void allowSecondaryReadsQueryOptionShouldApplyPrimaryPreferredReadPreferenceForStream() {

		template.stream(new Query().allowSecondaryReads(), AutogenerateableId.class);
		verify(collection).withReadPreference(eq(ReadPreference.primaryPreferred()));
	}

	@Test // DATAMONGO-2331
	void updateShouldAllowAggregationExpressions() {

		AggregationUpdate update = AggregationUpdate.update().set("total")
				.toValue(ArithmeticOperators.valueOf("val1").sum().and("val2"));

		template.updateFirst(new BasicQuery("{}"), update, Wrapper.class);

		ArgumentCaptor<List<Document>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection, times(1)).updateOne(any(org.bson.Document.class), captor.capture(), any(UpdateOptions.class));

		assertThat(captor.getValue()).isEqualTo(
				Collections.singletonList(Document.parse("{ $set : { total : { $sum : [  \"$val1\",\"$val2\" ] } } }")));
	}

	@Test // DATAMONGO-2331
	void updateShouldAllowMultipleAggregationExpressions() {

		AggregationUpdate update = AggregationUpdate.update() //
				.set("average").toValue(ArithmeticOperators.valueOf("tests").avg()) //
				.set("grade").toValue(ConditionalOperators.switchCases( //
						CaseOperator.when(Gte.valueOf("average").greaterThanEqualToValue(90)).then("A"), //
						CaseOperator.when(Gte.valueOf("average").greaterThanEqualToValue(80)).then("B"), //
						CaseOperator.when(Gte.valueOf("average").greaterThanEqualToValue(70)).then("C"), //
						CaseOperator.when(Gte.valueOf("average").greaterThanEqualToValue(60)).then("D") //
				) //
						.defaultTo("F"));//

		template.updateFirst(new BasicQuery("{}"), update, Wrapper.class);

		ArgumentCaptor<List<Document>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection, times(1)).updateOne(any(org.bson.Document.class), captor.capture(), any(UpdateOptions.class));

		assertThat(captor.getValue()).containsExactly(Document.parse("{ $set: { average : { $avg: \"$tests\" } } }"),
				Document.parse("{ $set: { grade: { $switch: {\n" + "                           branches: [\n"
						+ "                               { case: { $gte: [ \"$average\", 90 ] }, then: \"A\" },\n"
						+ "                               { case: { $gte: [ \"$average\", 80 ] }, then: \"B\" },\n"
						+ "                               { case: { $gte: [ \"$average\", 70 ] }, then: \"C\" },\n"
						+ "                               { case: { $gte: [ \"$average\", 60 ] }, then: \"D\" }\n"
						+ "                           ],\n" + "                           default: \"F\"\n" + "     } } } }"));
	}

	@Test // DATAMONGO-2331
	void updateShouldMapAggregationExpressionToDomainType() {

		AggregationUpdate update = AggregationUpdate.update().set("name")
				.toValue(ArithmeticOperators.valueOf("val1").sum().and("val2"));

		template.updateFirst(new BasicQuery("{}"), update, Jedi.class);

		ArgumentCaptor<List<Document>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection, times(1)).updateOne(any(org.bson.Document.class), captor.capture(), any(UpdateOptions.class));

		assertThat(captor.getValue()).isEqualTo(
				Collections.singletonList(Document.parse("{ $set : { firstname : { $sum:[  \"$val1\",\"$val2\" ] } } }")));
	}

	@Test // DATAMONGO-2331
	void updateShouldPassOnUnsetCorrectly() {

		SetOperation setOperation = SetOperation.builder().set("status").toValue("Modified").and().set("comments")
				.toValue(Fields.fields("misc1").and("misc2").asList());
		AggregationUpdate update = AggregationUpdate.update();
		update.set(setOperation);
		update.unset("misc1", "misc2");

		template.updateFirst(new BasicQuery("{}"), update, Wrapper.class);

		ArgumentCaptor<List<Document>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection, times(1)).updateOne(any(org.bson.Document.class), captor.capture(), any(UpdateOptions.class));

		assertThat(captor.getValue()).isEqualTo(
				Arrays.asList(Document.parse("{ $set: { status: \"Modified\", comments: [ \"$misc1\", \"$misc2\" ] } }"),
						Document.parse("{ $unset: [ \"misc1\", \"misc2\" ] }")));
	}

	@Test // DATAMONGO-2331
	void updateShouldMapAggregationUnsetToDomainType() {

		AggregationUpdate update = AggregationUpdate.update();
		update.unset("name");

		template.updateFirst(new BasicQuery("{}"), update, Jedi.class);

		ArgumentCaptor<List<Document>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection, times(1)).updateOne(any(org.bson.Document.class), captor.capture(), any(UpdateOptions.class));

		assertThat(captor.getValue()).isEqualTo(Collections.singletonList(Document.parse("{ $unset : \"firstname\" }")));
	}

	@Test // DATAMONGO-2341
	void saveShouldAppendNonDefaultShardKeyIfNotPresentInFilter() {

		template.save(new ShardedEntityWithNonDefaultShardKey("id-1", "AT", 4230));

		ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
		verify(collection).replaceOne(filter.capture(), any(), any());

		assertThat(filter.getValue()).isEqualTo(new Document("_id", "id-1").append("country", "AT").append("userid", 4230));
	}

	@Test // DATAMONGO-2341
	void saveShouldAppendNonDefaultShardKeyToVersionedEntityIfNotPresentInFilter() {

		when(collection.replaceOne(any(), any(), any(com.mongodb.client.model.ReplaceOptions.class)))
				.thenReturn(UpdateResult.acknowledged(1, 1L, null));

		template.save(new ShardedVersionedEntityWithNonDefaultShardKey("id-1", 1L, "AT", 4230));

		ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
		verify(collection).replaceOne(filter.capture(), any(), any());

		assertThat(filter.getValue())
				.isEqualTo(new Document("_id", "id-1").append("version", 1L).append("country", "AT").append("userid", 4230));
	}

	@Test // DATAMONGO-2341
	void saveShouldAppendNonDefaultShardKeyFromExistingDocumentIfNotPresentInFilter() {

		when(findIterable.first()).thenReturn(new Document("_id", "id-1").append("country", "US").append("userid", 4230));

		template.save(new ShardedEntityWithNonDefaultShardKey("id-1", "AT", 4230));

		ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Document> replacement = ArgumentCaptor.forClass(Document.class);

		verify(collection).replaceOne(filter.capture(), replacement.capture(), any());

		assertThat(filter.getValue()).isEqualTo(new Document("_id", "id-1").append("country", "US").append("userid", 4230));
		assertThat(replacement.getValue()).containsEntry("country", "AT").containsEntry("userid", 4230);
	}

	@Test // DATAMONGO-2341
	void saveShouldAppendNonDefaultShardKeyFromGivenDocumentIfShardKeyIsImmutable() {

		template.save(new ShardedEntityWithNonDefaultImmutableShardKey("id-1", "AT", 4230));

		ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Document> replacement = ArgumentCaptor.forClass(Document.class);

		verify(collection).replaceOne(filter.capture(), replacement.capture(), any());

		assertThat(filter.getValue()).isEqualTo(new Document("_id", "id-1").append("country", "AT").append("userid", 4230));
		assertThat(replacement.getValue()).containsEntry("country", "AT").containsEntry("userid", 4230);

		verifyNoInteractions(findIterable);
	}

	@Test // DATAMONGO-2341
	void saveShouldAppendDefaultShardKeyIfNotPresentInFilter() {

		template.save(new ShardedEntityWithDefaultShardKey("id-1", "AT", 4230));

		ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
		verify(collection).replaceOne(filter.capture(), any(), any());

		assertThat(filter.getValue()).isEqualTo(new Document("_id", "id-1"));
		verify(findIterable, never()).first();
	}

	@Test // GH-3590
	void shouldIncludeValueFromNestedShardKeyPath() {

		WithShardKeyPointingToNested source = new WithShardKeyPointingToNested();
		source.id = "id-1";
		source.value = "v1";
		source.nested = new WithNamedFields();
		source.nested.customName = "cname";
		source.nested.name = "name";

		template.save(source);

		ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
		verify(collection).replaceOne(filter.capture(), any(), any());

		assertThat(filter.getValue())
				.isEqualTo(new Document("_id", "id-1").append("value", "v1").append("nested.custom-named-field", "cname"));
	}

	@Test // DATAMONGO-2341
	void saveShouldProjectOnShardKeyWhenLoadingExistingDocument() {

		when(findIterable.first()).thenReturn(new Document("_id", "id-1").append("country", "US").append("userid", 4230));

		template.save(new ShardedEntityWithNonDefaultShardKey("id-1", "AT", 4230));

		verify(findIterable).projection(new Document("country", 1).append("userid", 1));
	}

	@Test // DATAMONGO-2341
	void saveVersionedShouldProjectOnShardKeyWhenLoadingExistingDocument() {

		when(collection.replaceOne(any(), any(), any(com.mongodb.client.model.ReplaceOptions.class)))
				.thenReturn(UpdateResult.acknowledged(1, 1L, null));
		when(findIterable.first()).thenReturn(new Document("_id", "id-1").append("country", "US").append("userid", 4230));

		template.save(new ShardedVersionedEntityWithNonDefaultShardKey("id-1", 1L, "AT", 4230));

		verify(findIterable).projection(new Document("country", 1).append("userid", 1));
	}

	@Test // DATAMONGO-2479
	void findShouldInvokeAfterConvertCallback() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(findIterable.iterator()).thenReturn(new OneElementCursor<>(document));

		template.find(new Query(), Person.class);

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
	}

	@Test // DATAMONGO-2479
	void findByIdShouldInvokeAfterConvertCallback() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(findIterable.first()).thenReturn(document);

		template.findById("init", Person.class);

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
	}

	@Test // DATAMONGO-2479
	void findOneShouldInvokeAfterConvertCallback() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(findIterable.first()).thenReturn(document);

		template.findOne(new Query(), Person.class);

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
	}

	@Test // DATAMONGO-2479
	void findAllShouldInvokeAfterConvertCallback() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(findIterable.iterator()).thenReturn(new OneElementCursor<>(document));

		template.findAll(Person.class);

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
	}

	@Test // DATAMONGO-2479
	void findAndModifyShouldInvokeAfterConvertCallback() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.findOneAndUpdate(any(Bson.class), any(Bson.class), any())).thenReturn(document);

		template.findAndModify(new Query(), new Update(), Person.class);

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
	}

	@Test // DATAMONGO-2479
	void findAndRemoveShouldInvokeAfterConvertCallback() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.findOneAndDelete(any(Bson.class), any())).thenReturn(document);

		template.findAndRemove(new Query(), Person.class);

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
	}

	@Test // DATAMONGO-2479
	void findAllAndRemoveShouldInvokeAfterConvertCallback() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(findIterable.iterator()).thenReturn(new OneElementCursor<>(document));

		template.findAllAndRemove(new Query(), Person.class);

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
	}

	@Test // DATAMONGO-2479
	void findAndReplaceShouldInvokeAfterConvertCallbacks() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterConvertCallback));

		Person entity = new Person("init", "luke");

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.findOneAndReplace(any(Bson.class), any(Document.class), any())).thenReturn(document);

		Person saved = template.findAndReplace(new Query(), entity);

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
		assertThat(saved.id).isEqualTo("after-convert");
	}

	@Test // DATAMONGO-2479
	void saveShouldInvokeAfterSaveCallbacks() {

		ValueCapturingAfterSaveCallback afterSaveCallback = spy(new ValueCapturingAfterSaveCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterSaveCallback));

		Person entity = new Person("init", "luke");

		Person saved = template.save(entity);

		verify(afterSaveCallback).onAfterSave(eq(entity), any(), anyString());
		assertThat(saved.id).isEqualTo("after-save");
	}

	@Test // DATAMONGO-2479
	void insertShouldInvokeAfterSaveCallbacks() {

		ValueCapturingAfterSaveCallback afterSaveCallback = spy(new ValueCapturingAfterSaveCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterSaveCallback));

		Person entity = new Person("init", "luke");

		Person saved = template.insert(entity);

		verify(afterSaveCallback).onAfterSave(eq(entity), any(), anyString());
		assertThat(saved.id).isEqualTo("after-save");
	}

	@Test // DATAMONGO-2479
	void insertAllShouldInvokeAfterSaveCallbacks() {

		ValueCapturingAfterSaveCallback afterSaveCallback = spy(new ValueCapturingAfterSaveCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterSaveCallback));

		Person entity1 = new Person();
		entity1.id = "1";
		entity1.firstname = "luke";

		Person entity2 = new Person();
		entity1.id = "2";
		entity1.firstname = "luke";

		Collection<Person> saved = template.insertAll(Arrays.asList(entity1, entity2));

		verify(afterSaveCallback, times(2)).onAfterSave(any(), any(), anyString());
		assertThat(saved.iterator().next().getId()).isEqualTo("after-save");
	}

	@Test // DATAMONGO-2479
	void findAndReplaceShouldInvokeAfterSaveCallbacks() {

		ValueCapturingAfterSaveCallback afterSaveCallback = spy(new ValueCapturingAfterSaveCallback());

		template.setEntityCallbacks(EntityCallbacks.create(afterSaveCallback));

		Person entity = new Person("init", "luke");

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.findOneAndReplace(any(Bson.class), any(Document.class), any())).thenReturn(document);

		Person saved = template.findAndReplace(new Query(), entity);

		verify(afterSaveCallback).onAfterSave(eq(new Person("init", "luke")), any(), anyString());
		assertThat(saved.id).isEqualTo("after-save");
	}

	@Test // DATAMONGO-2479
	void findAndReplaceShouldEmitAfterSaveEvent() {

		AbstractMongoEventListener<Person> eventListener = new AbstractMongoEventListener<Person>() {

			@Override
			public void onAfterSave(AfterSaveEvent<Person> event) {

				assertThat(event.getSource().id).isEqualTo("init");
				event.getSource().id = "after-save-event";
			}
		};

		StaticApplicationContext ctx = new StaticApplicationContext();
		ctx.registerBean(ApplicationListener.class, () -> eventListener);
		ctx.refresh();

		template.setApplicationContext(ctx);

		Person entity = new Person("init", "luke");

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.findOneAndReplace(any(Bson.class), any(Document.class), any())).thenReturn(document);

		Person saved = template.findAndReplace(new Query(), entity);

		assertThat(saved.id).isEqualTo("after-save-event");
	}

	@Test // DATAMONGO-2556
	void esitmatedCountShouldBeDelegatedCorrectly() {

		template.estimatedCount(Person.class);

		verify(db).getCollection("star-wars", Document.class);
		verify(collection).estimatedDocumentCount(any());
	}

	@Test // GH-2911
	void insertErrorsOnCustomIteratorImplementation() {

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> template.insert(new TypeImplementingIterator()));
	}

	@Test // GH-3570
	void saveErrorsOnCollectionLikeObjects() {

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> template.save(new ArrayList<>(Arrays.asList(1, 2, 3)), "myList"));
	}

	@Test // GH-3731
	void createCollectionShouldSetUpTimeSeriesWithDefaults() {

		template.createCollection(TimeSeriesTypeWithDefaults.class);

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getTimeSeriesOptions().toString())
				.isEqualTo(new com.mongodb.client.model.TimeSeriesOptions("timestamp").toString());
	}

	@Test // GH-3731
	void createCollectionShouldSetUpTimeSeries() {

		template.createCollection(TimeSeriesType.class);

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getTimeSeriesOptions().toString())
				.isEqualTo(new com.mongodb.client.model.TimeSeriesOptions("time_stamp").metaField("meta")
						.granularity(TimeSeriesGranularity.HOURS).toString());
	}

	@Test // GH-3522
	void usedCountDocumentsForEmptyQueryByDefault() {

		template.count(new Query(), Human.class);

		verify(collection).countDocuments(any(Document.class), any());
	}

	@Test // GH-3522
	void delegatesToEstimatedCountForEmptyQueryIfEnabled() {

		template.useEstimatedCount(true);

		template.count(new Query(), Human.class);

		verify(collection).estimatedDocumentCount(any());
	}

	@Test // GH-3522
	void stillUsesCountDocumentsForNonEmptyQueryEvenIfEstimationEnabled() {

		template.useEstimatedCount(true);

		template.count(new BasicQuery("{ 'spring' : 'data-mongodb' }"), Human.class);

		verify(collection).countDocuments(any(Document.class), any());
	}

	@Test // GH-4374
	void countConsidersMaxTimeMs() {

		template.count(new BasicQuery("{ 'spring' : 'data-mongodb' }").maxTimeMsec(5000), Human.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(Document.class), options.capture());
		assertThat(options.getValue().getMaxTime(TimeUnit.MILLISECONDS)).isEqualTo(5000);
	}

	@Test // GH-4374
	void countPassesOnComment() {

		template.count(new BasicQuery("{ 'spring' : 'data-mongodb' }").comment("rocks!"), Human.class);

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(Document.class), options.capture());
		assertThat(options.getValue().getComment()).isEqualTo(BsonUtils.simpleToBsonValue("rocks!"));
	}

	@Test // GH-3984
	void templatePassesOnTimeSeriesOptionsWhenNoTypeGiven() {

		template.createCollection("time-series-collection", CollectionOptions.timeSeries("time_stamp"));

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getTimeSeriesOptions().toString())
				.isEqualTo(new com.mongodb.client.model.TimeSeriesOptions("time_stamp").toString());
	}

	@Test // GH-4300
	void findAndReplaceAllowsDocumentSourceType() {

		template.findAndReplace(new Query(), new Document("spring", "data"), FindAndReplaceOptions.options().upsert(),
				Document.class, "coll-1", Person.class);

		verify(db).getCollection(eq("coll-1"), eq(Document.class));
		verify(collection).findOneAndReplace((Bson) any(Bson.class), eq(new Document("spring", "data")),
				any(FindOneAndReplaceOptions.class));
	}

	@Test // GH-4462
	void replaceShouldUseCollationWhenPresent() {

		template.replace(new BasicQuery("{}").collation(Collation.of("fr")), new AutogenerateableId());

		ArgumentCaptor<com.mongodb.client.model.ReplaceOptions> options = ArgumentCaptor
				.forClass(com.mongodb.client.model.ReplaceOptions.class);
		verify(collection).replaceOne(any(), any(), options.capture());

		assertThat(options.getValue().isUpsert()).isFalse();
		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // GH-4462
	void replaceShouldNotUpsertByDefault() {

		template.replace(new BasicQuery("{}"), new Sith());

		ArgumentCaptor<com.mongodb.client.model.ReplaceOptions> options = ArgumentCaptor
				.forClass(com.mongodb.client.model.ReplaceOptions.class);
		verify(collection).replaceOne(any(), any(), options.capture());

		assertThat(options.getValue().isUpsert()).isFalse();
	}

	@Test // GH-4462
	void replaceShouldUpsert() {

		template.replace(new BasicQuery("{}"), new Sith(), ReplaceOptions.replaceOptions().upsert());

		ArgumentCaptor<com.mongodb.client.model.ReplaceOptions> options = ArgumentCaptor
				.forClass(com.mongodb.client.model.ReplaceOptions.class);
		verify(collection).replaceOne(any(), any(), options.capture());

		assertThat(options.getValue().isUpsert()).isTrue();
	}

	@Test // GH-4462
	void replaceShouldUseDefaultCollationWhenPresent() {

		template.replace(new BasicQuery("{}"), new Sith(), ReplaceOptions.replaceOptions());

		ArgumentCaptor<com.mongodb.client.model.ReplaceOptions> options = ArgumentCaptor
				.forClass(com.mongodb.client.model.ReplaceOptions.class);
		verify(collection).replaceOne(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("de_AT");
	}

	@Test // GH-4462
	void replaceShouldUseHintIfPresent() {

		template.replace(new BasicQuery("{}").withHint("index-to-use"), new Sith(), ReplaceOptions.replaceOptions().upsert());

		ArgumentCaptor<com.mongodb.client.model.ReplaceOptions> options = ArgumentCaptor
				.forClass(com.mongodb.client.model.ReplaceOptions.class);
		verify(collection).replaceOne(any(), any(), options.capture());

		assertThat(options.getValue().getHintString()).isEqualTo("index-to-use");
	}

	@Test // GH-4462
	void replaceShouldApplyWriteConcern() {

		template.setWriteConcernResolver(new WriteConcernResolver() {
			public WriteConcern resolve(MongoAction action) {

				assertThat(action.getMongoActionOperation()).isEqualTo(MongoActionOperation.REPLACE);
				return WriteConcern.UNACKNOWLEDGED;
			}
		});

		template.replace(new BasicQuery("{}").withHint("index-to-use"), new Sith(), ReplaceOptions.replaceOptions().upsert());

		verify(collection).withWriteConcern(eq(WriteConcern.UNACKNOWLEDGED));
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

		@Field(write = Field.Write.ALWAYS) String name;
	}

	enum MyConverter implements Converter<AutogenerateableId, String> {

		INSTANCE;

		public String convert(AutogenerateableId source) {
			return source.toString();
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = "star-wars")
	static class Person {

		@Id String id;
		String firstname;

		public Person() {
		}

		public Person(String id, String firstname) {
			this.id = id;
			this.firstname = firstname;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Person person = (Person) o;
			return Objects.equals(id, person.id) && Objects.equals(firstname, person.firstname);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, firstname);
		}
	}

	static class PersonExtended extends Person {

		String lastname;
	}

	static class PersonWithTransientAttribute extends Person {

		@Transient boolean isNew = true;
	}

	interface PersonProjection {
		String getFirstname();
	}

	public interface PersonSpELProjection {

		@Value("#{target.firstname}")
		String getName();
	}

	static class Human {
		@Id String id;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
	}

	static class Jedi {

		@Field("firstname") String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
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

	@Sharded(shardKey = { "value", "nested.customName" })
	static class WithShardKeyPointingToNested {
		String id;
		String value;
		WithNamedFields nested;
	}

	@TimeSeries(timeField = "timestamp")
	static class TimeSeriesTypeWithDefaults {

		String id;
		Instant timestamp;
	}

	@TimeSeries(timeField = "timestamp", metaField = "meta", granularity = Granularity.HOURS)
	static class TimeSeriesType {

		String id;

		@Field("time_stamp") Instant timestamp;
		Object meta;
	}

	static class TypeImplementingIterator implements Iterator {

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public Object next() {
			return null;
		}
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

	@Override
	protected MongoOperations getOperationsForExceptionHandling() {
		when(template.getMongoDatabaseFactory().getMongoDatabase()).thenThrow(new MongoException("Error"));
		return template;
	}

	@Override
	protected MongoOperations getOperations() {
		return this.template;
	}

	private BeforeSaveCallback<PersonWithTransientAttribute> beforeSaveCallbackReturningNewPersonWithTransientAttribute() {
		return (entity, document, collection) -> {

			// Return a completely new instance, ie in case of an immutable entity;
			PersonWithTransientAttribute newEntity = new PersonWithTransientAttribute();
			newEntity.id = entity.id;
			newEntity.firstname = entity.firstname;
			newEntity.isNew = false;
			return newEntity;
		};
	}

	static class ValueCapturingEntityCallback<T> {

		private final List<T> values = new ArrayList<>(1);

		protected void capture(T value) {
			values.add(value);
		}

		public List<T> getValues() {
			return values;
		}

		@Nullable
		public T getValue() {
			return CollectionUtils.lastElement(values);
		}

	}

	static class ValueCapturingBeforeConvertCallback extends ValueCapturingEntityCallback<Person>
			implements BeforeConvertCallback<Person> {

		@Override
		public Person onBeforeConvert(Person entity, String collection) {

			capture(entity);
			return entity;
		}
	}

	static class ValueCapturingBeforeSaveCallback extends ValueCapturingEntityCallback<Person>
			implements BeforeSaveCallback<Person> {

		@Override
		public Person onBeforeSave(Person entity, Document document, String collection) {

			capture(entity);
			return entity;
		}
	}

	static class ValueCapturingAfterSaveCallback extends ValueCapturingEntityCallback<Person>
			implements AfterSaveCallback<Person> {

		@Override
		public Person onAfterSave(Person entity, Document document, String collection) {

			capture(entity);
			return new Person() {
				{
					id = "after-save";
					firstname = entity.firstname;
				}
			};
		}
	}

	static class ValueCapturingAfterConvertCallback extends ValueCapturingEntityCallback<Person>
			implements AfterConvertCallback<Person> {

		@Override
		public Person onAfterConvert(Person entity, Document document, String collection) {

			capture(entity);
			return new Person() {
				{
					id = "after-convert";
					firstname = entity.firstname;
				}
			};
		}
	}

	static class OneElementCursor<T> implements MongoCursor<T> {
		private final Iterator<T> iterator;

		OneElementCursor(T element) {
			iterator = Collections.singletonList(element).iterator();
		}

		@Override
		public void close() {
			// nothing to close
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public T next() {
			return iterator.next();
		}

		@Override
		public int available() {
			return 1;
		}

		@Override
		public T tryNext() {
			if (iterator.hasNext()) {
				return iterator.next();
			} else {
				return null;
			}
		}

		@Override
		public ServerCursor getServerCursor() {
			throw new IllegalStateException("Not implemented");
		}

		@Override
		public ServerAddress getServerAddress() {
			throw new IllegalStateException("Not implemented");
		}
	}
}
