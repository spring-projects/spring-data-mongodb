/*
 * Copyright 2016-2023 the original author or authors.
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
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.test.util.Assertions.assertThat;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoTemplateUnitTests.AutogenerateableId;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.aggregation.ArithmeticOperators;
import org.springframework.data.mongodb.core.aggregation.ComparisonOperators.Gte;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Switch.CaseOperator;
import org.springframework.data.mongodb.core.aggregation.Fields;
import org.springframework.data.mongodb.core.aggregation.SetOperation;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.TimeSeries;
import org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.ReactiveAfterConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveAfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeSaveCallback;
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
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Unit tests for {@link ReactiveMongoTemplate}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Roman Puchkovskiy
 * @author Mathieu Ouellet
 * @author Yadhukrishna S Pai
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ReactiveMongoTemplateUnitTests {

	private ReactiveMongoTemplate template;

	@Mock SimpleReactiveMongoDatabaseFactory factory;
	@Mock MongoClient mongoClient;
	@Mock MongoDatabase db;
	@Mock MongoCollection collection;
	@Mock FindPublisher findPublisher;
	@Mock AggregatePublisher aggregatePublisher;
	@Mock Publisher runCommandPublisher;
	@Mock Publisher<UpdateResult> updateResultPublisher;
	@Mock Publisher findAndUpdatePublisher;
	@Mock Publisher<Void> successPublisher;
	@Mock DistinctPublisher distinctPublisher;
	@Mock Publisher deletePublisher;
	@Mock MapReducePublisher mapReducePublisher;
	@Mock ChangeStreamPublisher changeStreamPublisher;

	private MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	private MappingMongoConverter converter;
	private MongoMappingContext mappingContext;

	@BeforeEach
	void beforeEach() {

		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslator);
		when(factory.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
		when(factory.getMongoDatabase()).thenReturn(Mono.just(db));
		when(db.getCollection(any())).thenReturn(collection);
		when(db.getCollection(any(), any())).thenReturn(collection);
		when(db.runCommand(any(), any(Class.class))).thenReturn(runCommandPublisher);
		when(db.createCollection(any(), any(CreateCollectionOptions.class))).thenReturn(runCommandPublisher);
		when(collection.withReadPreference(any())).thenReturn(collection);
		when(collection.withReadConcern(any())).thenReturn(collection);
		when(collection.find(any(Class.class))).thenReturn(findPublisher);
		when(collection.find(any(Document.class), any(Class.class))).thenReturn(findPublisher);
		when(collection.aggregate(anyList())).thenReturn(aggregatePublisher);
		when(collection.aggregate(anyList(), any(Class.class))).thenReturn(aggregatePublisher);
		when(collection.countDocuments(any(), any(CountOptions.class))).thenReturn(Mono.just(0L));
		when(collection.estimatedDocumentCount(any())).thenReturn(Mono.just(0L));
		when(collection.updateOne(any(), any(Bson.class), any(UpdateOptions.class))).thenReturn(updateResultPublisher);
		when(collection.updateMany(any(Bson.class), any(Bson.class), any())).thenReturn(updateResultPublisher);
		when(collection.updateOne(any(), anyList(), any())).thenReturn(updateResultPublisher);
		when(collection.updateMany(any(), anyList(), any())).thenReturn(updateResultPublisher);
		when(collection.findOneAndUpdate(any(), any(Bson.class), any(FindOneAndUpdateOptions.class)))
				.thenReturn(findAndUpdatePublisher);
		when(collection.findOneAndReplace(any(Bson.class), any(), any())).thenReturn(findPublisher);
		when(collection.findOneAndDelete(any(), any(FindOneAndDeleteOptions.class))).thenReturn(findPublisher);
		when(collection.distinct(anyString(), any(Document.class), any())).thenReturn(distinctPublisher);
		when(collection.deleteMany(any(Bson.class), any())).thenReturn(deletePublisher);
		when(collection.findOneAndUpdate(any(), any(Bson.class), any(FindOneAndUpdateOptions.class)))
				.thenReturn(findAndUpdatePublisher);
		when(collection.mapReduce(anyString(), anyString(), any())).thenReturn(mapReducePublisher);
		when(collection.replaceOne(any(Bson.class), any(), any(ReplaceOptions.class))).thenReturn(updateResultPublisher);
		when(collection.insertOne(any(Bson.class))).thenReturn(successPublisher);
		when(collection.insertMany(anyList())).thenReturn(successPublisher);
		when(findPublisher.projection(any())).thenReturn(findPublisher);
		when(findPublisher.limit(anyInt())).thenReturn(findPublisher);
		when(findPublisher.collation(any())).thenReturn(findPublisher);
		when(findPublisher.first()).thenReturn(findPublisher);
		when(findPublisher.allowDiskUse(anyBoolean())).thenReturn(findPublisher);
		when(aggregatePublisher.allowDiskUse(anyBoolean())).thenReturn(aggregatePublisher);
		when(aggregatePublisher.collation(any())).thenReturn(aggregatePublisher);
		when(aggregatePublisher.maxTime(anyLong(), any())).thenReturn(aggregatePublisher);
		when(aggregatePublisher.first()).thenReturn(findPublisher);

		this.mappingContext = new MongoMappingContext();
		this.mappingContext.setSimpleTypeHolder(new MongoCustomConversions(Collections.emptyList()).getSimpleTypeHolder());
		this.converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		this.template = new ReactiveMongoTemplate(factory, converter);
	}

	@Test // DATAMONGO-1444
	void rejectsNullDatabaseName() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ReactiveMongoTemplate(mongoClient, null));
	}

	@Test // DATAMONGO-1444
	void rejectsNullMongo() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ReactiveMongoTemplate(null, "database"));
	}

	@Test // DATAMONGO-1444
	void defaultsConverterToMappingMongoConverter() throws Exception {
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoClient, "database");
		assertThat(ReflectionTestUtils.getField(template, "mongoConverter") instanceof MappingMongoConverter).isTrue();
	}

	@Test // DATAMONGO-1912
	void autogeneratesIdForMap() {

		ReactiveMongoTemplate template = spy(this.template);
		doReturn(Mono.just(new ObjectId())).when(template).saveDocument(any(String.class), any(Document.class),
				any(Class.class));

		Map<String, String> entity = new LinkedHashMap<>();
		template.save(entity, "foo").as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(entity).containsKey("_id");
		}).verifyComplete();
	}

	@Test // DATAMONGO-1311
	void executeQueryShouldUseBatchSizeWhenPresent() {

		when(findPublisher.batchSize(anyInt())).thenReturn(findPublisher);

		Query query = new Query().cursorBatchSize(1234);
		template.find(query, Person.class).subscribe();

		verify(findPublisher).batchSize(1234);
	}

	@Test // DATAMONGO-2659
	void executeQueryShouldUseAllowDiskSizeWhenPresent() {

		when(findPublisher.batchSize(anyInt())).thenReturn(findPublisher);

		Query query = new Query().allowDiskUse(true);
		template.find(query, Person.class).subscribe();

		verify(findPublisher).allowDiskUse(true);
	}

	@Test // DATAMONGO-1518
	void findShouldUseCollationWhenPresent() {

		template.find(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	//
	@Test // DATAMONGO-1518
	void findOneShouldUseCollationWhenPresent() {

		template.findOne(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	void existsShouldUseCollationWhenPresent() {

		template.exists(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	void findAndModfiyShoudUseCollationWhenPresent() {

		when(collection.findOneAndUpdate(any(Bson.class), any(Bson.class), any())).thenReturn(Mono.empty());

		template.findAndModify(new BasicQuery("{}").collation(Collation.of("fr")), new Update(), AutogenerateableId.class)
				.subscribe();

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518
	void findAndRemoveShouldUseCollationWhenPresent() {

		when(collection.findOneAndDelete(any(Bson.class), any())).thenReturn(Mono.empty());

		template.findAndRemove(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class).subscribe();

		ArgumentCaptor<FindOneAndDeleteOptions> options = ArgumentCaptor.forClass(FindOneAndDeleteOptions.class);
		verify(collection).findOneAndDelete(any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518
	void findAndRemoveManyShouldUseCollationWhenPresent() {

		when(collection.deleteMany(any(Bson.class), any())).thenReturn(Mono.empty());

		template.doRemove("collection-1", new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class)
				.subscribe();

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518
	void updateOneShouldUseCollationWhenPresent() {

		when(collection.updateOne(any(Bson.class), any(Bson.class), any())).thenReturn(Mono.empty());

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), new Update().set("foo", "bar"),
				AutogenerateableId.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518
	void updateManyShouldUseCollationWhenPresent() {

		when(collection.updateMany(any(Bson.class), any(Bson.class), any())).thenReturn(Mono.empty());

		template.updateMulti(new BasicQuery("{}").collation(Collation.of("fr")), new Update().set("foo", "bar"),
				AutogenerateableId.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // GH-3218
	void updateUsesHintStringFromQuery() {

		template.updateFirst(new Query().withHint("index-1"), new Update().set("spring", "data"), Person.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(Bson.class), any(Bson.class), options.capture());

		assertThat(options.getValue().getHintString()).isEqualTo("index-1");
	}

	@Test // GH-3218
	void updateUsesHintDocumentFromQuery() {

		template.updateFirst(new Query().withHint("{ firstname : 1 }"), new Update().set("spring", "data"), Person.class)
				.subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(Bson.class), any(Bson.class), options.capture());

		assertThat(options.getValue().getHint()).isEqualTo(new Document("firstname", 1));
	}

	@Test // DATAMONGO-1518
	void replaceOneShouldUseCollationWhenPresent() {

		when(collection.replaceOne(any(Bson.class), any(), any(ReplaceOptions.class))).thenReturn(Mono.empty());

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), new Update(), AutogenerateableId.class)
				.subscribe();

		ArgumentCaptor<ReplaceOptions> options = ArgumentCaptor.forClass(ReplaceOptions.class);
		verify(collection).replaceOne(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518, DATAMONGO-2257
	void mapReduceShouldUseCollationWhenPresent() {

		template.mapReduce(new BasicQuery("{}"), AutogenerateableId.class, AutogenerateableId.class, "", "",
				MapReduceOptions.options().collation(Collation.of("fr"))).subscribe();

		verify(mapReducePublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518, DATAMONGO-2264
	void geoNearShouldUseCollationWhenPresent() {

		NearQuery query = NearQuery.near(0D, 0D).query(new BasicQuery("{}").collation(Collation.of("fr")));
		template.geoNear(query, AutogenerateableId.class).subscribe();

		verify(aggregatePublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // GH-4277
	void geoNearShouldHonorReadPreferenceFromQuery() {

		NearQuery query = NearQuery.near(new Point(1, 1));
		query.withReadPreference(ReadPreference.secondary());

		template.geoNear(query, Wrapper.class).subscribe();

		verify(collection).withReadPreference(eq(ReadPreference.secondary()));
	}

	@Test // GH-4277
	void geoNearShouldHonorReadConcernFromQuery() {

		NearQuery query = NearQuery.near(new Point(1, 1));
		query.withReadConcern(ReadConcern.SNAPSHOT);

		template.geoNear(query, Wrapper.class).subscribe();

		verify(collection).withReadConcern(eq(ReadConcern.SNAPSHOT));
	}

	@Test // DATAMONGO-1719
	void appliesFieldsWhenInterfaceProjectionIsClosedAndQueryDoesNotDefineFields() {

		template.doFind("star-wars", CollectionPreparer.identity(), new Document(), new Document(), Person.class,
				PersonProjection.class, FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher).projection(eq(new Document("firstname", 1)));
	}

	@Test // DATAMONGO-1719
	void doesNotApplyFieldsWhenInterfaceProjectionIsClosedAndQueryDefinesFields() {

		template.doFind("star-wars", CollectionPreparer.identity(), new Document(), new Document("bar", 1), Person.class,
				PersonProjection.class, FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher).projection(eq(new Document("bar", 1)));
	}

	@Test // DATAMONGO-1719
	void doesNotApplyFieldsWhenInterfaceProjectionIsOpen() {

		template.doFind("star-wars", CollectionPreparer.identity(), new Document(), new Document(), Person.class,
				PersonSpELProjection.class, FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher, never()).projection(any());
	}

	@Test // DATAMONGO-1719, DATAMONGO-2041
	void appliesFieldsToDtoProjection() {

		template.doFind("star-wars", CollectionPreparer.identity(), new Document(), new Document(), Person.class,
				Jedi.class, FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher).projection(eq(new Document("firstname", 1)));
	}

	@Test // DATAMONGO-1719
	void doesNotApplyFieldsToDtoProjectionWhenQueryDefinesFields() {

		template.doFind("star-wars", CollectionPreparer.identity(), new Document(), new Document("bar", 1), Person.class,
				Jedi.class, FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher).projection(eq(new Document("bar", 1)));
	}

	@Test // DATAMONGO-1719
	void doesNotApplyFieldsWhenTargetIsNotAProjection() {

		template.doFind("star-wars", CollectionPreparer.identity(), new Document(), new Document(), Person.class,
				Person.class, FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher, never()).projection(any());
	}

	@Test // DATAMONGO-1719
	void doesNotApplyFieldsWhenTargetExtendsDomainType() {

		template.doFind("star-wars", CollectionPreparer.identity(), new Document(), new Document(), Person.class,
				PersonExtended.class, FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher, never()).projection(any());
	}

	@Test // DATAMONGO-1783
	void countShouldUseSkipFromQuery() {

		template.count(new Query().skip(10), Person.class, "star-wars").subscribe();

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getSkip()).isEqualTo(10);
	}

	@Test // DATAMONGO-1783
	void countShouldUseLimitFromQuery() {

		template.count(new Query().limit(100), Person.class, "star-wars").subscribe();

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getLimit()).isEqualTo(100);
	}

	@Test // DATAMONGO-2360
	void countShouldApplyQueryHintIfPresent() {

		Document queryHint = new Document("age", 1);
		template.count(new Query().withHint(queryHint), Person.class, "star-wars").subscribe();

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getHint()).isEqualTo(queryHint);
	}

	@Test // DATAMONGO-2365
	void countShouldApplyQueryHintAsIndexNameIfPresent() {

		template.count(new Query().withHint("idx-1"), Person.class, "star-wars").subscribe();

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getHintString()).isEqualTo("idx-1");
	}

	@Test // DATAMONGO-2215
	void updateShouldApplyArrayFilters() {

		template.updateFirst(new BasicQuery("{}"),
				new Update().set("grades.$[element]", 100).filterArray(Criteria.where("element").gte(100)),
				EntityWithListOfSimple.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		Assertions.assertThat((List<Bson>) options.getValue().getArrayFilters())
				.contains(new org.bson.Document("element", new Document("$gte", 100)));
	}

	@Test // DATAMONGO-2215
	void findAndModifyShouldApplyArrayFilters() {

		template.findAndModify(new BasicQuery("{}"),
				new Update().set("grades.$[element]", 100).filterArray(Criteria.where("element").gte(100)),
				EntityWithListOfSimple.class).subscribe();

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(Bson.class), options.capture());

		Assertions.assertThat((List<Bson>) options.getValue().getArrayFilters())
				.contains(new org.bson.Document("element", new Document("$gte", 100)));
	}

	@Test // DATAMONGO-1854
	void findShouldNotUseCollationWhenNoDefaultPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}"), Jedi.class).subscribe();

		verify(findPublisher, never()).collation(any());
	}

	@Test // DATAMONGO-1854
	void findShouldUseDefaultCollationWhenPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}"), Sith.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void findOneShouldUseDefaultCollationWhenPresent() {

		template.findOne(new BasicQuery("{'foo' : 'bar'}"), Sith.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void existsShouldUseDefaultCollationWhenPresent() {

		template.exists(new BasicQuery("{}"), Sith.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void findAndModfiyShoudUseDefaultCollationWhenPresent() {

		template.findAndModify(new BasicQuery("{}"), new Update(), Sith.class).subscribe();

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void findAndRemoveShouldUseDefaultCollationWhenPresent() {

		template.findAndRemove(new BasicQuery("{}"), Sith.class).subscribe();

		ArgumentCaptor<FindOneAndDeleteOptions> options = ArgumentCaptor.forClass(FindOneAndDeleteOptions.class);
		verify(collection).findOneAndDelete(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void createCollectionShouldNotCollationIfNotPresent() {

		template.createCollection(AutogenerateableId.class).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		Assertions.assertThat(options.getValue().getCollation()).isNull();
	}

	@Test // DATAMONGO-1854
	void createCollectionShouldApplyDefaultCollation() {

		template.createCollection(Sith.class).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void createCollectionShouldFavorExplicitOptionsOverDefaultCollation() {

		template.createCollection(Sith.class, CollectionOptions.just(Collation.of("en_US"))).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("en_US").build());
	}

	@Test // DATAMONGO-1854
	void createCollectionShouldUseDefaultCollationIfCollectionOptionsAreNull() {

		template.createCollection(Sith.class, null).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void aggreateShouldUseDefaultCollationIfPresent() {

		template.aggregate(newAggregation(Sith.class, project("id")), AutogenerateableId.class, Document.class).subscribe();

		verify(aggregatePublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // GH-4277
	void aggreateShouldUseReadConcern() {

		AggregationOptions options = AggregationOptions.builder().readConcern(ReadConcern.SNAPSHOT).build();
		template.aggregate(newAggregation(Sith.class, project("id")).withOptions(options), AutogenerateableId.class,
				Document.class).subscribe();

		verify(collection).withReadConcern(ReadConcern.SNAPSHOT);
	}

	@Test // GH-4286
	void aggreateShouldUseReadReadPreference() {

		AggregationOptions options = AggregationOptions.builder().readPreference(ReadPreference.primaryPreferred()).build();
		template.aggregate(newAggregation(Sith.class, project("id")).withOptions(options), AutogenerateableId.class,
				Document.class).subscribe();

		verify(collection).withReadPreference(ReadPreference.primaryPreferred());
	}

	@Test // GH-4543
	void aggregateDoesNotLimitBackpressure() {

		reset(collection);

		AtomicLong request = new AtomicLong();
		Publisher<Document> realPublisher = Flux.just(new Document()).doOnRequest(request::addAndGet);

		doAnswer(invocation -> {
			Subscriber<Document> subscriber = invocation.getArgument(0);
			realPublisher.subscribe(subscriber);
			return null;
		}).when(aggregatePublisher).subscribe(any());

		when(collection.aggregate(anyList())).thenReturn(aggregatePublisher);
		when(collection.aggregate(anyList(), any(Class.class))).thenReturn(aggregatePublisher);

		template.aggregate(newAggregation(Sith.class, project("id")), AutogenerateableId.class, Document.class).subscribe();

		assertThat(request).hasValueGreaterThan(128);
	}

	@Test // DATAMONGO-1854
	void aggreateShouldUseCollationFromOptionsEvenIfDefaultCollationIsPresent() {

		template
				.aggregate(
						newAggregation(Sith.class, project("id"))
								.withOptions(newAggregationOptions().collation(Collation.of("fr")).build()),
						AutogenerateableId.class, Document.class)
				.subscribe();

		verify(aggregatePublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-2153
	void aggregateShouldHonorOptionsComment() {

		AggregationOptions options = AggregationOptions.builder().comment("expensive").build();

		template.aggregate(newAggregation(Sith.class, project("id")).withOptions(options), AutogenerateableId.class,
				Document.class).subscribe();

		verify(aggregatePublisher).comment("expensive");
	}

	@Test // DATAMONGO-1836
	void aggregateShouldHonorOptionsHint() {

		Document hint = new Document("dummyHint", 1);
		AggregationOptions options = AggregationOptions.builder().hint(hint).build();

		template.aggregate(newAggregation(Sith.class, project("id")).withOptions(options), AutogenerateableId.class,
				Document.class).subscribe();

		verify(aggregatePublisher).hint(hint);
	}

	@Test // GH-4238
	void aggregateShouldHonorOptionsHintString() {

		AggregationOptions options = AggregationOptions.builder().hint("index-1").build();

		template.aggregate(newAggregation(Sith.class, project("id")).withOptions(options), AutogenerateableId.class,
				Document.class).subscribe();

		verify(aggregatePublisher).hintString("index-1");
	}

	@Test // DATAMONGO-2390
	void aggregateShouldNoApplyZeroOrNegativeMaxTime() {

		template
				.aggregate(newAggregation(MongoTemplateUnitTests.Sith.class, project("id")).withOptions(
						newAggregationOptions().maxTime(Duration.ZERO).build()), AutogenerateableId.class, Document.class)
				.subscribe();
		template
				.aggregate(
						newAggregation(MongoTemplateUnitTests.Sith.class, project("id"))
								.withOptions(newAggregationOptions().maxTime(Duration.ofSeconds(-1)).build()),
						AutogenerateableId.class, Document.class)
				.subscribe();

		verify(aggregatePublisher, never()).maxTime(anyLong(), any());
	}

	@Test // DATAMONGO-2390
	void aggregateShouldApplyMaxTimeIfSet() {

		template
				.aggregate(
						newAggregation(MongoTemplateUnitTests.Sith.class, project("id"))
								.withOptions(newAggregationOptions().maxTime(Duration.ofSeconds(10)).build()),
						AutogenerateableId.class, Document.class)
				.subscribe();

		verify(aggregatePublisher).maxTime(eq(10000L), eq(TimeUnit.MILLISECONDS));
	}

	@Test // DATAMONGO-1854
	void findAndReplaceShouldUseCollationWhenPresent() {

		template.findAndReplace(new BasicQuery("{}").collation(Collation.of("fr")), new Jedi()).subscribe();

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1854
	void findAndReplaceShouldUseDefaultCollationWhenPresent() {

		template.findAndReplace(new BasicQuery("{}"), new Sith()).subscribe();

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("de_AT");
	}

	@Test // DATAMONGO-1854
	void findAndReplaceShouldUseCollationEvenIfDefaultCollationIsPresent() {

		template.findAndReplace(new BasicQuery("{}").collation(Collation.of("fr")), new MongoTemplateUnitTests.Sith())
				.subscribe();

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1854
	void findDistinctShouldUseDefaultCollationWhenPresent() {

		template.findDistinct(new BasicQuery("{}"), "name", Sith.class, String.class).subscribe();

		verify(distinctPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	void findDistinctPreferCollationFromQueryOverDefaultCollation() {

		template.findDistinct(new BasicQuery("{}").collation(Collation.of("fr")), "name", Sith.class, String.class)
				.subscribe();

		verify(distinctPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	void updateFirstShouldUseDefaultCollationWhenPresent() {

		template.updateFirst(new BasicQuery("{}"), Update.update("foo", "bar"), Sith.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void updateFirstShouldPreferExplicitCollationOverDefaultCollation() {

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), Update.update("foo", "bar"), Sith.class)
				.subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("fr").build());
	}

	@Test // DATAMONGO-1854
	void updateMultiShouldUseDefaultCollationWhenPresent() {

		template.updateMulti(new BasicQuery("{}"), Update.update("foo", "bar"), Sith.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void updateMultiShouldPreferExplicitCollationOverDefaultCollation() {

		template.updateMulti(new BasicQuery("{}").collation(Collation.of("fr")), Update.update("foo", "bar"), Sith.class)
				.subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("fr").build());
	}

	@Test // DATAMONGO-1854
	void removeShouldUseDefaultCollationWhenPresent() {

		template.remove(new BasicQuery("{}"), Sith.class).subscribe();

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	void removeShouldPreferExplicitCollationOverDefaultCollation() {

		template.remove(new BasicQuery("{}").collation(Collation.of("fr")), Sith.class).subscribe();

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("fr").build());
	}

	@Test // DATAMONGO-2261
	void saveShouldInvokeCallbacks() {

		ValueCapturingBeforeConvertCallback beforeConvertCallback = spy(new ValueCapturingBeforeConvertCallback());
		ValueCapturingBeforeSaveCallback beforeSaveCallback = spy(new ValueCapturingBeforeSaveCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(beforeConvertCallback, beforeSaveCallback));

		Person entity = new Person();
		entity.id = "init";
		entity.firstname = "luke";

		template.save(entity).subscribe();

		verify(beforeConvertCallback).onBeforeConvert(eq(entity), anyString());
		verify(beforeSaveCallback).onBeforeSave(eq(entity), any(), anyString());
	}

	@Test // DATAMONGO-2261
	void insertShouldInvokeCallbacks() {

		ValueCapturingBeforeConvertCallback beforeConvertCallback = spy(new ValueCapturingBeforeConvertCallback());
		ValueCapturingBeforeSaveCallback beforeSaveCallback = spy(new ValueCapturingBeforeSaveCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(beforeConvertCallback, beforeSaveCallback));

		Person entity = new Person();
		entity.id = "init";
		entity.firstname = "luke";

		template.insert(entity).subscribe();

		verify(beforeConvertCallback).onBeforeConvert(eq(entity), anyString());
		verify(beforeSaveCallback).onBeforeSave(eq(entity), any(), anyString());
	}

	@Test // DATAMONGO-2261
	void insertAllShouldInvokeCallbacks() {

		ValueCapturingBeforeConvertCallback beforeConvertCallback = spy(new ValueCapturingBeforeConvertCallback());
		ValueCapturingBeforeSaveCallback beforeSaveCallback = spy(new ValueCapturingBeforeSaveCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(beforeConvertCallback, beforeSaveCallback));

		Person entity1 = new Person();
		entity1.id = "1";
		entity1.firstname = "luke";

		Person entity2 = new Person();
		entity1.id = "2";
		entity1.firstname = "luke";

		template.insertAll(Arrays.asList(entity1, entity2)).subscribe();

		verify(beforeConvertCallback, times(2)).onBeforeConvert(any(), anyString());
		verify(beforeSaveCallback, times(2)).onBeforeSave(any(), any(), anyString());
	}

	@Test // DATAMONGO-2261
	void findAndReplaceShouldInvokeCallbacks() {

		ValueCapturingBeforeConvertCallback beforeConvertCallback = spy(new ValueCapturingBeforeConvertCallback());
		ValueCapturingBeforeSaveCallback beforeSaveCallback = spy(new ValueCapturingBeforeSaveCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(beforeConvertCallback, beforeSaveCallback));

		Person entity = new Person();
		entity.id = "init";
		entity.firstname = "luke";

		template.findAndReplace(new Query(), entity).subscribe();

		verify(beforeConvertCallback).onBeforeConvert(eq(entity), anyString());
		verify(beforeSaveCallback).onBeforeSave(eq(entity), any(), anyString());
	}

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

		ReactiveEntityCallbacks callbacks = ReactiveEntityCallbacks.create();
		template.setEntityCallbacks(callbacks);

		Assertions.assertThat(ReflectionTestUtils.getField(template, "entityCallbacks")).isSameAs(callbacks);
	}

	@Test // DATAMONGO-2261
	void setterForApplicationContextShouldNotOverrideAlreadySetEntityCallbacks() {

		ReactiveEntityCallbacks callbacks = ReactiveEntityCallbacks.create();
		ApplicationContext ctx = new StaticApplicationContext();

		template.setEntityCallbacks(callbacks);
		template.setApplicationContext(ctx);

		Assertions.assertThat(ReflectionTestUtils.getField(template, "entityCallbacks")).isSameAs(callbacks);
	}

	@Test // DATAMONGO-2344, DATAMONGO-2572
	void allowSecondaryReadsQueryOptionShouldApplyPrimaryPreferredReadPreferenceForFind() {

		template.find(new Query().allowSecondaryReads(), AutogenerateableId.class).subscribe();

		verify(collection).withReadPreference(eq(ReadPreference.primaryPreferred()));
	}

	@Test // DATAMONGO-2344, DATAMONGO-2572
	void allowSecondaryReadsQueryOptionShouldApplyPrimaryPreferredReadPreferenceForFindOne() {

		template.findOne(new Query().allowSecondaryReads(), AutogenerateableId.class).subscribe();

		verify(collection).withReadPreference(eq(ReadPreference.primaryPreferred()));
	}

	@Test // DATAMONGO-2344, DATAMONGO-2572
	void allowSecondaryReadsQueryOptionShouldApplyPrimaryPreferredReadPreferenceForFindDistinct() {

		template.findDistinct(new Query().allowSecondaryReads(), "name", AutogenerateableId.class, String.class)
				.subscribe();

		verify(collection).withReadPreference(eq(ReadPreference.primaryPreferred()));
	}

	@Test // DATAMONGO-2331
	void updateShouldAllowAggregationExpressions() {

		AggregationUpdate update = AggregationUpdate.update().set("total")
				.toValue(ArithmeticOperators.valueOf("val1").sum().and("val2"));

		template.updateFirst(new BasicQuery("{}"), update, Wrapper.class).subscribe();

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

		template.updateFirst(new BasicQuery("{}"), update, Wrapper.class).subscribe();

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

		template.updateFirst(new BasicQuery("{}"), update, Jedi.class).subscribe();

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

		template.updateFirst(new BasicQuery("{}"), update, Wrapper.class).subscribe();

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

		template.updateFirst(new BasicQuery("{}"), update, Jedi.class).subscribe();

		ArgumentCaptor<List<Document>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection, times(1)).updateOne(any(org.bson.Document.class), captor.capture(), any(UpdateOptions.class));

		assertThat(captor.getValue()).isEqualTo(Collections.singletonList(Document.parse("{ $unset : \"firstname\" }")));
	}

	@Test // DATAMONGO-2341
	void saveShouldAppendNonDefaultShardKeyIfNotPresentInFilter() {

		when(findPublisher.first()).thenReturn(Mono.empty());

		template.save(new ShardedEntityWithNonDefaultShardKey("id-1", "AT", 4230)).subscribe();

		ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
		verify(collection).replaceOne(filter.capture(), any(), any());

		assertThat(filter.getValue()).isEqualTo(new Document("_id", "id-1").append("country", "AT").append("userid", 4230));
	}

	@Test // DATAMONGO-2341
	void saveShouldAppendNonDefaultShardKeyFromGivenDocumentIfShardKeyIsImmutable() {

		template.save(new ShardedEntityWithNonDefaultImmutableShardKey("id-1", "AT", 4230)).subscribe();

		ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Document> replacement = ArgumentCaptor.forClass(Document.class);

		verify(collection).replaceOne(filter.capture(), replacement.capture(), any());

		assertThat(filter.getValue()).isEqualTo(new Document("_id", "id-1").append("country", "AT").append("userid", 4230));
		assertThat(replacement.getValue()).containsEntry("country", "AT").containsEntry("userid", 4230);

		verifyNoInteractions(findPublisher);
	}

	@Test // DATAMONGO-2341
	void saveShouldAppendNonDefaultShardKeyToVersionedEntityIfNotPresentInFilter() {

		when(collection.replaceOne(any(Bson.class), any(Document.class), any(ReplaceOptions.class)))
				.thenReturn(Mono.just(UpdateResult.acknowledged(1, 1L, null)));
		when(findPublisher.first()).thenReturn(Mono.empty());

		template.save(new ShardedVersionedEntityWithNonDefaultShardKey("id-1", 1L, "AT", 4230)).subscribe();

		ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
		verify(collection).replaceOne(filter.capture(), any(), any());

		assertThat(filter.getValue())
				.isEqualTo(new Document("_id", "id-1").append("version", 1L).append("country", "AT").append("userid", 4230));
	}

	@Test // DATAMONGO-2341
	void saveShouldAppendNonDefaultShardKeyFromExistingDocumentIfNotPresentInFilter() {

		when(findPublisher.first())
				.thenReturn(Mono.just(new Document("_id", "id-1").append("country", "US").append("userid", 4230)));

		template.save(new ShardedEntityWithNonDefaultShardKey("id-1", "AT", 4230)).subscribe();

		ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
		ArgumentCaptor<Document> replacement = ArgumentCaptor.forClass(Document.class);

		verify(collection).replaceOne(filter.capture(), replacement.capture(), any());

		assertThat(filter.getValue()).isEqualTo(new Document("_id", "id-1").append("country", "US").append("userid", 4230));
		assertThat(replacement.getValue()).containsEntry("country", "AT").containsEntry("userid", 4230);
	}

	@Test // DATAMONGO-2341
	void saveShouldAppendDefaultShardKeyIfNotPresentInFilter() {

		template.save(new ShardedEntityWithDefaultShardKey("id-1", "AT", 4230)).subscribe();

		ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
		verify(collection).replaceOne(filter.capture(), any(), any());

		assertThat(filter.getValue()).isEqualTo(new Document("_id", "id-1"));
	}

	@Test // DATAMONGO-2341
	void saveShouldProjectOnShardKeyWhenLoadingExistingDocument() {

		when(findPublisher.first())
				.thenReturn(Mono.just(new Document("_id", "id-1").append("country", "US").append("userid", 4230)));

		template.save(new ShardedEntityWithNonDefaultShardKey("id-1", "AT", 4230)).subscribe();

		verify(findPublisher).projection(new Document("country", 1).append("userid", 1));
	}

	@Test // DATAMONGO-2341
	void saveVersionedShouldProjectOnShardKeyWhenLoadingExistingDocument() {

		when(collection.replaceOne(any(Bson.class), any(Document.class), any(ReplaceOptions.class)))
				.thenReturn(Mono.just(UpdateResult.acknowledged(1, 1L, null)));
		when(findPublisher.first()).thenReturn(Mono.empty());

		template.save(new ShardedVersionedEntityWithNonDefaultShardKey("id-1", 1L, "AT", 4230)).subscribe();

		verify(findPublisher).projection(new Document("country", 1).append("userid", 1));
	}

	@Test // GH-3648
	void shouldThrowExceptionIfEntityReaderReturnsNull() {

		MappingMongoConverter converter = mock(MappingMongoConverter.class);
		when(converter.getMappingContext()).thenReturn((MappingContext) mappingContext);
		when(converter.getProjectionFactory()).thenReturn(new SpelAwareProxyProjectionFactory());
		template = new ReactiveMongoTemplate(factory, converter);

		when(collection.find(Document.class)).thenReturn(findPublisher);
		stubFindSubscribe(new Document());

		template.find(new Query(), Person.class).as(StepVerifier::create).verifyError(MappingException.class);
	}

	@Test // DATAMONGO-2479
	void findShouldInvokeAfterConvertCallbacks() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.find(Document.class)).thenReturn(findPublisher);
		stubFindSubscribe(document);

		List<Person> results = template.find(new Query(), Person.class).timeout(Duration.ofSeconds(1)).toStream()
				.collect(Collectors.toList());

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
		assertThat(results.get(0).id).isEqualTo("after-convert");
	}

	@Test // GH-4543
	void findShouldNotLimitBackpressure() {

		AtomicLong request = new AtomicLong();
		stubFindSubscribe(new Document(), request);

		template.find(new Query(), Person.class).subscribe();

		assertThat(request).hasValueGreaterThan(128);
	}

	@Test // DATAMONGO-2479
	void findByIdShouldInvokeAfterConvertCallbacks() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.find(any(Bson.class), eq(Document.class))).thenReturn(findPublisher);
		stubFindSubscribe(document);

		Person result = template.findById("init", Person.class).block(Duration.ofSeconds(1));

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
		assertThat(result.id).isEqualTo("after-convert");
	}

	@Test // DATAMONGO-2479
	void findOneShouldInvokeAfterConvertCallbacks() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.find(any(Bson.class), eq(Document.class))).thenReturn(findPublisher);
		stubFindSubscribe(document);

		Person result = template.findOne(new Query(), Person.class).block(Duration.ofSeconds(1));

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
		assertThat(result.id).isEqualTo("after-convert");
	}

	@Test // DATAMONGO-2479
	void findAllShouldInvokeAfterConvertCallbacks() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.find(Document.class)).thenReturn(findPublisher);
		stubFindSubscribe(document);

		List<Person> results = template.findAll(Person.class).timeout(Duration.ofSeconds(1)).toStream()
				.collect(Collectors.toList());

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
		assertThat(results.get(0).id).isEqualTo("after-convert");
	}

	@Test // DATAMONGO-2479
	void findAndModifyShouldInvokeAfterConvertCallbacks() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.findOneAndUpdate(any(Bson.class), any(Bson.class), any())).thenReturn(findPublisher);
		stubFindSubscribe(document);

		Person result = template.findAndModify(new Query(), new Update(), Person.class).block(Duration.ofSeconds(1));

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
		assertThat(result.id).isEqualTo("after-convert");
	}

	@Test // DATAMONGO-2479
	void findAndRemoveShouldInvokeAfterConvertCallbacks() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.findOneAndDelete(any(Bson.class), any())).thenReturn(findPublisher);
		stubFindSubscribe(document);

		Person result = template.findAndRemove(new Query(), Person.class).block(Duration.ofSeconds(1));

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
		assertThat(result.id).isEqualTo("after-convert");
	}

	@Test // DATAMONGO-2479
	void findAllAndRemoveShouldInvokeAfterConvertCallbacks() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterConvertCallback));

		Document document = new Document("_id", "init").append("firstname", "luke");
		when(collection.find(Document.class)).thenReturn(findPublisher);
		stubFindSubscribe(document);
		when(collection.deleteMany(any(Bson.class), any(DeleteOptions.class)))
				.thenReturn(Mono.just(spy(DeleteResult.class)));

		List<Person> results = template.findAllAndRemove(new Query(), Person.class).timeout(Duration.ofSeconds(1))
				.toStream().collect(Collectors.toList());

		verify(afterConvertCallback).onAfterConvert(eq(new Person("init", "luke")), eq(document), anyString());
		assertThat(results.get(0).id).isEqualTo("after-convert");
	}

	@Test // DATAMONGO-2479
	void findAndReplaceShouldInvokeAfterConvertCallbacks() {

		ValueCapturingAfterConvertCallback afterConvertCallback = spy(new ValueCapturingAfterConvertCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterConvertCallback));

		when(collection.findOneAndReplace(any(Bson.class), any(Document.class), any())).thenReturn(findPublisher);
		stubFindSubscribe(new Document("_id", "init").append("firstname", "luke"));

		Person entity = new Person();
		entity.id = "init";
		entity.firstname = "luke";

		Person saved = template.findAndReplace(new Query(), entity).block(Duration.ofSeconds(1));

		verify(afterConvertCallback).onAfterConvert(eq(entity), any(), anyString());
		assertThat(saved.id).isEqualTo("after-convert");
	}

	@Test // DATAMONGO-2479
	void saveShouldInvokeAfterSaveCallbacks() {

		ValueCapturingAfterSaveCallback afterSaveCallback = spy(new ValueCapturingAfterSaveCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterSaveCallback));

		when(collection.replaceOne(any(Bson.class), any(Document.class), any(ReplaceOptions.class)))
				.thenReturn(Mono.just(mock(UpdateResult.class)));

		Person entity = new Person("init", "luke");

		Person saved = template.save(entity).block(Duration.ofSeconds(1));

		verify(afterSaveCallback).onAfterSave(eq(entity), any(), anyString());
		assertThat(saved.id).isEqualTo("after-save");
	}

	@Test // DATAMONGO-2479
	void insertShouldInvokeAfterSaveCallbacks() {

		ValueCapturingAfterSaveCallback afterSaveCallback = spy(new ValueCapturingAfterSaveCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterSaveCallback));

		when(collection.insertOne(any())).thenReturn(Mono.just(mock(InsertOneResult.class)));

		Person entity = new Person("init", "luke");

		Person saved = template.insert(entity).block(Duration.ofSeconds(1));

		verify(afterSaveCallback).onAfterSave(eq(entity), any(), anyString());
		assertThat(saved.id).isEqualTo("after-save");
	}

	@Test // DATAMONGO-2479
	void insertAllShouldInvokeAfterSaveCallbacks() {

		ValueCapturingAfterSaveCallback afterSaveCallback = spy(new ValueCapturingAfterSaveCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterSaveCallback));

		Person entity1 = new Person();
		entity1.id = "1";
		entity1.firstname = "luke";

		Person entity2 = new Person();
		entity1.id = "2";
		entity1.firstname = "luke";

		when(collection.insertMany(anyList())).then(invocation -> {
			List<?> list = invocation.getArgument(0);
			return Flux.fromIterable(list).map(i -> mock(InsertManyResult.class));
		});

		List<Person> saved = template.insertAll(Arrays.asList(entity1, entity2)).timeout(Duration.ofSeconds(1)).toStream()
				.collect(Collectors.toList());

		verify(afterSaveCallback, times(2)).onAfterSave(any(), any(), anyString());
		assertThat(saved.get(0).id).isEqualTo("after-save");
		assertThat(saved.get(1).id).isEqualTo("after-save");
	}

	@Test // DATAMONGO-2479
	void findAndReplaceShouldInvokeAfterSaveCallbacks() {

		ValueCapturingAfterSaveCallback afterSaveCallback = spy(new ValueCapturingAfterSaveCallback());

		template.setEntityCallbacks(ReactiveEntityCallbacks.create(afterSaveCallback));

		when(collection.findOneAndReplace(any(Bson.class), any(Document.class), any())).thenReturn(findPublisher);
		stubFindSubscribe(new Document("_id", "init").append("firstname", "luke"));

		Person entity = new Person("init", "luke");

		Person saved = template.findAndReplace(new Query(), entity).block(Duration.ofSeconds(1));

		verify(afterSaveCallback).onAfterSave(eq(entity), any(), anyString());
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
		when(collection.findOneAndReplace(any(Bson.class), any(Document.class), any())).thenReturn(Mono.just(document));

		Person saved = template.findAndReplace(new Query(), entity).block(Duration.ofSeconds(1));

		assertThat(saved.id).isEqualTo("after-save-event");
	}

	@Test // DATAMONGO-2556
	void esitmatedCountShouldBeDelegatedCorrectly() {

		template.estimatedCount(Person.class).subscribe();

		verify(db).getCollection("star-wars", Document.class);
		verify(collection).estimatedDocumentCount(any());
	}

	@Test // GH-3522
	void usedCountDocumentsForEmptyQueryByDefault() {

		template.count(new Query(), Person.class).subscribe();

		verify(collection).countDocuments(any(Document.class), any());
	}

	@Test // GH-3522
	void delegatesToEstimatedCountForEmptyQueryIfEnabled() {

		template.useEstimatedCount(true);

		template.count(new Query(), Person.class).subscribe();

		verify(collection).estimatedDocumentCount(any());
	}

	@Test // GH-3522
	void stillUsesCountDocumentsForNonEmptyQueryEvenIfEstimationEnabled() {

		template.useEstimatedCount(true);

		template.count(new BasicQuery("{ 'spring' : 'data-mongodb' }"), Person.class).subscribe();

		verify(collection).countDocuments(any(Document.class), any());
	}

	@Test // GH-4374
	void countConsidersMaxTimeMs() {

		template.count(new BasicQuery("{ 'spring' : 'data-mongodb' }").maxTimeMsec(5000), Person.class).subscribe();

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(Document.class), options.capture());
		assertThat(options.getValue().getMaxTime(TimeUnit.MILLISECONDS)).isEqualTo(5000);
	}

	@Test // GH-4374
	void countPassesOnComment() {

		template.count(new BasicQuery("{ 'spring' : 'data-mongodb' }").comment("rocks!"), Person.class).subscribe();

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(Document.class), options.capture());
		assertThat(options.getValue().getComment()).isEqualTo(BsonUtils.simpleToBsonValue("rocks!"));
	}

	@Test // GH-2911
	void insertErrorsOnPublisher() {

		Publisher<String> publisher = Mono.just("data");

		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> template.insert(publisher));
	}

	@Test // GH-3731
	void createCollectionShouldSetUpTimeSeriesWithDefaults() {

		template.createCollection(TimeSeriesTypeWithDefaults.class).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getTimeSeriesOptions().toString())
				.isEqualTo(new com.mongodb.client.model.TimeSeriesOptions("timestamp").toString());
	}

	@Test // GH-3731
	void createCollectionShouldSetUpTimeSeries() {

		template.createCollection(TimeSeriesType.class).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getTimeSeriesOptions().toString())
				.isEqualTo(new com.mongodb.client.model.TimeSeriesOptions("time_stamp").metaField("meta")
						.granularity(TimeSeriesGranularity.HOURS).toString());
	}

	@Test // GH-4167
	void changeStreamOptionStartAftershouldApplied() {

		when(factory.getMongoDatabase(anyString())).thenReturn(Mono.just(db));

		when(collection.watch(any(Class.class))).thenReturn(changeStreamPublisher);
		when(changeStreamPublisher.batchSize(anyInt())).thenReturn(changeStreamPublisher);
		when(changeStreamPublisher.startAfter(any())).thenReturn(changeStreamPublisher);
		when(changeStreamPublisher.fullDocument(any())).thenReturn(changeStreamPublisher);

		BsonDocument token = new BsonDocument("token", new BsonString("id"));
		template
				.changeStream("database", "collection", ChangeStreamOptions.builder().startAfter(token).build(), Object.class)
				.subscribe();

		verify(changeStreamPublisher).startAfter(eq(token));
	}

	@Test // GH-4495
	void changeStreamOptionFullDocumentBeforeChangeShouldBeApplied() {

		when(factory.getMongoDatabase(anyString())).thenReturn(Mono.just(db));

		when(collection.watch(any(Class.class))).thenReturn(changeStreamPublisher);
		when(changeStreamPublisher.batchSize(anyInt())).thenReturn(changeStreamPublisher);
		when(changeStreamPublisher.startAfter(any())).thenReturn(changeStreamPublisher);
		when(changeStreamPublisher.fullDocument(any())).thenReturn(changeStreamPublisher);
		when(changeStreamPublisher.fullDocumentBeforeChange(any())).thenReturn(changeStreamPublisher);

		ChangeStreamOptions options = ChangeStreamOptions.builder()
				.fullDocumentBeforeChangeLookup(FullDocumentBeforeChange.REQUIRED).build();
		template.changeStream("database", "collection", options, Object.class).subscribe();

		verify(changeStreamPublisher).fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED);

	}

	@Test // GH-4462
	void replaceShouldUseCollationWhenPresent() {

		template.replace(new BasicQuery("{}").collation(Collation.of("fr")), new Jedi()).subscribe();

		ArgumentCaptor<com.mongodb.client.model.ReplaceOptions> options = ArgumentCaptor
				.forClass(com.mongodb.client.model.ReplaceOptions.class);
		verify(collection).replaceOne(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().isUpsert()).isFalse();
		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // GH-4462
	void replaceShouldNotUpsertByDefault() {

		template.replace(new BasicQuery("{}"), new MongoTemplateUnitTests.Sith()).subscribe();

		ArgumentCaptor<com.mongodb.client.model.ReplaceOptions> options = ArgumentCaptor
				.forClass(com.mongodb.client.model.ReplaceOptions.class);
		verify(collection).replaceOne(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().isUpsert()).isFalse();
	}

	@Test // GH-4462
	void replaceShouldUpsert() {

		template.replace(new BasicQuery("{}"), new MongoTemplateUnitTests.Sith(),
				org.springframework.data.mongodb.core.ReplaceOptions.replaceOptions().upsert()).subscribe();

		ArgumentCaptor<com.mongodb.client.model.ReplaceOptions> options = ArgumentCaptor
				.forClass(com.mongodb.client.model.ReplaceOptions.class);
		verify(collection).replaceOne(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().isUpsert()).isTrue();
	}

	@Test // GH-4462
	void replaceShouldUseDefaultCollationWhenPresent() {

		template.replace(new BasicQuery("{}"), new MongoTemplateUnitTests.Sith(),
				org.springframework.data.mongodb.core.ReplaceOptions.replaceOptions()).subscribe();

		ArgumentCaptor<com.mongodb.client.model.ReplaceOptions> options = ArgumentCaptor
				.forClass(com.mongodb.client.model.ReplaceOptions.class);
		verify(collection).replaceOne(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("de_AT");
	}

	@Test // GH-4462
	void replaceShouldUseHintIfPresent() {

		template.replace(new BasicQuery("{}").withHint("index-to-use"), new MongoTemplateUnitTests.Sith(),
				org.springframework.data.mongodb.core.ReplaceOptions.replaceOptions().upsert()).subscribe();

		ArgumentCaptor<com.mongodb.client.model.ReplaceOptions> options = ArgumentCaptor
				.forClass(com.mongodb.client.model.ReplaceOptions.class);
		verify(collection).replaceOne(any(Bson.class), any(), options.capture());

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

		template.replace(new BasicQuery("{}").withHint("index-to-use"), new Sith(),
				org.springframework.data.mongodb.core.ReplaceOptions.replaceOptions().upsert()).subscribe();

		verify(collection).withWriteConcern(eq(WriteConcern.UNACKNOWLEDGED));
	}

	private void stubFindSubscribe(Document document) {
		stubFindSubscribe(document, new AtomicLong());
	}

	private void stubFindSubscribe(Document document, AtomicLong request) {

		Publisher<Document> realPublisher = Flux.just(document).doOnRequest(request::addAndGet);

		doAnswer(invocation -> {
			Subscriber<Document> subscriber = invocation.getArgument(0);
			realPublisher.subscribe(subscriber);
			return null;
		}).when(findPublisher).subscribe(any());
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = "star-wars")
	static class Person {

		@Id String id;
		String firstname;

		public Person() {}

		public Person(String id, String firstname) {
			this.id = id;
			this.firstname = firstname;
		}

		public String getId() {
			return this.id;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public void setId(String id) {
			this.id = id;
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

		public String toString() {
			return "ReactiveMongoTemplateUnitTests.Person(id=" + this.getId() + ", firstname=" + this.getFirstname() + ")";
		}
	}

	class Wrapper {

		AutogenerateableId foo;
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

	static class Jedi {

		@Field("firstname") String name;

		public Jedi() {}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Jedi jedi = (Jedi) o;
			return Objects.equals(name, jedi.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name);
		}

		public String toString() {
			return "ReactiveMongoTemplateUnitTests.Jedi(name=" + this.getName() + ")";
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document(collation = "de_AT")
	static class Sith {

		@Field("firstname") String name;
	}

	static class EntityWithListOfSimple {
		List<Integer> grades;
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
			implements ReactiveBeforeConvertCallback<Person> {

		@Override
		public Mono<Person> onBeforeConvert(Person entity, String collection) {

			capture(entity);
			return Mono.just(entity);
		}
	}

	static class ValueCapturingBeforeSaveCallback extends ValueCapturingEntityCallback<Person>
			implements ReactiveBeforeSaveCallback<Person> {

		@Override
		public Mono<Person> onBeforeSave(Person entity, Document document, String collection) {

			capture(entity);
			return Mono.just(entity);
		}
	}

	static class ValueCapturingAfterConvertCallback extends ValueCapturingEntityCallback<Person>
			implements ReactiveAfterConvertCallback<Person> {

		@Override
		public Mono<Person> onAfterConvert(Person entity, Document document, String collection) {

			capture(entity);
			return Mono.just(new Person() {
				{
					id = "after-convert";
					firstname = entity.firstname;
				}
			});
		}
	}

	static class ValueCapturingAfterSaveCallback extends ValueCapturingEntityCallback<Person>
			implements ReactiveAfterSaveCallback<Person> {

		@Override
		public Mono<Person> onAfterSave(Person entity, Document document, String collection) {

			capture(entity);
			return Mono.just(new Person() {
				{
					id = "after-save";
					firstname = entity.firstname;
				}
			});
		}
	}

}
