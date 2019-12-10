/*
 * Copyright 2016-2019 the original author or authors.
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

import lombok.Data;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
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
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.lang.Nullable;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.CollectionUtils;

import com.mongodb.ReadPreference;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.AggregatePublisher;
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
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ReactiveMongoTemplateUnitTests {

	ReactiveMongoTemplate template;

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

	MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	MappingMongoConverter converter;
	MongoMappingContext mappingContext;

	@BeforeEach
	public void beforeEach() {

		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslator);
		when(factory.getMongoDatabase()).thenReturn(db);
		when(db.getCollection(any())).thenReturn(collection);
		when(db.getCollection(any(), any())).thenReturn(collection);
		when(db.runCommand(any(), any(Class.class))).thenReturn(runCommandPublisher);
		when(db.createCollection(any(), any(CreateCollectionOptions.class))).thenReturn(runCommandPublisher);
		when(collection.withReadPreference(any())).thenReturn(collection);
		when(collection.find(any(Class.class))).thenReturn(findPublisher);
		when(collection.find(any(Document.class), any(Class.class))).thenReturn(findPublisher);
		when(collection.aggregate(anyList())).thenReturn(aggregatePublisher);
		when(collection.aggregate(anyList(), any(Class.class))).thenReturn(aggregatePublisher);
		when(collection.countDocuments(any(), any(CountOptions.class))).thenReturn(Mono.just(0L));
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
		when(aggregatePublisher.allowDiskUse(anyBoolean())).thenReturn(aggregatePublisher);
		when(aggregatePublisher.collation(any())).thenReturn(aggregatePublisher);
		when(aggregatePublisher.maxTime(anyLong(), any())).thenReturn(aggregatePublisher);
		when(aggregatePublisher.first()).thenReturn(findPublisher);

		this.mappingContext = new MongoMappingContext();
		this.converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		this.template = new ReactiveMongoTemplate(factory, converter);
	}

	@Test // DATAMONGO-1444
	public void rejectsNullDatabaseName() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ReactiveMongoTemplate(mongoClient, null));
	}

	@Test // DATAMONGO-1444
	public void rejectsNullMongo() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ReactiveMongoTemplate(null, "database"));
	}

	@Test // DATAMONGO-1444
	public void defaultsConverterToMappingMongoConverter() throws Exception {
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoClient, "database");
		assertThat(ReflectionTestUtils.getField(template, "mongoConverter") instanceof MappingMongoConverter).isTrue();
	}

	@Test // DATAMONGO-1912
	public void autogeneratesIdForMap() {

		ReactiveMongoTemplate template = spy(this.template);
		doReturn(Mono.just(new ObjectId())).when(template).saveDocument(any(String.class), any(Document.class),
				any(Class.class));

		Map<String, String> entity = new LinkedHashMap<>();
		template.save(entity, "foo").as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(entity).containsKey("_id");
		}).verifyComplete();
	}

	@Test // DATAMONGO-1311
	public void executeQueryShouldUseBatchSizeWhenPresent() {

		when(findPublisher.batchSize(anyInt())).thenReturn(findPublisher);

		Query query = new Query().cursorBatchSize(1234);
		template.find(query, Person.class).subscribe();

		verify(findPublisher).batchSize(1234);
	}

	@Test // DATAMONGO-1518
	public void findShouldUseCollationWhenPresent() {

		template.find(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	//
	@Test // DATAMONGO-1518
	public void findOneShouldUseCollationWhenPresent() {

		template.findOne(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	public void existsShouldUseCollationWhenPresent() {

		template.exists(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518
	public void findAndModfiyShoudUseCollationWhenPresent() {

		when(collection.findOneAndUpdate(any(Bson.class), any(Bson.class), any())).thenReturn(Mono.empty());

		template.findAndModify(new BasicQuery("{}").collation(Collation.of("fr")), new Update(), AutogenerateableId.class)
				.subscribe();

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518
	public void findAndRemoveShouldUseCollationWhenPresent() {

		when(collection.findOneAndDelete(any(Bson.class), any())).thenReturn(Mono.empty());

		template.findAndRemove(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class).subscribe();

		ArgumentCaptor<FindOneAndDeleteOptions> options = ArgumentCaptor.forClass(FindOneAndDeleteOptions.class);
		verify(collection).findOneAndDelete(any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518
	public void findAndRemoveManyShouldUseCollationWhenPresent() {

		when(collection.deleteMany(any(Bson.class), any())).thenReturn(Mono.empty());

		template.doRemove("collection-1", new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class)
				.subscribe();

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518
	public void updateOneShouldUseCollationWhenPresent() {

		when(collection.updateOne(any(Bson.class), any(Bson.class), any())).thenReturn(Mono.empty());

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), new Update().set("foo", "bar"),
				AutogenerateableId.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518
	public void updateManyShouldUseCollationWhenPresent() {

		when(collection.updateMany(any(Bson.class), any(Bson.class), any())).thenReturn(Mono.empty());

		template.updateMulti(new BasicQuery("{}").collation(Collation.of("fr")), new Update().set("foo", "bar"),
				AutogenerateableId.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");

	}

	@Test // DATAMONGO-1518
	public void replaceOneShouldUseCollationWhenPresent() {

		when(collection.replaceOne(any(Bson.class), any(), any(ReplaceOptions.class))).thenReturn(Mono.empty());

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), new Update(), AutogenerateableId.class)
				.subscribe();

		ArgumentCaptor<ReplaceOptions> options = ArgumentCaptor.forClass(ReplaceOptions.class);
		verify(collection).replaceOne(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1518, DATAMONGO-2257
	public void mapReduceShouldUseCollationWhenPresent() {

		template.mapReduce(new BasicQuery("{}"), AutogenerateableId.class, AutogenerateableId.class, "", "",
				MapReduceOptions.options().collation(Collation.of("fr"))).subscribe();

		verify(mapReducePublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1518, DATAMONGO-2264
	public void geoNearShouldUseCollationWhenPresent() {

		NearQuery query = NearQuery.near(0D, 0D).query(new BasicQuery("{}").collation(Collation.of("fr")));
		template.geoNear(query, AutogenerateableId.class).subscribe();

		verify(aggregatePublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1719
	public void appliesFieldsWhenInterfaceProjectionIsClosedAndQueryDoesNotDefineFields() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, PersonProjection.class,
				FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher).projection(eq(new Document("firstname", 1)));
	}

	@Test // DATAMONGO-1719
	public void doesNotApplyFieldsWhenInterfaceProjectionIsClosedAndQueryDefinesFields() {

		template.doFind("star-wars", new Document(), new Document("bar", 1), Person.class, PersonProjection.class,
				FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher).projection(eq(new Document("bar", 1)));
	}

	@Test // DATAMONGO-1719
	public void doesNotApplyFieldsWhenInterfaceProjectionIsOpen() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, PersonSpELProjection.class,
				FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher, never()).projection(any());
	}

	@Test // DATAMONGO-1719, DATAMONGO-2041
	public void appliesFieldsToDtoProjection() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, Jedi.class,
				FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher).projection(eq(new Document("firstname", 1)));
	}

	@Test // DATAMONGO-1719
	public void doesNotApplyFieldsToDtoProjectionWhenQueryDefinesFields() {

		template.doFind("star-wars", new Document(), new Document("bar", 1), Person.class, Jedi.class,
				FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher).projection(eq(new Document("bar", 1)));
	}

	@Test // DATAMONGO-1719
	public void doesNotApplyFieldsWhenTargetIsNotAProjection() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, Person.class,
				FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher, never()).projection(any());
	}

	@Test // DATAMONGO-1719
	public void doesNotApplyFieldsWhenTargetExtendsDomainType() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, PersonExtended.class,
				FindPublisherPreparer.NO_OP_PREPARER).subscribe();

		verify(findPublisher, never()).projection(any());
	}

	@Test // DATAMONGO-1783
	public void countShouldUseSkipFromQuery() {

		template.count(new Query().skip(10), Person.class, "star-wars").subscribe();

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getSkip()).isEqualTo(10);
	}

	@Test // DATAMONGO-1783
	public void countShouldUseLimitFromQuery() {

		template.count(new Query().limit(100), Person.class, "star-wars").subscribe();

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getLimit()).isEqualTo(100);
	}

	@Test // DATAMONGO-2360
	public void countShouldApplyQueryHintIfPresent() {

		Document queryHint = new Document("age", 1);
		template.count(new Query().withHint(queryHint), Person.class, "star-wars").subscribe();

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).countDocuments(any(), options.capture());

		assertThat(options.getValue().getHint()).isEqualTo(queryHint);
	}

	@Test // DATAMONGO-2215
	public void updateShouldApplyArrayFilters() {

		template.updateFirst(new BasicQuery("{}"),
				new Update().set("grades.$[element]", 100).filterArray(Criteria.where("element").gte(100)),
				EntityWithListOfSimple.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		Assertions.assertThat((List<Bson>) options.getValue().getArrayFilters())
				.contains(new org.bson.Document("element", new Document("$gte", 100)));
	}

	@Test // DATAMONGO-2215
	public void findAndModifyShouldApplyArrayFilters() {

		template.findAndModify(new BasicQuery("{}"),
				new Update().set("grades.$[element]", 100).filterArray(Criteria.where("element").gte(100)),
				EntityWithListOfSimple.class).subscribe();

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(Bson.class), options.capture());

		Assertions.assertThat((List<Bson>) options.getValue().getArrayFilters())
				.contains(new org.bson.Document("element", new Document("$gte", 100)));
	}

	@Test // DATAMONGO-1854
	public void findShouldNotUseCollationWhenNoDefaultPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}"), Jedi.class).subscribe();

		verify(findPublisher, never()).collation(any());
	}

	@Test // DATAMONGO-1854
	public void findShouldUseDefaultCollationWhenPresent() {

		template.find(new BasicQuery("{'foo' : 'bar'}"), Sith.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void findOneShouldUseDefaultCollationWhenPresent() {

		template.findOne(new BasicQuery("{'foo' : 'bar'}"), Sith.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void existsShouldUseDefaultCollationWhenPresent() {

		template.exists(new BasicQuery("{}"), Sith.class).subscribe();

		verify(findPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void findAndModfiyShoudUseDefaultCollationWhenPresent() {

		template.findAndModify(new BasicQuery("{}"), new Update(), Sith.class).subscribe();

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	public void findAndRemoveShouldUseDefaultCollationWhenPresent() {

		template.findAndRemove(new BasicQuery("{}"), Sith.class).subscribe();

		ArgumentCaptor<FindOneAndDeleteOptions> options = ArgumentCaptor.forClass(FindOneAndDeleteOptions.class);
		verify(collection).findOneAndDelete(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	public void createCollectionShouldNotCollationIfNotPresent() {

		template.createCollection(AutogenerateableId.class).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		Assertions.assertThat(options.getValue().getCollation()).isNull();
	}

	@Test // DATAMONGO-1854
	public void createCollectionShouldApplyDefaultCollation() {

		template.createCollection(Sith.class).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	public void createCollectionShouldFavorExplicitOptionsOverDefaultCollation() {

		template.createCollection(Sith.class, CollectionOptions.just(Collation.of("en_US"))).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("en_US").build());
	}

	@Test // DATAMONGO-1854
	public void createCollectionShouldUseDefaultCollationIfCollectionOptionsAreNull() {

		template.createCollection(Sith.class, null).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	public void aggreateShouldUseDefaultCollationIfPresent() {

		template.aggregate(newAggregation(Sith.class, project("id")), AutogenerateableId.class, Document.class).subscribe();

		verify(aggregatePublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void aggreateShouldUseCollationFromOptionsEvenIfDefaultCollationIsPresent() {

		template
				.aggregate(
						newAggregation(Sith.class, project("id"))
								.withOptions(newAggregationOptions().collation(Collation.of("fr")).build()),
						AutogenerateableId.class, Document.class)
				.subscribe();

		verify(aggregatePublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-2153
	public void aggregateShouldHonorOptionsComment() {

		AggregationOptions options = AggregationOptions.builder().comment("expensive").build();

		template.aggregate(newAggregation(Sith.class, project("id")).withOptions(options), AutogenerateableId.class,
				Document.class).subscribe();

		verify(aggregatePublisher).comment("expensive");
	}

	@Test // DATAMONGO-2390
	public void aggregateShouldNoApplyZeroOrNegativeMaxTime() {

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
	public void aggregateShouldApplyMaxTimeIfSet() {

		template
				.aggregate(
						newAggregation(MongoTemplateUnitTests.Sith.class, project("id"))
								.withOptions(newAggregationOptions().maxTime(Duration.ofSeconds(10)).build()),
						AutogenerateableId.class, Document.class)
				.subscribe();

		verify(aggregatePublisher).maxTime(eq(10000L), eq(TimeUnit.MILLISECONDS));
	}

	@Test // DATAMONGO-1854
	public void findAndReplaceShouldUseCollationWhenPresent() {

		template.findAndReplace(new BasicQuery("{}").collation(Collation.of("fr")), new Jedi()).subscribe();

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1854
	public void findAndReplaceShouldUseDefaultCollationWhenPresent() {

		template.findAndReplace(new BasicQuery("{}"), new Sith()).subscribe();

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("de_AT");
	}

	@Test // DATAMONGO-1854
	public void findAndReplaceShouldUseCollationEvenIfDefaultCollationIsPresent() {

		template.findAndReplace(new BasicQuery("{}").collation(Collation.of("fr")), new MongoTemplateUnitTests.Sith())
				.subscribe();

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale()).isEqualTo("fr");
	}

	@Test // DATAMONGO-1854
	public void findDistinctShouldUseDefaultCollationWhenPresent() {

		template.findDistinct(new BasicQuery("{}"), "name", Sith.class, String.class).subscribe();

		verify(distinctPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void findDistinctPreferCollationFromQueryOverDefaultCollation() {

		template.findDistinct(new BasicQuery("{}").collation(Collation.of("fr")), "name", Sith.class, String.class)
				.subscribe();

		verify(distinctPublisher).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	public void updateFirstShouldUseDefaultCollationWhenPresent() {

		template.updateFirst(new BasicQuery("{}"), Update.update("foo", "bar"), Sith.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	public void updateFirstShouldPreferExplicitCollationOverDefaultCollation() {

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), Update.update("foo", "bar"), Sith.class)
				.subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("fr").build());
	}

	@Test // DATAMONGO-1854
	public void updateMultiShouldUseDefaultCollationWhenPresent() {

		template.updateMulti(new BasicQuery("{}"), Update.update("foo", "bar"), Sith.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	public void updateMultiShouldPreferExplicitCollationOverDefaultCollation() {

		template.updateMulti(new BasicQuery("{}").collation(Collation.of("fr")), Update.update("foo", "bar"), Sith.class)
				.subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(Bson.class), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("fr").build());
	}

	@Test // DATAMONGO-1854
	public void removeShouldUseDefaultCollationWhenPresent() {

		template.remove(new BasicQuery("{}"), Sith.class).subscribe();

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de_AT").build());
	}

	@Test // DATAMONGO-1854
	public void removeShouldPreferExplicitCollationOverDefaultCollation() {

		template.remove(new BasicQuery("{}").collation(Collation.of("fr")), Sith.class).subscribe();

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("fr").build());
	}

	@Test // DATAMONGO-2261
	public void saveShouldInvokeCallbacks() {

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
	public void insertShouldInvokeCallbacks() {

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
	public void insertAllShouldInvokeCallbacks() {

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
	public void findAndReplaceShouldInvokeCallbacks() {

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
	public void entityCallbacksAreNotSetByDefault() {
		Assertions.assertThat(ReflectionTestUtils.getField(template, "entityCallbacks")).isNull();
	}

	@Test // DATAMONGO-2261
	public void entityCallbacksShouldBeInitiatedOnSettingApplicationContext() {

		ApplicationContext ctx = new StaticApplicationContext();
		template.setApplicationContext(ctx);

		Assertions.assertThat(ReflectionTestUtils.getField(template, "entityCallbacks")).isNotNull();
	}

	@Test // DATAMONGO-2261
	public void setterForEntityCallbackOverridesContextInitializedOnes() {

		ApplicationContext ctx = new StaticApplicationContext();
		template.setApplicationContext(ctx);

		ReactiveEntityCallbacks callbacks = ReactiveEntityCallbacks.create();
		template.setEntityCallbacks(callbacks);

		Assertions.assertThat(ReflectionTestUtils.getField(template, "entityCallbacks")).isSameAs(callbacks);
	}

	@Test // DATAMONGO-2261
	public void setterForApplicationContextShouldNotOverrideAlreadySetEntityCallbacks() {

		ReactiveEntityCallbacks callbacks = ReactiveEntityCallbacks.create();
		ApplicationContext ctx = new StaticApplicationContext();

		template.setEntityCallbacks(callbacks);
		template.setApplicationContext(ctx);

		Assertions.assertThat(ReflectionTestUtils.getField(template, "entityCallbacks")).isSameAs(callbacks);
	}

	@Test // DATAMONGO-2344
	public void slaveOkQueryOptionShouldApplyPrimaryPreferredReadPreferenceForFind() {

		template.find(new Query().slaveOk(), AutogenerateableId.class).subscribe();

		verify(collection).withReadPreference(eq(ReadPreference.primaryPreferred()));
	}

	@Test // DATAMONGO-2344
	public void slaveOkQueryOptionShouldApplyPrimaryPreferredReadPreferenceForFindOne() {

		template.findOne(new Query().slaveOk(), AutogenerateableId.class).subscribe();

		verify(collection).withReadPreference(eq(ReadPreference.primaryPreferred()));
	}

	@Test // DATAMONGO-2344
	public void slaveOkQueryOptionShouldApplyPrimaryPreferredReadPreferenceForFindDistinct() {

		template.findDistinct(new Query().slaveOk(), "name", AutogenerateableId.class, String.class).subscribe();

		verify(collection).withReadPreference(eq(ReadPreference.primaryPreferred()));
	}

	@Test // DATAMONGO-2331
	public void updateShouldAllowAggregationExpressions() {

		AggregationUpdate update = AggregationUpdate.update().set("total")
				.toValue(ArithmeticOperators.valueOf("val1").sum().and("val2"));

		template.updateFirst(new BasicQuery("{}"), update, Wrapper.class).subscribe();

		ArgumentCaptor<List<Document>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection, times(1)).updateOne(any(org.bson.Document.class), captor.capture(), any(UpdateOptions.class));

		assertThat(captor.getValue()).isEqualTo(
				Collections.singletonList(Document.parse("{ $set : { total : { $sum : [  \"$val1\",\"$val2\" ] } } }")));
	}

	@Test // DATAMONGO-2331
	public void updateShouldAllowMultipleAggregationExpressions() {

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
	public void updateShouldMapAggregationExpressionToDomainType() {

		AggregationUpdate update = AggregationUpdate.update().set("name")
				.toValue(ArithmeticOperators.valueOf("val1").sum().and("val2"));

		template.updateFirst(new BasicQuery("{}"), update, Jedi.class).subscribe();

		ArgumentCaptor<List<Document>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection, times(1)).updateOne(any(org.bson.Document.class), captor.capture(), any(UpdateOptions.class));

		assertThat(captor.getValue()).isEqualTo(
				Collections.singletonList(Document.parse("{ $set : { firstname : { $sum:[  \"$val1\",\"$val2\" ] } } }")));
	}

	@Test // DATAMONGO-2331
	public void updateShouldPassOnUnsetCorrectly() {

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
	public void updateShouldMapAggregationUnsetToDomainType() {

		AggregationUpdate update = AggregationUpdate.update();
		update.unset("name");

		template.updateFirst(new BasicQuery("{}"), update, Jedi.class).subscribe();

		ArgumentCaptor<List<Document>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection, times(1)).updateOne(any(org.bson.Document.class), captor.capture(), any(UpdateOptions.class));

		assertThat(captor.getValue()).isEqualTo(Collections.singletonList(Document.parse("{ $unset : \"firstname\" }")));
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = "star-wars")
	static class Person {

		@Id String id;
		String firstname;
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

	@Data
	static class Jedi {

		@Field("firstname") String name;
	}

	@org.springframework.data.mongodb.core.mapping.Document(collation = "de_AT")
	static class Sith {

		@Field("firstname") String name;
	}

	static class EntityWithListOfSimple {
		List<Integer> grades;
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
}
