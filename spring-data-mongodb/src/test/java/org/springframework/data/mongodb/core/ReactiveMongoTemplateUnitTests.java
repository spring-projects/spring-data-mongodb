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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import lombok.Data;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplateUnitTests.AutogenerateableId;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
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
@RunWith(MockitoJUnitRunner.Silent.class)
public class ReactiveMongoTemplateUnitTests {

	ReactiveMongoTemplate template;

	@Mock SimpleReactiveMongoDatabaseFactory factory;
	@Mock MongoClient mongoClient;
	@Mock MongoDatabase db;
	@Mock MongoCollection collection;
	@Mock FindPublisher findPublisher;
	@Mock AggregatePublisher aggregatePublisher;
	@Mock Publisher runCommandPublisher;
	@Mock Publisher updatePublisher;
	@Mock Publisher findAndUpdatePublisher;
	@Mock DistinctPublisher distinctPublisher;
	@Mock Publisher deletePublisher;
	@Mock MapReducePublisher mapReducePublisher;

	MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	MappingMongoConverter converter;
	MongoMappingContext mappingContext;

	@Before
	public void setUp() {

		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslator);
		when(factory.getMongoDatabase()).thenReturn(db);
		when(db.getCollection(any())).thenReturn(collection);
		when(db.getCollection(any(), any())).thenReturn(collection);
		when(db.runCommand(any(), any(Class.class))).thenReturn(runCommandPublisher);
		when(db.createCollection(any(), any(CreateCollectionOptions.class))).thenReturn(runCommandPublisher);
		when(collection.find(any(Class.class))).thenReturn(findPublisher);
		when(collection.find(any(Document.class), any(Class.class))).thenReturn(findPublisher);
		when(collection.aggregate(anyList())).thenReturn(aggregatePublisher);
		when(collection.aggregate(anyList(), any(Class.class))).thenReturn(aggregatePublisher);
		when(collection.count(any(), any(CountOptions.class))).thenReturn(Mono.just(0L));
		when(collection.updateOne(any(), any(), any(UpdateOptions.class))).thenReturn(updatePublisher);
		when(collection.updateMany(any(Bson.class), any(), any())).thenReturn(updatePublisher);
		when(collection.findOneAndUpdate(any(), any(), any(FindOneAndUpdateOptions.class)))
				.thenReturn(findAndUpdatePublisher);
		when(collection.findOneAndReplace(any(Bson.class), any(), any())).thenReturn(findPublisher);
		when(collection.findOneAndDelete(any(), any(FindOneAndDeleteOptions.class))).thenReturn(findPublisher);
		when(collection.distinct(anyString(), any(Document.class), any())).thenReturn(distinctPublisher);
		when(collection.deleteMany(any(Bson.class), any())).thenReturn(deletePublisher);
		when(collection.findOneAndUpdate(any(), any(), any(FindOneAndUpdateOptions.class)))
				.thenReturn(findAndUpdatePublisher);
		when(collection.mapReduce(anyString(), anyString(), any())).thenReturn(mapReducePublisher);
		when(findPublisher.projection(any())).thenReturn(findPublisher);
		when(findPublisher.limit(anyInt())).thenReturn(findPublisher);
		when(findPublisher.collation(any())).thenReturn(findPublisher);
		when(findPublisher.first()).thenReturn(findPublisher);
		when(aggregatePublisher.allowDiskUse(anyBoolean())).thenReturn(aggregatePublisher);
		when(aggregatePublisher.collation(any())).thenReturn(aggregatePublisher);
		when(aggregatePublisher.first()).thenReturn(findPublisher);

		this.mappingContext = new MongoMappingContext();
		this.converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		this.template = new ReactiveMongoTemplate(factory, converter);

	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1444
	public void rejectsNullDatabaseName() throws Exception {
		new ReactiveMongoTemplate(mongoClient, null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1444
	public void rejectsNullMongo() throws Exception {
		new ReactiveMongoTemplate(null, "database");
	}

	@Test // DATAMONGO-1444
	public void defaultsConverterToMappingMongoConverter() throws Exception {
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoClient, "database");
		assertTrue(ReflectionTestUtils.getField(template, "mongoConverter") instanceof MappingMongoConverter);
	}

	@Test // DATAMONGO-1912
	public void autogeneratesIdForMap() {

		ReactiveMongoTemplate template = spy(this.template);
		doReturn(Mono.just(new ObjectId())).when(template).saveDocument(any(String.class), any(Document.class),
				any(Class.class));

		Map<String, String> entity = new LinkedHashMap<>();
		StepVerifier.create(template.save(entity, "foo")).consumeNextWith(actual -> {

			assertThat(entity, hasKey("_id"));
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

		when(collection.findOneAndUpdate(any(Bson.class), any(), any())).thenReturn(Mono.empty());

		template.findAndModify(new BasicQuery("{}").collation(Collation.of("fr")), new Update(), AutogenerateableId.class)
				.subscribe();

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-1518
	public void findAndRemoveShouldUseCollationWhenPresent() {

		when(collection.findOneAndDelete(any(Bson.class), any())).thenReturn(Mono.empty());

		template.findAndRemove(new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class).subscribe();

		ArgumentCaptor<FindOneAndDeleteOptions> options = ArgumentCaptor.forClass(FindOneAndDeleteOptions.class);
		verify(collection).findOneAndDelete(any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-1518
	public void findAndRemoveManyShouldUseCollationWhenPresent() {

		when(collection.deleteMany(any(Bson.class), any())).thenReturn(Mono.empty());

		template.doRemove("collection-1", new BasicQuery("{}").collation(Collation.of("fr")), AutogenerateableId.class)
				.subscribe();

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-1518
	public void updateOneShouldUseCollationWhenPresent() {

		when(collection.updateOne(any(Bson.class), any(), any())).thenReturn(Mono.empty());

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), new Update().set("foo", "bar"),
				AutogenerateableId.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-1518
	public void updateManyShouldUseCollationWhenPresent() {

		when(collection.updateMany(any(Bson.class), any(), any())).thenReturn(Mono.empty());

		template.updateMulti(new BasicQuery("{}").collation(Collation.of("fr")), new Update().set("foo", "bar"),
				AutogenerateableId.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));

	}

	@Test // DATAMONGO-1518
	public void replaceOneShouldUseCollationWhenPresent() {

		when(collection.replaceOne(any(Bson.class), any(), any(ReplaceOptions.class))).thenReturn(Mono.empty());

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), new Update(), AutogenerateableId.class)
				.subscribe();

		ArgumentCaptor<ReplaceOptions> options = ArgumentCaptor.forClass(ReplaceOptions.class);
		verify(collection).replaceOne(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
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

		template.doFind("star-wars", new Document(), new Document(), Person.class, PersonProjection.class, null)
				.subscribe();

		verify(findPublisher).projection(eq(new Document("firstname", 1)));
	}

	@Test // DATAMONGO-1719
	public void doesNotApplyFieldsWhenInterfaceProjectionIsClosedAndQueryDefinesFields() {

		template.doFind("star-wars", new Document(), new Document("bar", 1), Person.class, PersonProjection.class, null)
				.subscribe();

		verify(findPublisher).projection(eq(new Document("bar", 1)));
	}

	@Test // DATAMONGO-1719
	public void doesNotApplyFieldsWhenInterfaceProjectionIsOpen() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, PersonSpELProjection.class, null)
				.subscribe();

		verify(findPublisher, never()).projection(any());
	}

	@Test // DATAMONGO-1719, DATAMONGO-2041
	public void appliesFieldsToDtoProjection() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, Jedi.class, null).subscribe();

		verify(findPublisher).projection(eq(new Document("firstname", 1)));
	}

	@Test // DATAMONGO-1719
	public void doesNotApplyFieldsToDtoProjectionWhenQueryDefinesFields() {

		template.doFind("star-wars", new Document(), new Document("bar", 1), Person.class, Jedi.class, null).subscribe();

		verify(findPublisher).projection(eq(new Document("bar", 1)));
	}

	@Test // DATAMONGO-1719
	public void doesNotApplyFieldsWhenTargetIsNotAProjection() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, Person.class, null).subscribe();

		verify(findPublisher, never()).projection(any());
	}

	@Test // DATAMONGO-1719
	public void doesNotApplyFieldsWhenTargetExtendsDomainType() {

		template.doFind("star-wars", new Document(), new Document(), Person.class, PersonExtended.class, null).subscribe();

		verify(findPublisher, never()).projection(any());
	}

	@Test // DATAMONGO-1783
	public void countShouldUseSkipFromQuery() {

		template.count(new Query().skip(10), Person.class, "star-wars").subscribe();

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).count(any(), options.capture());

		assertThat(options.getValue().getSkip(), is(equalTo(10)));
	}

	@Test // DATAMONGO-1783
	public void countShouldUseLimitFromQuery() {

		template.count(new Query().limit(100), Person.class, "star-wars").subscribe();

		ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
		verify(collection).count(any(), options.capture());

		assertThat(options.getValue().getLimit(), is(equalTo(100)));
	}

	@Test // DATAMONGO-2215
	public void updateShouldApplyArrayFilters() {

		template.updateFirst(new BasicQuery("{}"),
				new Update().set("grades.$[element]", 100).filterArray(Criteria.where("element").gte(100)),
				EntityWithListOfSimple.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(), options.capture());

		Assertions.assertThat((List<Bson>) options.getValue().getArrayFilters())
				.contains(new org.bson.Document("element", new Document("$gte", 100)));
	}

	@Test // DATAMONGO-2215
	public void findAndModifyShouldApplyArrayFilters() {

		template.findAndModify(new BasicQuery("{}"),
				new Update().set("grades.$[element]", 100).filterArray(Criteria.where("element").gte(100)),
				EntityWithListOfSimple.class).subscribe();

		ArgumentCaptor<FindOneAndUpdateOptions> options = ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);
		verify(collection).findOneAndUpdate(any(), any(), options.capture());

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
		verify(collection).findOneAndUpdate(any(), any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void findAndRemoveShouldUseDefaultCollationWhenPresent() {

		template.findAndRemove(new BasicQuery("{}"), Sith.class).subscribe();

		ArgumentCaptor<FindOneAndDeleteOptions> options = ArgumentCaptor.forClass(FindOneAndDeleteOptions.class);
		verify(collection).findOneAndDelete(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
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

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void createCollectionShouldFavorExplicitOptionsOverDefaultCollation() {

		template.createCollection(Sith.class, CollectionOptions.just(Collation.of("en_US"))).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("en_US").build()));
	}

	@Test // DATAMONGO-1854
	public void createCollectionShouldUseDefaultCollationIfCollectionOptionsAreNull() {

		template.createCollection(Sith.class, null).subscribe();

		ArgumentCaptor<CreateCollectionOptions> options = ArgumentCaptor.forClass(CreateCollectionOptions.class);
		verify(db).createCollection(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
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

	@Test // DATAMONGO-18545
	public void findAndReplaceShouldUseCollationWhenPresent() {

		template.findAndReplace(new BasicQuery("{}").collation(Collation.of("fr")), new Jedi()).subscribe();

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
	}

	@Test // DATAMONGO-18545
	public void findAndReplaceShouldUseDefaultCollationWhenPresent() {

		template.findAndReplace(new BasicQuery("{}"), new Sith()).subscribe();

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("de_AT"));
	}

	@Test // DATAMONGO-18545
	public void findAndReplaceShouldUseCollationEvenIfDefaultCollationIsPresent() {

		template.findAndReplace(new BasicQuery("{}").collation(Collation.of("fr")), new MongoTemplateUnitTests.Sith())
				.subscribe();

		ArgumentCaptor<FindOneAndReplaceOptions> options = ArgumentCaptor.forClass(FindOneAndReplaceOptions.class);
		verify(collection).findOneAndReplace(any(Bson.class), any(), options.capture());

		assertThat(options.getValue().getCollation().getLocale(), is("fr"));
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
		verify(collection).updateOne(any(), any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void updateFirstShouldPreferExplicitCollationOverDefaultCollation() {

		template.updateFirst(new BasicQuery("{}").collation(Collation.of("fr")), Update.update("foo", "bar"), Sith.class)
				.subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateOne(any(), any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	public void updateMultiShouldUseDefaultCollationWhenPresent() {

		template.updateMulti(new BasicQuery("{}"), Update.update("foo", "bar"), Sith.class).subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void updateMultiShouldPreferExplicitCollationOverDefaultCollation() {

		template.updateMulti(new BasicQuery("{}").collation(Collation.of("fr")), Update.update("foo", "bar"), Sith.class)
				.subscribe();

		ArgumentCaptor<UpdateOptions> options = ArgumentCaptor.forClass(UpdateOptions.class);
		verify(collection).updateMany(any(), any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1854
	public void removeShouldUseDefaultCollationWhenPresent() {

		template.remove(new BasicQuery("{}"), Sith.class).subscribe();

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("de_AT").build()));
	}

	@Test // DATAMONGO-1854
	public void removeShouldPreferExplicitCollationOverDefaultCollation() {

		template.remove(new BasicQuery("{}").collation(Collation.of("fr")), Sith.class).subscribe();

		ArgumentCaptor<DeleteOptions> options = ArgumentCaptor.forClass(DeleteOptions.class);
		verify(collection).deleteMany(any(), options.capture());

		assertThat(options.getValue().getCollation(),
				is(com.mongodb.client.model.Collation.builder().locale("fr").build()));
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
}
