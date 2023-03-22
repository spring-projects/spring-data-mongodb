/*
 * Copyright 2023 the original author or authors.
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

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Optional;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.DefaultBulkOperationsUnitTests.NullExceptionTranslator;
import org.springframework.data.mongodb.core.DefaultReactiveBulkOperations.ReactiveBulkOperationContext;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.ReactiveAfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.MongoWriteException;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DefaultReactiveBulkOperationsUnitTests {

	ReactiveMongoTemplate template;
	@Mock ReactiveMongoDatabaseFactory factory;

	@Mock MongoDatabase database;
	@Mock(answer = Answers.RETURNS_DEEP_STUBS) MongoCollection<Document> collection;
	@Captor ArgumentCaptor<List<WriteModel<Document>>> captor;

	private MongoConverter converter;
	private MongoMappingContext mappingContext;

	private DefaultReactiveBulkOperations ops;

	@BeforeEach
	void setUp() {

		when(factory.getMongoDatabase()).thenReturn(Mono.just(database));
		when(factory.getExceptionTranslator()).thenReturn(new NullExceptionTranslator());
		when(database.getCollection(anyString(), eq(Document.class))).thenReturn(collection);
		when(collection.bulkWrite(anyList(), any())).thenReturn(Mono.just(mock(BulkWriteResult.class)));

		mappingContext = new MongoMappingContext();
		mappingContext.afterPropertiesSet();

		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		template = new ReactiveMongoTemplate(factory, converter);

		ops = new DefaultReactiveBulkOperations(template, "collection-1",
				new ReactiveBulkOperationContext(BulkMode.ORDERED,
						Optional.of(mappingContext.getPersistentEntity(SomeDomainType.class)), new QueryMapper(converter),
						new UpdateMapper(converter), null, null));
	}

	@Test // GH-2821
	void updateOneShouldUseCollationWhenPresent() {

		ops.updateOne(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.execute().subscribe();

		verify(collection).bulkWrite(captor.capture(), any());
		assertThat(captor.getValue().get(0)).isInstanceOf(UpdateOneModel.class);
		assertThat(((UpdateOneModel<Document>) captor.getValue().get(0)).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-2821
	void replaceOneShouldUseCollationWhenPresent() {

		ops.replaceOne(new BasicQuery("{}").collation(Collation.of("de")), new SomeDomainType()).execute().subscribe();

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(ReplaceOneModel.class);
		assertThat(((ReplaceOneModel<Document>) captor.getValue().get(0)).getReplaceOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-2821
	void removeShouldUseCollationWhenPresent() {

		ops.remove(new BasicQuery("{}").collation(Collation.of("de"))).execute().subscribe();

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(DeleteManyModel.class);
		assertThat(((DeleteManyModel<Document>) captor.getValue().get(0)).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-2821
	void bulkUpdateShouldMapQueryAndUpdateCorrectly() {

		ops.updateOne(query(where("firstName").is("danerys")), Update.update("firstName", "queen danerys")).execute()
				.subscribe();

		verify(collection).bulkWrite(captor.capture(), any());

		UpdateOneModel<Document> updateModel = (UpdateOneModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
		assertThat(updateModel.getUpdate()).isEqualTo(new Document("$set", new Document("first_name", "queen danerys")));
	}

	@Test // GH-2821
	void bulkRemoveShouldMapQueryCorrectly() {

		ops.remove(query(where("firstName").is("danerys"))).execute().subscribe();

		verify(collection).bulkWrite(captor.capture(), any());

		DeleteManyModel<Document> updateModel = (DeleteManyModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
	}

	@Test // GH-2821
	void bulkReplaceOneShouldMapQueryCorrectly() {

		SomeDomainType replacement = new SomeDomainType();
		replacement.firstName = "Minsu";
		replacement.lastName = "Kim";

		ops.replaceOne(query(where("firstName").is("danerys")), replacement).execute().subscribe();

		verify(collection).bulkWrite(captor.capture(), any());

		ReplaceOneModel<Document> updateModel = (ReplaceOneModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
		assertThat(updateModel.getReplacement().getString("first_name")).isEqualTo("Minsu");
		assertThat(updateModel.getReplacement().getString("lastName")).isEqualTo("Kim");
	}

	@Test // GH-2821
	void bulkInsertInvokesEntityCallbacks() {

		BeforeConvertPersonCallback beforeConvertCallback = spy(new BeforeConvertPersonCallback());
		BeforeSavePersonCallback beforeSaveCallback = spy(new BeforeSavePersonCallback());
		AfterSavePersonCallback afterSaveCallback = spy(new AfterSavePersonCallback());

		ops = new DefaultReactiveBulkOperations(template, "collection-1",
				new ReactiveBulkOperationContext(BulkMode.ORDERED,
						Optional.of(mappingContext.getPersistentEntity(Person.class)), new QueryMapper(converter),
						new UpdateMapper(converter), null,
						ReactiveEntityCallbacks.create(beforeConvertCallback, beforeSaveCallback, afterSaveCallback)));

		Person entity = new Person("init");
		ops.insert(entity);

		ArgumentCaptor<Person> personArgumentCaptor = ArgumentCaptor.forClass(Person.class);
		verifyNoInteractions(beforeConvertCallback);
		verifyNoInteractions(beforeSaveCallback);

		ops.execute().then().as(StepVerifier::create).verifyComplete();

		verify(beforeConvertCallback).onBeforeConvert(personArgumentCaptor.capture(), eq("collection-1"));
		verify(beforeSaveCallback).onBeforeSave(personArgumentCaptor.capture(), any(), eq("collection-1"));
		verify(afterSaveCallback).onAfterSave(personArgumentCaptor.capture(), any(), eq("collection-1"));
		assertThat(personArgumentCaptor.getAllValues()).extracting("firstName").containsExactly("init", "before-convert",
				"before-save");
		verify(collection).bulkWrite(captor.capture(), any());

		InsertOneModel<Document> updateModel = (InsertOneModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getDocument()).containsEntry("firstName", "after-save");
	}

	@Test // GH-2821
	void bulkReplaceOneEmitsEventsCorrectly() {

		ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);

		ops = new DefaultReactiveBulkOperations(template, "collection-1",
				new ReactiveBulkOperationContext(BulkMode.ORDERED,
						Optional.of(mappingContext.getPersistentEntity(Person.class)), new QueryMapper(converter),
						new UpdateMapper(converter), eventPublisher, null));

		ops.replaceOne(query(where("firstName").is("danerys")), new SomeDomainType());

		verify(eventPublisher, never()).publishEvent(any(BeforeConvertEvent.class));
		verify(eventPublisher, never()).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher, never()).publishEvent(any(AfterSaveEvent.class));

		ops.execute().then().as(StepVerifier::create).verifyComplete();

		verify(eventPublisher).publishEvent(any(BeforeConvertEvent.class));
		verify(eventPublisher).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher).publishEvent(any(AfterSaveEvent.class));
	}

	@Test // GH-2821
	void bulkInsertEmitsEventsCorrectly() {

		ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);

		ops = new DefaultReactiveBulkOperations(template, "collection-1",
				new ReactiveBulkOperationContext(BulkMode.ORDERED,
						Optional.of(mappingContext.getPersistentEntity(Person.class)), new QueryMapper(converter),
						new UpdateMapper(converter), eventPublisher, null));

		ops.insert(new SomeDomainType());

		verify(eventPublisher, never()).publishEvent(any(BeforeConvertEvent.class));
		verify(eventPublisher, never()).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher, never()).publishEvent(any(AfterSaveEvent.class));

		ops.execute().then().as(StepVerifier::create).verifyComplete();

		verify(eventPublisher).publishEvent(any(BeforeConvertEvent.class));
		verify(eventPublisher).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher).publishEvent(any(AfterSaveEvent.class));
	}

	@Test // GH-2821
	void noAfterSaveEventOnFailure() {

		ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);

		when(collection.bulkWrite(anyList(), any(BulkWriteOptions.class))).thenThrow(new MongoWriteException(
				new WriteError(89, "NetworkTimeout", new BsonDocument("hi", new BsonString("there"))), null));

		ops = new DefaultReactiveBulkOperations(template, "collection-1",
				new ReactiveBulkOperationContext(BulkMode.ORDERED,
						Optional.of(mappingContext.getPersistentEntity(Person.class)), new QueryMapper(converter),
						new UpdateMapper(converter), eventPublisher, null));

		ops.insert(new SomeDomainType());

		ops.execute().as(StepVerifier::create).expectError();

		verify(eventPublisher, never()).publishEvent(any(AfterSaveEvent.class));
	}

	@Test // GH-2821
	void appliesArrayFilterWhenPresent() {

		ops.updateOne(new BasicQuery("{}"), new Update().filterArray(Criteria.where("element").gte(100))).execute()
				.subscribe();

		verify(collection).bulkWrite(captor.capture(), any());

		UpdateOneModel<Document> updateModel = (UpdateOneModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getOptions().getArrayFilters().get(0))
				.isEqualTo(new org.bson.Document("element", new Document("$gte", 100)));
	}

	static class BeforeConvertPersonCallback implements ReactiveBeforeConvertCallback<Person> {

		@Override
		public Mono<Person> onBeforeConvert(Person entity, String collection) {
			return Mono.just(new Person("before-convert"));
		}
	}

	static class BeforeSavePersonCallback implements ReactiveBeforeSaveCallback<Person> {

		@Override
		public Mono<Person> onBeforeSave(Person entity, Document document, String collection) {

			document.put("firstName", "before-save");
			return Mono.just(new Person("before-save"));
		}
	}

	static class AfterSavePersonCallback implements ReactiveAfterSaveCallback<Person> {

		@Override
		public Mono<Person> onAfterSave(Person entity, Document document, String collection) {

			document.put("firstName", "after-save");
			return Mono.just(new Person("after-save"));
		}
	}

	class SomeDomainType {

		@Id String id;
		DefaultBulkOperationsUnitTests.Gender gender;
		@Field("first_name") String firstName;
		@Field String lastName;
	}

	enum Gender {
		M, F
	}
}
