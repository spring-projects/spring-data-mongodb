/*
 * Copyright 2026-present the original author or authors.
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

import java.util.Collection;
import java.util.List;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockingDetails;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.context.ApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.mongodb.core.bulk.Bulk;
import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
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
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.internal.client.model.bulk.AbstractClientNamespacedWriteModel;
import com.mongodb.internal.client.model.bulk.ClientWriteModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientDeleteManyModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientInsertOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientReplaceOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateManyModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateOneModel;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Unit tests for {@link ReactiveBulkWriter} through {@link ReactiveMongoTemplate}. Tests use at least two collections
 * so that {@code client.bulkWrite} is exercised (multi-collection path).
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveBulkWriterUnitTests {

	/** Simple insert in another collection so bulk has multiple namespaces and client.bulkWrite is used. */
	private static Document simpleInsertInOtherCollection() {
		return new Document("_id", 1);
	}

	private ReactiveMongoTemplate template;
	@Mock MongoClient client;
	@Mock MongoDatabase database;
	@Mock(answer = Answers.RETURNS_DEEP_STUBS) MongoCollection<Document> collection;
	SimpleReactiveMongoDatabaseFactory factory;
	@Mock DbRefResolver dbRefResolver;

	@Mock ApplicationContext applicationContext;
	@Captor ArgumentCaptor<List<ClientNamespacedWriteModel>> captor;
	private MongoConverter converter;
	private MongoMappingContext mappingContext;

	ReactiveBeforeConvertPersonCallback beforeConvertCallback;
	ReactiveBeforeSavePersonCallback beforeSaveCallback;
	ReactiveAfterSavePersonCallback afterSaveCallback;
	ReactiveEntityCallbacks entityCallbacks;

	private Bulk.BulkSpec ops;
	private Bulk.BulkBuilder builder;

	@BeforeEach
	void setUp() {

		factory = spy(new SimpleReactiveMongoDatabaseFactory(client, "default-db"));
		when(factory.getMongoCluster()).thenReturn(client);
		when(factory.getMongoDatabase()).thenReturn(Mono.just(database));
		when(factory.getExceptionTranslator()).thenReturn(new NullExceptionTranslator());
		when(database.getCollection(anyString(), eq(Document.class))).thenReturn(collection);
		when(database.getName()).thenReturn("default-db");
		when(client.bulkWrite(anyList(), any())).thenReturn(Mono.just(Mockito.mock(ClientBulkWriteResult.class)));

		beforeConvertCallback = spy(new ReactiveBeforeConvertPersonCallback());
		beforeSaveCallback = spy(new ReactiveBeforeSavePersonCallback());
		afterSaveCallback = spy(new ReactiveAfterSavePersonCallback());
		entityCallbacks = ReactiveEntityCallbacks.create(beforeConvertCallback, beforeSaveCallback, afterSaveCallback);

		mappingContext = new MongoMappingContext();
		mappingContext.afterPropertiesSet();

		converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		template = new ReactiveMongoTemplate(factory, converter);
		template.setApplicationEventPublisher(applicationContext);
		template.setEntityCallbacks(entityCallbacks);

		builder = Bulk.builder();
		builder.inCollection("default-collection", it -> {
			ops = it;
		});
	}

	@Test // GH-5087
	void delegatesToCollectionOnSingleNamespace() {

		ops.insert(new BaseDoc());

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).subscribe();

		verifyNoInteractions(client);
		verify(collection).bulkWrite(anyList(), any());
	}

	@Test // GH-5087
	void delegatesToClientOnMultiNamespace() {

		ops.insert(new BaseDoc());
		builder.inCollection("other-collection", it -> it.insert(new BaseDoc()));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).subscribe();

		verify(client).bulkWrite(anyList(), any());
		verifyNoInteractions(collection);
	}

	@Test // GH-5087
	void updateOneShouldUseCollationWhenPresent() {

		builder.inCollection("other-collection", it -> it.insert(simpleInsertInOtherCollection()))
				.inCollection("default-collection", it -> it.updateOne(new BasicQuery("{}").collation(Collation.of("de")),
						new Update().set("lastName", "targaryen")));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).block();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientUpdateOneModel.class, captor.getValue().get(1)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void updateManyShouldUseCollationWhenPresent() {

		builder.inCollection("other-collection", it -> it.insert(simpleInsertInOtherCollection()))
				.inCollection("default-collection", it -> it.updateMulti(new BasicQuery("{}").collation(Collation.of("de")),
						new Update().set("lastName", "targaryen")));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).block();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientUpdateManyModel.class, captor.getValue().get(1)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void removeShouldUseCollationWhenPresent() {

		builder.inCollection("other-collection", it -> it.insert(simpleInsertInOtherCollection()))
				.inCollection("default-collection", it -> it.remove(new BasicQuery("{}").collation(Collation.of("de"))));

		builder.inCollection("other-collection",
				it -> it.updateOne(new BasicQuery("{}"), new Update().set("items.$.documents.0.fileId", "new-id")));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).block();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientDeleteManyModel.class, captor.getValue().get(1)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void replaceOneShouldUseCollationWhenPresent() {

		builder.inCollection("other-collection", it -> it.insert(simpleInsertInOtherCollection())).inCollection(
				"default-collection",
				it -> it.replaceOne(new BasicQuery("{}").collation(Collation.of("de")), new SomeDomainType()));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).block();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientReplaceOneModel.class, captor.getValue().get(1)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void bulkUpdateShouldMapQueryAndUpdateCorrectly() {

		builder.inCollection("other-collection", it -> it.insert(simpleInsertInOtherCollection())).inCollection(
				SomeDomainType.class, "test",
				it -> it.updateOne(query(where("firstName").is("danerys")), Update.update("firstName", "queen danerys")));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(1));
		assertThat(updateModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
		assertThat(updateModel.getUpdate()).contains(new Document("$set", new Document("first_name", "queen danerys")));
	}

	@Test // GH-5087
	void bulkRemoveShouldMapQueryCorrectly() {

		builder.inCollection("other-collection", it -> it.insert(simpleInsertInOtherCollection()))
				.inCollection(SomeDomainType.class, "test", it -> it.remove(query(where("firstName").is("danerys"))));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientDeleteManyModel deleteModel = extractWriteModel(ConcreteClientDeleteManyModel.class,
				captor.getValue().get(1));
		assertThat(deleteModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
	}

	@Test // GH-5087
	void bulkReplaceOneShouldMapQueryCorrectly() {

		SomeDomainType replacement = new SomeDomainType();
		replacement.firstName = "Minsu";
		replacement.lastName = "Kim";

		builder
				.inCollection(SomeDomainType.class, "test",
						it -> it.replaceOne(query(where("firstName").is("danerys")), replacement)) //
				.inCollection("other-collection",
						it -> it.updateOne(new BasicQuery("{}"), new Update().set("items.$.documents.0.fileId", "new-id")));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientReplaceOneModel replaceModel = extractWriteModel(ConcreteClientReplaceOneModel.class,
				captor.getValue().get(0));
		assertThat(replaceModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
		assertThat(replaceModel.getReplacement()).asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
				.containsEntry("first_name", "Minsu") //
				.containsEntry("lastName", "Kim");
	}

	@Test // GH-5087
	void bulkInsertInvokesEntityCallbacks() {

		Person entity = new Person("init");
		builder.inCollection("person", it -> it.insert(entity)) //
				.inCollection("other-collection",
						it -> it.updateOne(new BasicQuery("{}"), new Update().set("items.$.documents.0.fileId", "new-id")));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).block();

		ArgumentCaptor<Person> personArgumentCaptor = ArgumentCaptor.forClass(Person.class);
		verify(beforeConvertCallback).onBeforeConvert(personArgumentCaptor.capture(), eq("person"));
		verify(beforeSaveCallback).onBeforeSave(personArgumentCaptor.capture(), any(), eq("person"));
		verify(afterSaveCallback).onAfterSave(personArgumentCaptor.capture(), any(), eq("person"));
		assertThat(personArgumentCaptor.getAllValues()).extracting("firstName").containsExactly("init", "before-convert",
				"before-save");
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientInsertOneModel insertModel = extractWriteModel(ConcreteClientInsertOneModel.class,
				captor.getValue().get(0));
		assertThat(insertModel.getDocument()).asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
				.containsEntry("firstName", "after-save");
	}

	@Test // GH-5087
	@SuppressWarnings("rawtypes")
	void bulkReplaceOneEmitsEventsCorrectly() {

		Bulk bulk = Bulk.builder().inCollection("other-collection", it -> it.insert(simpleInsertInOtherCollection()))
				.inCollection("default-collection",
						it -> it.replaceOne(query(where("firstName").is("danerys")), new SomeDomainType()))
				.build();

		verifyNoInteractions(applicationContext);

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();

		MockingDetails mockingDetails = Mockito.mockingDetails(applicationContext);
		Collection<Invocation> invocations = mockingDetails.getInvocations();
		// Insert in other-collection (3 events) + replace in default-collection (3 events)
		assertThat(invocations).hasSize(6).extracting(tt -> tt.getArgument(0)).map(Object::getClass)
				.containsExactlyInAnyOrder((Class) BeforeConvertEvent.class, (Class) BeforeConvertEvent.class,
						(Class) BeforeSaveEvent.class, (Class) BeforeSaveEvent.class, (Class) AfterSaveEvent.class,
						(Class) AfterSaveEvent.class);
	}

	@Test // GH-5087
	@SuppressWarnings("rawtypes")
	void bulkInsertEmitsEventsCorrectly() {

		Bulk bulk = Bulk.builder().inCollection("other-collection", it -> it.insert(simpleInsertInOtherCollection()))
				.inCollection("default-collection", it -> it.insert(new SomeDomainType())).build();

		verify(applicationContext, never()).publishEvent(any(BeforeConvertEvent.class));
		verify(applicationContext, never()).publishEvent(any(BeforeSaveEvent.class));
		verify(applicationContext, never()).publishEvent(any(AfterSaveEvent.class));

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();

		MockingDetails mockingDetails = Mockito.mockingDetails(applicationContext);
		Collection<Invocation> invocations = mockingDetails.getInvocations();
		// Two inserts (other-collection + default-collection) each emit BeforeConvert, BeforeSave, AfterSave
		assertThat(invocations).hasSize(6).extracting(tt -> tt.getArgument(0)).map(Object::getClass)
				.containsExactlyInAnyOrder((Class) BeforeConvertEvent.class, (Class) BeforeConvertEvent.class,
						(Class) BeforeSaveEvent.class, (Class) BeforeSaveEvent.class, (Class) AfterSaveEvent.class,
						(Class) AfterSaveEvent.class);
	}

	@Test // GH-5087
	void appliesArrayFilterWhenPresent() {

		ops.updateOne(new BasicQuery("{}"), new Update().filterArray(where("element").gte(100)));
		builder.inCollection("other-collection",
				it -> it.updateOne(new BasicQuery("{}"), new Update().set("items.$.documents.0.fileId", "new-id")));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(0));
		assertThat(updateModel.getOptions().getArrayFilters().get()).satisfies(it -> {
			assertThat((List<Document>) it).containsExactly(new Document("element", new Document("$gte", 100)));
		});
	}

	@Test // GH-5087
	void shouldRetainNestedArrayPathWithPlaceholdersForNoMatchingPaths() {

		ops.updateOne(new BasicQuery("{}"), new Update().set("items.$.documents.0.fileId", "new-id"));
		builder.inCollection("other-collection",
				it -> it.updateOne(new BasicQuery("{}"), new Update().set("items.$.documents.0.fileId", "new-id")));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(0));
		assertThat(updateModel.getUpdate())
				.contains(new Document("$set", new Document("items.$.documents.0.fileId", "new-id")));
	}

	@Test // GH-5087
	void shouldRetainNestedArrayPathWithPlaceholdersForMappedEntity() {

		builder
				.inCollection(OrderTest.class, "collection-1",
						it -> it.updateOne(new BasicQuery("{}"), Update.update("items.$.documents.0.fileId", "file-id"))) //
				.inCollection(OrderTest.class, "collection-2",
						it -> it.updateOne(new BasicQuery("{}"), Update.update("items.$.documents.0.fileId", "file-id")));

		template.bulkWrite(builder.build(), BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(1));
		assertThat(updateModel.getUpdate())
				.contains(new Document("$set", new Document("items.$.documents.0.the_file_id", "file-id")));
	}

	static <T extends ClientWriteModel> T extractWriteModel(Class<T> type, ClientNamespacedWriteModel source) {

		if (!(source instanceof AbstractClientNamespacedWriteModel)) {
			throw new IllegalArgumentException("Expected AbstractClientNamespacedWriteModel, got " + source.getClass());
		}
		ClientWriteModel model = ((AbstractClientNamespacedWriteModel) source).getModel();

		return type.cast(model);
	}

	static class OrderTest {

		String id;
		List<OrderTestItem> items;
	}

	static class OrderTestItem {

		private String cartId;
		private List<OrderTestDocument> documents;
	}

	static class OrderTestDocument {

		@Field("the_file_id") private String fileId;
	}

	static class SomeDomainType {

		@Id String id;
		Gender gender;
		@Field("first_name") String firstName;
		@Field String lastName;
	}

	enum Gender {
		M, F
	}

	static class ReactiveBeforeConvertPersonCallback implements ReactiveBeforeConvertCallback<Person> {

		@Override
		public Mono<Person> onBeforeConvert(Person entity, String collection) {
			return Mono.just(new Person("before-convert"));
		}
	}

	static class ReactiveBeforeSavePersonCallback implements ReactiveBeforeSaveCallback<Person> {

		@Override
		public Mono<Person> onBeforeSave(Person entity, Document document, String collection) {

			document.put("firstName", "before-save");
			return Mono.just(new Person("before-save"));
		}
	}

	static class ReactiveAfterSavePersonCallback implements ReactiveAfterSaveCallback<Person> {

		@Override
		public Mono<Person> onAfterSave(Person entity, Document document, String collection) {

			document.put("firstName", "after-save");
			return Mono.just(new Person("after-save"));
		}
	}

	static class NullExceptionTranslator implements PersistenceExceptionTranslator {

		@Override
		public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
			return null;
		}
	}
}
