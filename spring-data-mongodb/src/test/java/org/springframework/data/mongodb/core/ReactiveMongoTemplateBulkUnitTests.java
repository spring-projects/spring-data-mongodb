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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

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
import org.springframework.data.mongodb.core.bulk.Bulk.NamespaceBoundBulkBuilder;
import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.ReactiveAfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
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

import reactor.core.publisher.Mono;

/**
 * Unit tests for {@link ReactiveMongoOperations#bulkWrite}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveMongoTemplateBulkUnitTests {

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

	private NamespaceBoundBulkBuilder<Object> ops;

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

		ops = Bulk.builder().inCollection("default-collection");
	}

	@Test // GH-5087
	void updateOneShouldUseCollationWhenPresent() {

		Bulk bulk = ops
				.updateOne(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.build();

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientUpdateOneModel.class, captor.getValue().get(0)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void updateManyShouldUseCollationWhenPresent() {

		Bulk bulk = ops
				.updateMulti(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.build();

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientUpdateManyModel.class, captor.getValue().get(0)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void removeShouldUseCollationWhenPresent() {

		Bulk bulk = ops.remove(new BasicQuery("{}").collation(Collation.of("de"))).build();

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientDeleteManyModel.class, captor.getValue().get(0)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void replaceOneShouldUseCollationWhenPresent() {

		Bulk bulk = ops.replaceOne(new BasicQuery("{}").collation(Collation.of("de")), new SomeDomainType()).build();

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientReplaceOneModel.class, captor.getValue().get(0)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void bulkUpdateShouldMapQueryAndUpdateCorrectly() {

		Bulk bulk = ops.inCollection("test", SomeDomainType.class)
				.updateOne(query(where("firstName").is("danerys")), Update.update("firstName", "queen danerys")).build();

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(0));
		assertThat(updateModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
		assertThat(updateModel.getUpdate()).contains(new Document("$set", new Document("first_name", "queen danerys")));
	}

	@Test // GH-5087
	void bulkRemoveShouldMapQueryCorrectly() {

		Bulk bulk = ops.inCollection("test", SomeDomainType.class).remove(query(where("firstName").is("danerys"))).build();

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientDeleteManyModel deleteModel = extractWriteModel(ConcreteClientDeleteManyModel.class,
				captor.getValue().get(0));
		assertThat(deleteModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
	}

	@Test // GH-5087
	void bulkReplaceOneShouldMapQueryCorrectly() {

		SomeDomainType replacement = new SomeDomainType();
		replacement.firstName = "Minsu";
		replacement.lastName = "Kim";

		Bulk bulk = ops.inCollection("test", SomeDomainType.class)
				.replaceOne(query(where("firstName").is("danerys")), replacement).build();

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientReplaceOneModel replaceModel = extractWriteModel(ConcreteClientReplaceOneModel.class,
				captor.getValue().get(0));
		assertThat(replaceModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
		assertThat(replaceModel.getReplacement()).asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
				.containsEntry("first_name", "Minsu")
				.containsEntry("lastName", "Kim");
	}

	@Test // GH-5087
	void bulkInsertInvokesEntityCallbacks() {

		Person entity = new Person("init");
		Bulk bulk = ops.inCollection("person").insert(entity).build();

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();

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

		ops.replaceOne(query(where("firstName").is("danerys")), new SomeDomainType());

		verifyNoInteractions(applicationContext);

		template.bulkWrite(ops.build(), BulkWriteOptions.ordered()).block();

		MockingDetails mockingDetails = Mockito.mockingDetails(applicationContext);
		Collection<Invocation> invocations = mockingDetails.getInvocations();
		assertThat(invocations).hasSize(3).extracting(tt -> tt.getArgument(0)).map(Object::getClass)
				.containsExactly((Class) BeforeConvertEvent.class, (Class) BeforeSaveEvent.class, (Class) AfterSaveEvent.class);
	}

	@Test // GH-5087
	@SuppressWarnings("rawtypes")
	void bulkInsertEmitsEventsCorrectly() {

		ops.insert(new SomeDomainType());

		verify(applicationContext, never()).publishEvent(any(BeforeConvertEvent.class));
		verify(applicationContext, never()).publishEvent(any(BeforeSaveEvent.class));
		verify(applicationContext, never()).publishEvent(any(AfterSaveEvent.class));

		template.bulkWrite(ops.build(), BulkWriteOptions.ordered()).block();

		MockingDetails mockingDetails = Mockito.mockingDetails(applicationContext);
		Collection<Invocation> invocations = mockingDetails.getInvocations();
		assertThat(invocations).hasSize(3).extracting(tt -> tt.getArgument(0)).map(Object::getClass)
				.containsExactly((Class) BeforeConvertEvent.class, (Class) BeforeSaveEvent.class, (Class) AfterSaveEvent.class);
	}

	@Test // GH-5087
	void appliesArrayFilterWhenPresent() {

		Bulk bulk = ops.updateOne(new BasicQuery("{}"), new Update().filterArray(where("element").gte(100))).build();

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(0));
		assertThat(updateModel.getOptions().getArrayFilters().get()).satisfies(it -> {
			assertThat((List<Document>) it).containsExactly(new Document("element", new Document("$gte", 100)));
		});
	}

	@Test // GH-5087
	void shouldRetainNestedArrayPathWithPlaceholdersForNoMatchingPaths() {

		Bulk bulk = ops.updateOne(new BasicQuery("{}"), new Update().set("items.$.documents.0.fileId", "new-id")).build();

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(0));
		assertThat(updateModel.getUpdate())
				.contains(new Document("$set", new Document("items.$.documents.0.fileId", "new-id")));
	}

	@Test // GH-5087
	void shouldRetainNestedArrayPathWithPlaceholdersForMappedEntity() {

		Bulk bulk = ops.inCollection("collection-1", OrderTest.class)
				.updateOne(new BasicQuery("{}"), Update.update("items.$.documents.0.fileId", "file-id")).build();

		template.bulkWrite(bulk, BulkWriteOptions.ordered()).block();
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(0));
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
