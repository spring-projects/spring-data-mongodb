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
import static org.junit.Assert.fail;
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

import java.util.List;

import org.assertj.core.api.InstanceOfAssertFactories;
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
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.NamespaceBulkOperations.NamespaceAwareBulkOperations;
import org.springframework.data.mongodb.core.NamespacedBulkOperationSupport.NamespacedBulkOperationContext;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.MongoWriteException;
import com.mongodb.WriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.internal.client.model.bulk.AbstractClientNamespacedWriteModel;
import com.mongodb.internal.client.model.bulk.ClientWriteModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientDeleteManyModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientInsertOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientReplaceOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateManyModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateOneModel;

/**
 * Unit tests for {@link NamespacedBulkOperationSupport}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class NamespacedBulkOperationsUnitTests {

	private MongoTemplate template;
	@Mock MongoClient client;
	@Mock MongoDatabase database;
	@Mock(answer = Answers.RETURNS_DEEP_STUBS) MongoCollection<Document> collection;
	MongoDatabaseFactory factory;
	@Mock DbRefResolver dbRefResolver;

	@Mock ApplicationEventPublisher eventPublisher;
	@Captor ArgumentCaptor<List<ClientNamespacedWriteModel>> captor;
	private MongoConverter converter;
	private MongoMappingContext mappingContext;

	BeforeConvertPersonCallback beforeConvertCallback;
	BeforeSavePersonCallback beforeSaveCallback;
	AfterSavePersonCallback afterSaveCallback;
	EntityCallbacks entityCallbacks;

	private NamespaceAwareBulkOperations<Object> ops;

	@BeforeEach
	void setUp() {

		factory = spy(MongoDatabaseFactory.class);
		when(factory.getCluster()).thenReturn(client);
		when(factory.getMongoDatabase()).thenReturn(database);
		when(factory.getExceptionTranslator()).thenReturn(new NullExceptionTranslator());
		when(database.getCollection(anyString(), eq(Document.class))).thenReturn(collection);
		when(database.getName()).thenReturn("default-db");

		mappingContext = new MongoMappingContext();
		mappingContext.afterPropertiesSet();

		converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		template = new MongoTemplate(factory, converter);

		beforeConvertCallback = spy(new BeforeConvertPersonCallback());
		beforeSaveCallback = spy(new BeforeSavePersonCallback());
		afterSaveCallback = spy(new AfterSavePersonCallback());

		entityCallbacks = EntityCallbacks.create(beforeConvertCallback, beforeSaveCallback, afterSaveCallback);

		NamespacedBulkOperationContext bulkOperationContext = new NamespacedBulkOperationContext(database.getName(),
				converter, new QueryMapper(converter), new UpdateMapper(converter), eventPublisher, entityCallbacks);
		ops = new NamespacedBulkOperationSupport<>(BulkMode.ORDERED, bulkOperationContext, template)
				.inCollection("default-collection");
	}

	@Test // GH-5087
	void updateOneShouldUseCollationWhenPresent() {

		ops.updateOne(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.execute();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientUpdateOneModel.class, captor.getValue().get(0)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void updateManyShouldUseCollationWhenPresent() {

		ops.updateMulti(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.execute();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientUpdateManyModel.class, captor.getValue().get(0)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void removeShouldUseCollationWhenPresent() {

		ops.remove(new BasicQuery("{}").collation(Collation.of("de"))).execute();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientDeleteManyModel.class, captor.getValue().get(0)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void replaceOneShouldUseCollationWhenPresent() {

		ops.replaceOne(new BasicQuery("{}").collation(Collation.of("de")), new SomeDomainType()).execute();

		verify(client).bulkWrite(captor.capture(), any());

		assertThat(
				extractWriteModel(ConcreteClientReplaceOneModel.class, captor.getValue().get(0)).getOptions().getCollation())
				.contains(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // GH-5087
	void bulkUpdateShouldMapQueryAndUpdateCorrectly() {

		ops.inCollection("test", SomeDomainType.class)
				.updateOne(query(where("firstName").is("danerys")), Update.update("firstName", "queen danerys")).execute();

		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(0));
		assertThat(updateModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
		assertThat(updateModel.getUpdate()).contains(new Document("$set", new Document("first_name", "queen danerys")));
	}

	@Test // GH-5087
	void bulkRemoveShouldMapQueryCorrectly() {

		ops.inCollection("test", SomeDomainType.class).remove(query(where("firstName").is("danerys"))).execute();

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

		ops.inCollection("test", SomeDomainType.class).replaceOne(query(where("firstName").is("danerys")), replacement)
				.execute();

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
		ops.switchDatabase("test").inCollection("person").insert(entity);

		ArgumentCaptor<Person> personArgumentCaptor = ArgumentCaptor.forClass(Person.class);
		verify(beforeConvertCallback).onBeforeConvert(personArgumentCaptor.capture(), eq("test.person"));
		verifyNoInteractions(beforeSaveCallback);

		ops.execute();

		verify(beforeSaveCallback).onBeforeSave(personArgumentCaptor.capture(), any(), eq("test.person"));
		verify(afterSaveCallback).onAfterSave(personArgumentCaptor.capture(), any(), eq("test.person"));
		assertThat(personArgumentCaptor.getAllValues()).extracting("firstName").containsExactly("init", "before-convert",
				"before-convert");
		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientInsertOneModel insertModel = extractWriteModel(ConcreteClientInsertOneModel.class,
				captor.getValue().get(0));
		assertThat(insertModel.getDocument()).asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
				.containsEntry("firstName", "after-save");
	}

	@Test // GH-5087
	void bulkReplaceOneEmitsEventsCorrectly() {

		ops.replaceOne(query(where("firstName").is("danerys")), new SomeDomainType());

		verify(eventPublisher).publishEvent(any(BeforeConvertEvent.class));
		verify(eventPublisher, never()).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher, never()).publishEvent(any(AfterSaveEvent.class));

		ops.execute();

		verify(eventPublisher).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher).publishEvent(any(AfterSaveEvent.class));
	}

	@Test // GH-5087
	void bulkInsertEmitsEventsCorrectly() {

		ops.insert(new SomeDomainType());

		verify(eventPublisher).publishEvent(any(BeforeConvertEvent.class));
		verify(eventPublisher, never()).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher, never()).publishEvent(any(AfterSaveEvent.class));

		ops.execute();

		verify(eventPublisher).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher).publishEvent(any(AfterSaveEvent.class));
	}

	@Test // GH-5087
	void noAfterSaveEventOnFailure() {

		// ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
		when(client.bulkWrite(anyList(), any(ClientBulkWriteOptions.class))).thenThrow(new MongoWriteException(
				new WriteError(89, "NetworkTimeout", new BsonDocument("hi", new BsonString("there"))), null));

		ops.insert(new SomeDomainType());

		verify(eventPublisher).publishEvent(any(BeforeConvertEvent.class));

		try {
			ops.execute();
			fail("Missing MongoWriteException");
		} catch (MongoWriteException expected) {

		}

		verify(eventPublisher).publishEvent(any(BeforeSaveEvent.class));
	}

	@Test // GH-5087
	void appliesArrayFilterWhenPresent() {

		ops.updateOne(new BasicQuery("{}"), new Update().filterArray(where("element").gte(100))).execute();

		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(0));
		assertThat(updateModel.getOptions().getArrayFilters().get()).satisfies(it -> {
			assertThat((List<Document>) it).containsExactly(new Document("element", new Document("$gte", 100)));
		});
	}

	@Test // GH-5087
	void shouldRetainNestedArrayPathWithPlaceholdersForNoMatchingPaths() {

		ops.updateOne(new BasicQuery("{}"), new Update().set("items.$.documents.0.fileId", "new-id")).execute();

		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(0));
		assertThat(updateModel.getUpdate())
				.contains(new Document("$set", new Document("items.$.documents.0.fileId", "new-id")));
	}

	@Test // GH-5087
	void shouldRetainNestedArrayPathWithPlaceholdersForMappedEntity() {

		ops.inCollection("collection-1", OrderTest.class)
				.updateOne(new BasicQuery("{}"), Update.update("items.$.documents.0.fileId", "file-id")).execute();

		verify(client).bulkWrite(captor.capture(), any());

		ConcreteClientUpdateOneModel updateModel = extractWriteModel(ConcreteClientUpdateOneModel.class,
				captor.getValue().get(0));
		assertThat(updateModel.getUpdate())
				.contains(new Document("$set", new Document("items.$.documents.0.the_file_id", "file-id")));
	}

	static <T extends ClientWriteModel> T extractWriteModel(Class<T> type, ClientNamespacedWriteModel source) {

		if (!(source instanceof AbstractClientNamespacedWriteModel cnwm)) {
			throw new IllegalArgumentException("Expected AbstractClientNamespacedWriteModel, got " + source.getClass());
		}
		ClientWriteModel model = cnwm.getModel();

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

	class SomeDomainType {

		@Id String id;
		Gender gender;
		@Field("first_name") String firstName;
		@Field String lastName;
	}

	enum Gender {
		M, F
	}

	static class BeforeConvertPersonCallback implements BeforeConvertCallback<Person> {

		@Override
		public Person onBeforeConvert(Person entity, String collection) {
			return new Person("before-convert");
		}
	}

	static class BeforeSavePersonCallback implements BeforeSaveCallback<Person> {

		@Override
		public Person onBeforeSave(Person entity, Document document, String collection) {

			document.put("firstName", "before-save");
			return new Person("before-save");
		}
	}

	static class AfterSavePersonCallback implements AfterSaveCallback<Person> {

		@Override
		public Person onAfterSave(Person entity, Document document, String collection) {

			document.put("firstName", "after-save");
			return new Person("after-save");
		}
	}

	static class NullExceptionTranslator implements PersistenceExceptionTranslator {

		@Override
		public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
			return null;
		}
	}
}
