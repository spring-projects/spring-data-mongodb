/*
 * Copyright 2017-2023 the original author or authors.
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

import java.util.Collections;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.DefaultBulkOperations.BulkOperationContext;
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
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

/**
 * Unit tests for {@link DefaultBulkOperations}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Minsu Kim
 * @author Jens Schauder
 * @author Roman Puchkovskiy
 * @author Jacob Botuck
 */
@ExtendWith(MockitoExtension.class)
class DefaultBulkOperationsUnitTests {

	private MongoTemplate template;
	@Mock MongoDatabase database;
	@Mock(answer = Answers.RETURNS_DEEP_STUBS) MongoCollection<Document> collection;
	@Mock MongoDatabaseFactory factory;
	@Mock DbRefResolver dbRefResolver;
	@Captor ArgumentCaptor<List<WriteModel<Document>>> captor;
	private MongoConverter converter;
	private MongoMappingContext mappingContext;

	private DefaultBulkOperations ops;

	@BeforeEach
	void setUp() {

		when(factory.getMongoDatabase()).thenReturn(database);
		when(factory.getExceptionTranslator()).thenReturn(new NullExceptionTranslator());
		when(database.getCollection(anyString(), eq(Document.class))).thenReturn(collection);

		mappingContext = new MongoMappingContext();
		mappingContext.afterPropertiesSet();

		converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		template = new MongoTemplate(factory, converter);

		ops = new DefaultBulkOperations(template, "collection-1",
				new BulkOperationContext(BulkMode.ORDERED,
						Optional.of(mappingContext.getPersistentEntity(SomeDomainType.class)), new QueryMapper(converter),
						new UpdateMapper(converter), null, null));
	}

	@Test // DATAMONGO-1518
	void updateOneShouldUseCollationWhenPresent() {

		ops.updateOne(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.execute();

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(UpdateOneModel.class);
		assertThat(((UpdateOneModel<Document>) captor.getValue().get(0)).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // DATAMONGO-1518
	void updateManyShouldUseCollationWhenPresent() {

		ops.updateMulti(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.execute();

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(UpdateManyModel.class);
		assertThat(((UpdateManyModel<Document>) captor.getValue().get(0)).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // DATAMONGO-1518
	void removeShouldUseCollationWhenPresent() {

		ops.remove(new BasicQuery("{}").collation(Collation.of("de"))).execute();

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(DeleteManyModel.class);
		assertThat(((DeleteManyModel<Document>) captor.getValue().get(0)).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // DATAMONGO-2218
	void replaceOneShouldUseCollationWhenPresent() {

		ops.replaceOne(new BasicQuery("{}").collation(Collation.of("de")), new SomeDomainType()).execute();

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(ReplaceOneModel.class);
		assertThat(((ReplaceOneModel<Document>) captor.getValue().get(0)).getReplaceOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // DATAMONGO-1678
	void bulkUpdateShouldMapQueryAndUpdateCorrectly() {

		ops.updateOne(query(where("firstName").is("danerys")), Update.update("firstName", "queen danerys")).execute();

		verify(collection).bulkWrite(captor.capture(), any());

		UpdateOneModel<Document> updateModel = (UpdateOneModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
		assertThat(updateModel.getUpdate()).isEqualTo(new Document("$set", new Document("first_name", "queen danerys")));
	}

	@Test // DATAMONGO-1678
	void bulkRemoveShouldMapQueryCorrectly() {

		ops.remove(query(where("firstName").is("danerys"))).execute();

		verify(collection).bulkWrite(captor.capture(), any());

		DeleteManyModel<Document> updateModel = (DeleteManyModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
	}

	@Test // DATAMONGO-2218
	void bulkReplaceOneShouldMapQueryCorrectly() {

		SomeDomainType replacement = new SomeDomainType();
		replacement.firstName = "Minsu";
		replacement.lastName = "Kim";

		ops.replaceOne(query(where("firstName").is("danerys")), replacement).execute();

		verify(collection).bulkWrite(captor.capture(), any());

		ReplaceOneModel<Document> updateModel = (ReplaceOneModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
		assertThat(updateModel.getReplacement().getString("first_name")).isEqualTo("Minsu");
		assertThat(updateModel.getReplacement().getString("lastName")).isEqualTo("Kim");
	}

	@Test // DATAMONGO-2261, DATAMONGO-2479
	void bulkInsertInvokesEntityCallbacks() {

		BeforeConvertPersonCallback beforeConvertCallback = spy(new BeforeConvertPersonCallback());
		BeforeSavePersonCallback beforeSaveCallback = spy(new BeforeSavePersonCallback());
		AfterSavePersonCallback afterSaveCallback = spy(new AfterSavePersonCallback());

		ops = new DefaultBulkOperations(template, "collection-1",
				new BulkOperationContext(BulkMode.ORDERED, Optional.of(mappingContext.getPersistentEntity(Person.class)),
						new QueryMapper(converter), new UpdateMapper(converter), null,
						EntityCallbacks.create(beforeConvertCallback, beforeSaveCallback, afterSaveCallback)));

		Person entity = new Person("init");
		ops.insert(entity);

		ArgumentCaptor<Person> personArgumentCaptor = ArgumentCaptor.forClass(Person.class);
		verify(beforeConvertCallback).onBeforeConvert(personArgumentCaptor.capture(), eq("collection-1"));
		verifyNoInteractions(beforeSaveCallback);

		ops.execute();

		verify(beforeSaveCallback).onBeforeSave(personArgumentCaptor.capture(), any(), eq("collection-1"));
		verify(afterSaveCallback).onAfterSave(personArgumentCaptor.capture(), any(), eq("collection-1"));
		assertThat(personArgumentCaptor.getAllValues()).extracting("firstName").containsExactly("init", "before-convert",
				"before-convert");
		verify(collection).bulkWrite(captor.capture(), any());

		InsertOneModel<Document> updateModel = (InsertOneModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getDocument()).containsEntry("firstName", "after-save");
	}

	@Test // DATAMONGO-2290
	void bulkReplaceOneEmitsEventsCorrectly() {

		ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);

		ops = new DefaultBulkOperations(template, "collection-1",
				new BulkOperationContext(BulkMode.ORDERED, Optional.of(mappingContext.getPersistentEntity(Person.class)),
						new QueryMapper(converter), new UpdateMapper(converter), eventPublisher, null));

		ops.replaceOne(query(where("firstName").is("danerys")), new SomeDomainType());

		verify(eventPublisher).publishEvent(any(BeforeConvertEvent.class));
		verify(eventPublisher, never()).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher, never()).publishEvent(any(AfterSaveEvent.class));

		ops.execute();

		verify(eventPublisher).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher).publishEvent(any(AfterSaveEvent.class));
	}

	@Test // DATAMONGO-2290
	void bulkInsertEmitsEventsCorrectly() {

		ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);

		ops = new DefaultBulkOperations(template, "collection-1",
				new BulkOperationContext(BulkMode.ORDERED, Optional.of(mappingContext.getPersistentEntity(Person.class)),
						new QueryMapper(converter), new UpdateMapper(converter), eventPublisher, null));

		ops.insert(new SomeDomainType());

		verify(eventPublisher).publishEvent(any(BeforeConvertEvent.class));
		verify(eventPublisher, never()).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher, never()).publishEvent(any(AfterSaveEvent.class));

		ops.execute();

		verify(eventPublisher).publishEvent(any(BeforeSaveEvent.class));
		verify(eventPublisher).publishEvent(any(AfterSaveEvent.class));
	}

	@Test // DATAMONGO-2290
	void noAfterSaveEventOnFailure() {

		ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
		when(collection.bulkWrite(anyList(), any(BulkWriteOptions.class))).thenThrow(new MongoWriteException(
				new WriteError(89, "NetworkTimeout", new BsonDocument("hi", new BsonString("there"))), null));

		ops = new DefaultBulkOperations(template, "collection-1",
				new BulkOperationContext(BulkMode.ORDERED, Optional.of(mappingContext.getPersistentEntity(Person.class)),
						new QueryMapper(converter), new UpdateMapper(converter), eventPublisher, null));

		ops.insert(new SomeDomainType());

		verify(eventPublisher).publishEvent(any(BeforeConvertEvent.class));

		try {
			ops.execute();
			fail("Missing MongoWriteException");
		} catch (MongoWriteException expected) {

		}

		verify(eventPublisher).publishEvent(any(BeforeSaveEvent.class));
	}

	@Test // DATAMONGO-2330
	void writeConcernNotAppliedWhenNotSet() {

		ops.updateOne(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.execute();

		verify(collection, never()).withWriteConcern(any());
	}

	@Test // DATAMONGO-2330
	void writeConcernAppliedCorrectlyWhenSet() {

		ops.setDefaultWriteConcern(WriteConcern.MAJORITY);

		ops.updateOne(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.execute();

		verify(collection).withWriteConcern(eq(WriteConcern.MAJORITY));
	}

	@Test // DATAMONGO-2450
	void appliesArrayFilterWhenPresent() {

		ops.updateOne(new BasicQuery("{}"), new Update().filterArray(Criteria.where("element").gte(100))).execute();

		verify(collection).bulkWrite(captor.capture(), any());

		UpdateOneModel<Document> updateModel = (UpdateOneModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getOptions().getArrayFilters().get(0))
				.isEqualTo(new org.bson.Document("element", new Document("$gte", 100)));
	}

	@Test // DATAMONGO-2502
	void shouldRetainNestedArrayPathWithPlaceholdersForNoMatchingPaths() {

		ops.updateOne(new BasicQuery("{}"), new Update().set("items.$.documents.0.fileId", "new-id")).execute();

		verify(collection).bulkWrite(captor.capture(), any());

		UpdateOneModel<Document> updateModel = (UpdateOneModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getUpdate())
				.isEqualTo(new Document("$set", new Document("items.$.documents.0.fileId", "new-id")));
	}

	@Test // DATAMONGO-2502
	void shouldRetainNestedArrayPathWithPlaceholdersForMappedEntity() {

		DefaultBulkOperations ops = new DefaultBulkOperations(template, "collection-1",
				new BulkOperationContext(BulkMode.ORDERED, Optional.of(mappingContext.getPersistentEntity(OrderTest.class)),
						new QueryMapper(converter), new UpdateMapper(converter), null, null));

		ops.updateOne(new BasicQuery("{}"), Update.update("items.$.documents.0.fileId", "file-id")).execute();

		verify(collection).bulkWrite(captor.capture(), any());

		UpdateOneModel<Document> updateModel = (UpdateOneModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getUpdate())
				.isEqualTo(new Document("$set", new Document("items.$.documents.0.the_file_id", "file-id")));
	}

	@Test // DATAMONGO-2285
	public void translateMongoBulkOperationExceptionWithWriteConcernError() {

		when(collection.bulkWrite(anyList(), any(BulkWriteOptions.class))).thenThrow(new MongoBulkWriteException(null,
				Collections.emptyList(),
				new WriteConcernError(42, "codename", "writeconcern error happened", new BsonDocument()), new ServerAddress()));

		assertThatExceptionOfType(DataIntegrityViolationException.class)
				.isThrownBy(() -> ops.insert(new SomeDomainType()).execute());

	}

	@Test // DATAMONGO-2285
	public void translateMongoBulkOperationExceptionWithoutWriteConcernError() {

		when(collection.bulkWrite(anyList(), any(BulkWriteOptions.class))).thenThrow(new MongoBulkWriteException(null,
				Collections.singletonList(new BulkWriteError(42, "a write error happened", new BsonDocument(), 49)), null,
				new ServerAddress()));

		assertThatExceptionOfType(BulkOperationException.class)
				.isThrownBy(() -> ops.insert(new SomeDomainType()).execute());
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

		@Field("the_file_id")
		private String fileId;
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
