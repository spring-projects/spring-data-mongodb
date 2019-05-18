/*
 * Copyright 2012-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.Point;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;

/**
 * Unit tests for {@link MongoPersistentEntityIndexCreator}.
 *
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Johno Crawford
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Nick Balkissoon
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoPersistentEntityIndexCreatorUnitTests {

	private @Mock MongoDbFactory factory;
	private @Mock MongoDatabase db;
	private @Mock MongoCollection<org.bson.Document> collection;
	private MongoTemplate mongoTemplate;

	ArgumentCaptor<org.bson.Document> keysCaptor;
	ArgumentCaptor<IndexOptions> optionsCaptor;
	ArgumentCaptor<String> collectionCaptor;

	@Before
	public void setUp() {

		keysCaptor = ArgumentCaptor.forClass(org.bson.Document.class);
		optionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);
		collectionCaptor = ArgumentCaptor.forClass(String.class);

		when(factory.getDb()).thenReturn(db);
		when(factory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());
		when(db.getCollection(collectionCaptor.capture(), eq(org.bson.Document.class)))
				.thenReturn((MongoCollection) collection);

		mongoTemplate = new MongoTemplate(factory);

		when(collection.createIndex(keysCaptor.capture(), optionsCaptor.capture())).thenReturn("OK");
	}

	@Test
	public void buildsIndexDefinitionUsingFieldName() {

		MongoMappingContext mappingContext = prepareMappingContext(Person.class);

		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		assertThat(keysCaptor.getValue()).isNotNull().containsKey("fieldname");
		assertThat(optionsCaptor.getValue().getName()).isEqualTo("indexName");
		assertThat(optionsCaptor.getValue().isBackground()).isFalse();
		assertThat(optionsCaptor.getValue().getExpireAfter(TimeUnit.SECONDS)).isNull();
	}

	@Test
	public void doesNotCreateIndexForEntityComingFromDifferentMappingContext() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		MongoMappingContext personMappingContext = prepareMappingContext(Person.class);

		MongoPersistentEntityIndexCreator creator = new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		MongoPersistentEntity<?> entity = personMappingContext.getRequiredPersistentEntity(Person.class);
		MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty> event = new MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty>(
				personMappingContext, entity);

		creator.onApplicationEvent(event);

		verifyZeroInteractions(collection);
	}

	@Test // DATAMONGO-530
	public void isIndexCreatorForMappingContextHandedIntoConstructor() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.initialize();

		MongoPersistentEntityIndexCreator creator = new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);
		assertThat(creator.isIndexCreatorFor(mappingContext)).isTrue();
		assertThat(creator.isIndexCreatorFor(new MongoMappingContext())).isFalse();
	}

	@Test // DATAMONGO-554
	public void triggersBackgroundIndexingIfConfigured() {

		MongoMappingContext mappingContext = prepareMappingContext(AnotherPerson.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		assertThat(keysCaptor.getValue()).isNotNull().containsKey("lastname");
		assertThat(optionsCaptor.getValue().getName()).isEqualTo("lastname");
		assertThat(optionsCaptor.getValue().isBackground()).isTrue();
		assertThat(optionsCaptor.getValue().getExpireAfter(TimeUnit.SECONDS)).isNull();
	}

	@Test // DATAMONGO-544
	public void expireAfterSecondsIfConfigured() {

		MongoMappingContext mappingContext = prepareMappingContext(Milk.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		assertThat(keysCaptor.getValue()).isNotNull().containsKey("expiry");
		assertThat(optionsCaptor.getValue().getExpireAfter(TimeUnit.SECONDS)).isEqualTo(60);
	}

	@Test // DATAMONGO-899
	public void createsNotNestedGeoSpatialIndexCorrectly() {

		MongoMappingContext mappingContext = prepareMappingContext(Wrapper.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		assertThat(keysCaptor.getValue()).isEqualTo(new org.bson.Document("company.address.location", "2d"));

		IndexOptions opts = optionsCaptor.getValue();
		assertThat(opts.getName()).isEqualTo("company.address.location");
		assertThat(opts.getMin()).isCloseTo(-180d, offset(0d));
		assertThat(opts.getMax()).isCloseTo(180d, offset(0d));
		assertThat(opts.getBits()).isEqualTo(26);
	}

	@Test // DATAMONGO-827
	public void autoGeneratedIndexNameShouldGenerateNoName() {

		MongoMappingContext mappingContext = prepareMappingContext(EntityWithGeneratedIndexName.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		assertThat(keysCaptor.getValue()).doesNotContainKey("name").containsKey("lastname");
		assertThat(optionsCaptor.getValue().getName()).isNull();
	}

	@Test // DATAMONGO-367
	public void indexCreationShouldNotCreateNewCollectionForNestedGeoSpatialIndexStructures() {

		MongoMappingContext mappingContext = prepareMappingContext(Wrapper.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		ArgumentCaptor<String> collectionNameCapturer = ArgumentCaptor.forClass(String.class);

		verify(db, times(1)).getCollection(collectionNameCapturer.capture(), any());
		assertThat(collectionNameCapturer.getValue()).isEqualTo("wrapper");
	}

	@Test // DATAMONGO-367
	public void indexCreationShouldNotCreateNewCollectionForNestedIndexStructures() {

		MongoMappingContext mappingContext = prepareMappingContext(IndexedDocumentWrapper.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		ArgumentCaptor<String> collectionNameCapturer = ArgumentCaptor.forClass(String.class);

		verify(db, times(1)).getCollection(collectionNameCapturer.capture(), any());
		assertThat(collectionNameCapturer.getValue()).isEqualTo("indexedDocumentWrapper");
	}

	@Test(expected = DataAccessException.class) // DATAMONGO-1125
	public void createIndexShouldUsePersistenceExceptionTranslatorForNonDataIntegrityConcerns() {

		doThrow(new MongoException(6, "HostUnreachable")).when(collection).createIndex(any(org.bson.Document.class),
				any(IndexOptions.class));

		MongoMappingContext mappingContext = prepareMappingContext(Person.class);

		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);
	}

	@Test(expected = ClassCastException.class) // DATAMONGO-1125
	public void createIndexShouldNotConvertUnknownExceptionTypes() {

		doThrow(new ClassCastException("o_O")).when(collection).createIndex(any(org.bson.Document.class),
				any(IndexOptions.class));

		MongoMappingContext mappingContext = prepareMappingContext(Person.class);

		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);
	}

	@Test // DATAMONGO-719
	public void createIndexShouldCreateIndexesForSamePersistentEntityButDifferentCollectionName() {

		String collection1 = "collection1";
		DynamicCollectionRouter.collectionName = collection1;
		MongoMappingContext mappingContext = prepareMappingContext(DynamicCollectionDocument.class);

		MongoPersistentEntityIndexCreator indexCreator = new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty> event = new MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty>(mappingContext, mappingContext.getRequiredPersistentEntity(DynamicCollectionDocument.class));

		String collection2 = "collection2";
		DynamicCollectionRouter.collectionName = collection2;

		indexCreator.onApplicationEvent(event);
		indexCreator.onApplicationEvent(event);

		ArgumentCaptor<String> collectionNameCapturer = ArgumentCaptor.forClass(String.class);
		verify(db, times(2)).getCollection(collectionNameCapturer.capture(), any());
		assertThat(collection1).isEqualTo(collectionNameCapturer.getAllValues().get(0));
		assertThat(collection2).isEqualTo(collectionNameCapturer.getAllValues().get(1));
	}

	@Test // DATAMONGO-719
	public void createIndexShouldCreateIndexesForDifferentPersistentEntitySameCollectionName() {

		String collection1 = "collection1";
		DynamicCollectionRouter.collectionName = collection1;
		MongoMappingContext mappingContext = prepareMappingContext(DynamicCollectionDocument.class, AnotherDynamicCollectionDocument.class);

		MongoPersistentEntityIndexCreator indexCreator = new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty> event = new MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty>(mappingContext, mappingContext.getRequiredPersistentEntity(DynamicCollectionDocument.class));
		MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty> otherEvent = new MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty>(mappingContext, mappingContext.getRequiredPersistentEntity(AnotherDynamicCollectionDocument.class));

		String collection2 = "collection2";
		DynamicCollectionRouter.collectionName = collection2;

		indexCreator.onApplicationEvent(event);
		indexCreator.onApplicationEvent(otherEvent);
		indexCreator.onApplicationEvent(event);
		indexCreator.onApplicationEvent(otherEvent);

		ArgumentCaptor<String> collectionNameCapturer = ArgumentCaptor.forClass(String.class);
		verify(db, times(4)).getCollection(collectionNameCapturer.capture(), any());
		assertThat(collection1).isEqualTo(collectionNameCapturer.getAllValues().get(0));
		assertThat(collection1).isEqualTo(collectionNameCapturer.getAllValues().get(1));
		assertThat(collection2).isEqualTo(collectionNameCapturer.getAllValues().get(2));
		assertThat(collection2).isEqualTo(collectionNameCapturer.getAllValues().get(3));
	}

	private static MongoMappingContext prepareMappingContext(Class<?>... types) {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(new HashSet<>(Arrays.asList(types)));
		mappingContext.initialize();
		return mappingContext;
	}

	@Document
	static class Person {

		@Indexed(name = "indexName") //
		@Field("fieldname") //
		String field;

	}

	@Document
	static class AnotherPerson {

		@Indexed(background = true) String lastname;
	}

	@Document
	static class Milk {

		@Indexed(expireAfterSeconds = 60) Date expiry;
	}

	@Document
	static class Wrapper {

		String id;
		Company company;

	}

	static class Company {

		String name;
		Address address;
	}

	static class Address {

		String street;
		String city;

		@GeoSpatialIndexed Point location;
	}

	@Document
	static class IndexedDocumentWrapper {

		IndexedDocument indexedDocument;
	}

	static class IndexedDocument {

		@Indexed String indexedValue;
	}

	@Document
	class EntityWithGeneratedIndexName {

		@Indexed(useGeneratedName = true, name = "ignored") String lastname;
	}

	static class DynamicCollectionRouter {
		static String collectionName;

		public static String determineCollection() {
			return collectionName;
		}
	}

	private static final String dynamicCollectionSpel = "#{ T(org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreatorUnitTests.DynamicCollectionRouter).determineCollection()}";

	@Document(collection = dynamicCollectionSpel)
	static class DynamicCollectionDocument {

		@Indexed
		String field;

	}

	@Document(collection = dynamicCollectionSpel)
	static class AnotherDynamicCollectionDocument {

		@Indexed
		String anotherField;

	}

}
