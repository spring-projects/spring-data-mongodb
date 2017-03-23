/*
 * Copyright 2012-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.index;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.hamcrest.core.IsEqual;
import org.hamcrest.number.IsCloseTo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.Point;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.DefaultIndexOperations;
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
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoPersistentEntityIndexCreatorUnitTests {

	private @Mock MongoDbFactory factory;
	private @Mock ApplicationContext context;
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
		when(db.getCollection(collectionCaptor.capture())).thenReturn(collection);

		mongoTemplate = new MongoTemplate(factory);

		when(collection.createIndex(keysCaptor.capture(), optionsCaptor.capture())).thenReturn("OK");
	}

	@Test
	public void buildsIndexDefinitionUsingFieldName() {

		MongoMappingContext mappingContext = prepareMappingContext(Person.class);

		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		assertThat(keysCaptor.getValue(), is(notNullValue()));
		assertThat(keysCaptor.getValue().keySet(), hasItem("fieldname"));
		assertThat(optionsCaptor.getValue().getName(), is("indexName"));
		assertThat(optionsCaptor.getValue().isBackground(), is(false));
		assertThat(optionsCaptor.getValue().getExpireAfter(TimeUnit.SECONDS), nullValue());
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
		assertThat(creator.isIndexCreatorFor(mappingContext), is(true));
		assertThat(creator.isIndexCreatorFor(new MongoMappingContext()), is(false));
	}

	@Test // DATAMONGO-554
	public void triggersBackgroundIndexingIfConfigured() {

		MongoMappingContext mappingContext = prepareMappingContext(AnotherPerson.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		assertThat(keysCaptor.getValue(), is(notNullValue()));
		assertThat(keysCaptor.getValue().keySet(), hasItem("lastname"));
		assertThat(optionsCaptor.getValue().getName(), is("lastname"));
		assertThat(optionsCaptor.getValue().isBackground(), IsEqual.<Object> equalTo(true));
		assertThat(optionsCaptor.getValue().getExpireAfter(TimeUnit.SECONDS), nullValue());
	}

	@Test // DATAMONGO-544
	public void expireAfterSecondsIfConfigured() {

		MongoMappingContext mappingContext = prepareMappingContext(Milk.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		assertThat(keysCaptor.getValue(), is(notNullValue()));
		assertThat(keysCaptor.getValue().keySet(), hasItem("expiry"));
		assertThat(optionsCaptor.getValue().getExpireAfter(TimeUnit.SECONDS), IsEqual.<Object> equalTo(60L));
	}

	@Test // DATAMONGO-899
	public void createsNotNestedGeoSpatialIndexCorrectly() {

		MongoMappingContext mappingContext = prepareMappingContext(Wrapper.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		assertThat(keysCaptor.getValue(), equalTo(new org.bson.Document().append("company.address.location", "2d")));

		IndexOptions opts = optionsCaptor.getValue();
		assertThat(opts.getName(), is(equalTo("company.address.location")));
		assertThat(opts.getMin(), IsCloseTo.closeTo(-180, 0));
		assertThat(opts.getMax(), IsCloseTo.closeTo(180, 0));
		assertThat(opts.getBits(), is(26));
	}

	@Test // DATAMONGO-827
	public void autoGeneratedIndexNameShouldGenerateNoName() {

		MongoMappingContext mappingContext = prepareMappingContext(EntityWithGeneratedIndexName.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		assertThat(keysCaptor.getValue().containsKey("name"), is(false));
		assertThat(keysCaptor.getValue().keySet(), hasItem("lastname"));

		assertThat(optionsCaptor.getValue().getName(), nullValue());
	}

	@Test // DATAMONGO-367
	public void indexCreationShouldNotCreateNewCollectionForNestedGeoSpatialIndexStructures() {

		MongoMappingContext mappingContext = prepareMappingContext(Wrapper.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		ArgumentCaptor<String> collectionNameCapturer = ArgumentCaptor.forClass(String.class);

		verify(db, times(1)).getCollection(collectionNameCapturer.capture());
		assertThat(collectionNameCapturer.getValue(), equalTo("wrapper"));
	}

	@Test // DATAMONGO-367
	public void indexCreationShouldNotCreateNewCollectionForNestedIndexStructures() {

		MongoMappingContext mappingContext = prepareMappingContext(IndexedDocumentWrapper.class);
		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);

		ArgumentCaptor<String> collectionNameCapturer = ArgumentCaptor.forClass(String.class);

		verify(db, times(1)).getCollection(collectionNameCapturer.capture());
		assertThat(collectionNameCapturer.getValue(), equalTo("indexedDocumentWrapper"));
	}

	@Test(expected = DataAccessException.class) // DATAMONGO-1125
	public void createIndexShouldUsePersistenceExceptionTranslatorForNonDataIntegrityConcerns() {

		when(factory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());
		doThrow(new MongoException(6, "HostUnreachable")).when(collection).createIndex(Mockito.any(org.bson.Document.class),
				Mockito.any(IndexOptions.class));

		MongoMappingContext mappingContext = prepareMappingContext(Person.class);

		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);
	}

	@Test(expected = ClassCastException.class) // DATAMONGO-1125
	public void createIndexShouldNotConvertUnknownExceptionTypes() {

		when(factory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());
		doThrow(new ClassCastException("o_O")).when(collection).createIndex(Mockito.any(org.bson.Document.class),
				Mockito.any(IndexOptions.class));

		MongoMappingContext mappingContext = prepareMappingContext(Person.class);

		new MongoPersistentEntityIndexCreator(mappingContext, mongoTemplate);
	}

	private static MongoMappingContext prepareMappingContext(Class<?> type) {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(Collections.singleton(type));
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
}
