/*
 * Copyright 2012-2014 the original author or authors.
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

import java.util.Collections;
import java.util.Date;

import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.data.geo.Point;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link MongoPersistentEntityIndexCreator}.
 * 
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Johno Crawford
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoPersistentEntityIndexCreatorUnitTests {

	private @Mock MongoDbFactory factory;
	private @Mock ApplicationContext context;
	private @Mock DB db;
	private @Mock DBCollection collection;

	ArgumentCaptor<DBObject> keysCaptor;
	ArgumentCaptor<DBObject> optionsCaptor;
	ArgumentCaptor<String> collectionCaptor;

	@Before
	public void setUp() {

		keysCaptor = ArgumentCaptor.forClass(DBObject.class);
		optionsCaptor = ArgumentCaptor.forClass(DBObject.class);
		collectionCaptor = ArgumentCaptor.forClass(String.class);

		Mockito.when(factory.getDb()).thenReturn(db);
		Mockito.when(db.getCollection(collectionCaptor.capture())).thenReturn(collection);

		Mockito.doNothing().when(collection).ensureIndex(keysCaptor.capture(), optionsCaptor.capture());
	}

	@Test
	public void buildsIndexDefinitionUsingFieldName() {

		MongoMappingContext mappingContext = prepareMappingContext(Person.class);

		new MongoPersistentEntityIndexCreator(mappingContext, factory);

		assertThat(keysCaptor.getValue(), is(notNullValue()));
		assertThat(keysCaptor.getValue().keySet(), hasItem("fieldname"));
		assertThat(optionsCaptor.getValue().get("name").toString(), is("indexName"));
		assertThat(optionsCaptor.getValue().get("background"), nullValue());
		assertThat(optionsCaptor.getValue().get("expireAfterSeconds"), nullValue());
	}

	@Test
	public void doesNotCreateIndexForEntityComingFromDifferentMappingContext() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		MongoMappingContext personMappingContext = prepareMappingContext(Person.class);

		MongoPersistentEntityIndexCreator creator = new MongoPersistentEntityIndexCreator(mappingContext, factory);

		MongoPersistentEntity<?> entity = personMappingContext.getPersistentEntity(Person.class);
		MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty> event = new MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty>(
				personMappingContext, entity);

		creator.onApplicationEvent(event);

		Mockito.verifyZeroInteractions(collection);
	}

	/**
	 * @see DATAMONGO-530
	 */
	@Test
	public void isIndexCreatorForMappingContextHandedIntoConstructor() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.initialize();

		MongoPersistentEntityIndexCreator creator = new MongoPersistentEntityIndexCreator(mappingContext, factory);
		assertThat(creator.isIndexCreatorFor(mappingContext), is(true));
		assertThat(creator.isIndexCreatorFor(new MongoMappingContext()), is(false));
	}

	/**
	 * @see DATAMONGO-554
	 */
	@Test
	public void triggersBackgroundIndexingIfConfigured() {

		MongoMappingContext mappingContext = prepareMappingContext(AnotherPerson.class);
		new MongoPersistentEntityIndexCreator(mappingContext, factory);

		assertThat(keysCaptor.getValue(), is(notNullValue()));
		assertThat(keysCaptor.getValue().keySet(), hasItem("lastname"));
		assertThat(optionsCaptor.getValue().get("name").toString(), is("lastname"));
		assertThat(optionsCaptor.getValue().get("background"), IsEqual.<Object> equalTo(true));
		assertThat(optionsCaptor.getValue().get("expireAfterSeconds"), nullValue());
	}

	/**
	 * @see DATAMONGO-544
	 */
	@Test
	public void expireAfterSecondsIfConfigured() {

		MongoMappingContext mappingContext = prepareMappingContext(Milk.class);
		new MongoPersistentEntityIndexCreator(mappingContext, factory);

		assertThat(keysCaptor.getValue(), is(notNullValue()));
		assertThat(keysCaptor.getValue().keySet(), hasItem("expiry"));
		assertThat(optionsCaptor.getValue().get("expireAfterSeconds"), IsEqual.<Object> equalTo(60L));
	}

	/**
	 * @see DATAMONGO-899
	 */
	@Test
	public void createsNotNestedGeoSpatialIndexCorrectly() {

		MongoMappingContext mappingContext = prepareMappingContext(Wrapper.class);
		new MongoPersistentEntityIndexCreator(mappingContext, factory);

		assertThat(keysCaptor.getValue(), equalTo(new BasicDBObjectBuilder().add("company.address.location", "2d").get()));
		assertThat(optionsCaptor.getValue(), equalTo(new BasicDBObjectBuilder().add("name", "location").add("min", -180)
				.add("max", 180).add("bits", 26).get()));
	}

	/**
	 * @see DATAMONGO-827
	 */
	@Test
	public void autoGeneratedIndexNameShouldGenerateNoName() {

		MongoMappingContext mappingContext = prepareMappingContext(EntityWithGeneratedIndexName.class);
		new MongoPersistentEntityIndexCreator(mappingContext, factory);

		assertThat(keysCaptor.getValue().containsField("name"), is(false));
		assertThat(keysCaptor.getValue().keySet(), hasItem("lastname"));
		assertThat(optionsCaptor.getValue(), is(new BasicDBObjectBuilder().get()));
	}

	private static MongoMappingContext prepareMappingContext(Class<?> type) {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(Collections.singleton(type));
		mappingContext.initialize();

		return mappingContext;
	}

	@Document
	static class Person {

		@Indexed(name = "indexName")//
		@Field("fieldname")//
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
	class EntityWithGeneratedIndexName {

		@Indexed(useGeneratedName = true, name = "ignored") String lastname;
	}
}
