/*
 * Copyright 2012-2013 the original author or authors.
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.DBObject;

/**
 * Unit tests for {@link MongoPersistentEntityIndexCreator}.
 * 
 * @author Oliver Gierke
 * @author Philipp Schneider
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoPersistentEntityIndexCreatorUnitTests {

	@Mock
	MongoDbFactory factory;
	@Mock
	ApplicationContext context;

	@Test
	public void buildsIndexDefinitionUsingFieldName() {

		MongoMappingContext mappingContext = prepareMappingContext(Person.class);
		DummyMongoPersistentEntityIndexCreator creator = new DummyMongoPersistentEntityIndexCreator(mappingContext, factory);

		assertThat(creator.indexDefinition, is(notNullValue()));
		assertThat(creator.indexDefinition.keySet(), hasItem("fieldname"));
		assertThat(creator.name, is("indexName"));
		assertThat(creator.background, is(false));
	}

	@Test
	public void doesNotCreateIndexForEntityComingFromDifferentMappingContext() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		MongoMappingContext personMappingContext = prepareMappingContext(Person.class);

		DummyMongoPersistentEntityIndexCreator creator = new DummyMongoPersistentEntityIndexCreator(mappingContext, factory);

		MongoPersistentEntity<?> entity = personMappingContext.getPersistentEntity(Person.class);
		MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty> event = new MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty>(
				personMappingContext, entity);

		creator.onApplicationEvent(event);

		assertThat(creator.indexDefinition, is(nullValue()));
	}

	/**
	 * @see DATAMONGO-530
	 */
	@Test
	public void isIndexCreatorForMappingContextHandedIntoConstructor() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.initialize();

		MongoPersistentEntityIndexCreator creator = new DummyMongoPersistentEntityIndexCreator(mappingContext, factory);
		assertThat(creator.isIndexCreatorFor(mappingContext), is(true));
		assertThat(creator.isIndexCreatorFor(new MongoMappingContext()), is(false));
	}

	/**
	 * @see DATAMONGO-554
	 */
	@Test
	public void triggersBackgroundIndexingIfConfigured() {

		MongoMappingContext mappingContext = prepareMappingContext(AnotherPerson.class);
		DummyMongoPersistentEntityIndexCreator creator = new DummyMongoPersistentEntityIndexCreator(mappingContext, factory);

		assertThat(creator.indexDefinition, is(notNullValue()));
		assertThat(creator.indexDefinition.keySet(), hasItem("lastname"));
		assertThat(creator.name, is("lastname"));
		assertThat(creator.background, is(true));
	}

	private static MongoMappingContext prepareMappingContext(Class<?> type) {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(Collections.singleton(type));
		mappingContext.initialize();

		return mappingContext;
	}

	static class Person {

		@Indexed(name = "indexName")
		@Field("fieldname")
		String field;

	}

	static class AnotherPerson {

		@Indexed(background = true)
		String lastname;
	}

	static class DummyMongoPersistentEntityIndexCreator extends MongoPersistentEntityIndexCreator {

		DBObject indexDefinition;
		String name;
		boolean background;

		public DummyMongoPersistentEntityIndexCreator(MongoMappingContext mappingContext, MongoDbFactory mongoDbFactory) {
			super(mappingContext, mongoDbFactory);
		}

		@Override
		protected void ensureIndex(String collection, String name, DBObject indexDefinition, boolean unique,
				boolean dropDups, boolean sparse, boolean background) {

			this.name = name;
			this.indexDefinition = indexDefinition;
			this.background = background;
		}
	}
}
