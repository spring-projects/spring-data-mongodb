/*
 * Copyright 2012 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Collections;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.DBObject;

/**
 * Unit tests for {@link MongoPersistentEntityIndexCreator}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoPersistentEntityIndexCreatorUnitTests {

	@Mock
	MongoDbFactory factory;

	@Test
	public void buildsIndexDefinitionUsingFieldName() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(Collections.singleton(Person.class));
		mappingContext.afterPropertiesSet();

		DummyMongoPersistentEntityIndexCreator creator = new DummyMongoPersistentEntityIndexCreator(mappingContext, factory);

		assertThat(creator.indexDefinition, is(notNullValue()));
		assertThat(creator.indexDefinition.keySet(), hasItem("fieldname"));
		assertThat(creator.name, is("indexName"));
	}

	static class Person {

		@Indexed(name = "indexName")
		@Field("fieldname")
		String field;
	}

	static class DummyMongoPersistentEntityIndexCreator extends MongoPersistentEntityIndexCreator {

		DBObject indexDefinition;
		String name;

		public DummyMongoPersistentEntityIndexCreator(MongoMappingContext mappingContext, MongoDbFactory mongoDbFactory) {
			super(mappingContext, mongoDbFactory);
		}

		@Override
		protected void ensureIndex(String collection, String name, DBObject indexDefinition, boolean unique,
				boolean dropDups, boolean sparse) {

			this.name = name;
			this.indexDefinition = indexDefinition;
		}
	}
}
