/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.mongodb;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.MongoOperations;
import org.springframework.data.mongodb.MongoTemplate;
import org.springframework.data.mongodb.convert.MappingMongoConverter;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for {@link MongoTemplate}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoTemplateUnitTests extends MongoOperationsUnitTests {

	MongoTemplate template;

	@Mock
	Mongo mongo;

	@Mock
	DB db;

	@Before
	public void setUp() {
		this.template = new MongoTemplate(mongo, "database");
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullDatabaseName() throws Exception {
		new MongoTemplate(mongo, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullMongo() throws Exception {
		new MongoTemplate(null, "database");
	}

	@Test(expected = DataAccessException.class)
	public void removeHandlesMongoExceptionProperly() throws Exception {
		MongoTemplate template = mockOutGetDb();
		when(db.getCollection("collection")).thenThrow(new MongoException("Exception!"));

		template.remove(null, "collection");
	}

	@Test
	public void defaultsConverterToMappingMongoConverter() throws Exception {
		MongoTemplate template = new MongoTemplate(mongo, "database");
		assertTrue(ReflectionTestUtils.getField(template, "mongoConverter") instanceof MappingMongoConverter);
	}

	/**
	 * Mocks out the {@link MongoTemplate#getDb()} method to return the {@link DB} mock instead of executing the actual
	 * behaviour.
	 * 
	 * @return
	 */
	private MongoTemplate mockOutGetDb() {

		MongoTemplate template = spy(this.template);
		stub(template.getDb()).toReturn(db);
		return template;
	}

	/* (non-Javadoc)
	  * @see org.springframework.data.mongodb.MongoOperationsUnitTests#getOperations()
	  */
	@Override
	protected MongoOperations getOperationsForExceptionHandling() {
		MongoTemplate template = spy(this.template);
		stub(template.getDb()).toThrow(new MongoException("Error!"));
		return template;
	}

	/* (non-Javadoc)
	  * @see org.springframework.data.mongodb.MongoOperationsUnitTests#getOperations()
	  */
	@Override
	protected MongoOperations getOperations() {
		return this.template;
	}
}
