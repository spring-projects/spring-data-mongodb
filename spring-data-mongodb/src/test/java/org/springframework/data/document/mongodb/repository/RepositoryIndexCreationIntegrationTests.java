/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.document.mongodb.repository;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.document.mongodb.CollectionCallback;
import org.springframework.data.document.mongodb.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

/**
 * Integration test for index creation for query methods.
 *
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RepositoryIndexCreationIntegrationTests {

	@Autowired
	MongoOperations operations;
	
	@After
	public void tearDown() {
		operations.dropCollection(Person.class);
	}
	
	@Test
	public void testname() {
		operations.execute(Person.class, new CollectionCallback<Void>() {

			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				List<DBObject> indexInfo = collection.getIndexInfo();
				
				assertThat(indexInfo.isEmpty(), is(false));
				assertThat(indexInfo.size(), is(greaterThan(2)));
				assertThat(getIndexNamesFrom(indexInfo), hasItems("findByLastname", "findByFirstnameNotIn"));
				
				return null;
			}
		});
	}
	
	private static List<String> getIndexNamesFrom(List<DBObject> indexes) {
		List<String> result = new ArrayList<String>();
		for (DBObject dbObject : indexes) {
			result.add(dbObject.get("name").toString());
		}
		return result;
	}
}
