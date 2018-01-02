/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import org.bson.Document;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;

/**
 * Integration test for index creation for query methods.
 *
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RepositoryIndexCreationIntegrationTests {

	@Autowired MongoOperations operations;

	@Autowired PersonRepository repository;

	@After
	public void tearDown() {
		operations.execute(Person.class, new CollectionCallback<Void>() {

			public Void doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

				List<Document> indexes = new ArrayList<Document>();
				collection.listIndexes(Document.class).into(indexes);

				for (Document index : indexes) {
					String indexName = index.get("name").toString();
					if (indexName.startsWith("find")) {
						collection.dropIndex(indexName);
					}
				}

				return null;
			}
		});
	}

	@Test
	public void testname() {

		List<IndexInfo> indexInfo = operations.indexOps(Person.class).getIndexInfo();

		assertHasIndexForField(indexInfo, "lastname");
		assertHasIndexForField(indexInfo, "firstname");
	}

	private static void assertHasIndexForField(List<IndexInfo> indexInfo, String... fields) {

		for (IndexInfo info : indexInfo) {
			if (info.isIndexForFields(Arrays.asList(fields))) {
				return;
			}
		}

		fail(String.format("Did not find index for field(s) %s in %s!", fields, indexInfo));
	}
}
