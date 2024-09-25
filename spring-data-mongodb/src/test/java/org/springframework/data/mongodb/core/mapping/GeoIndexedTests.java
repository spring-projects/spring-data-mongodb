/*
 * Copyright 2011-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = GeoIndexedAppConfig.class)
public class GeoIndexedTests {

	private final String[] collectionsToDrop = new String[] { GeoIndexedAppConfig.GEO_COLLECTION, "Person" };

	@Autowired ApplicationContext applicationContext;
	@Autowired MongoTemplate template;
	@Autowired MongoMappingContext mappingContext;

	@Before
	public void setUp() {
		cleanDb();
	}

	@After
	public void cleanUp() {
		cleanDb();
	}

	private void cleanDb() {

		try (MongoClient mongo = MongoTestUtils.client()) {

			MongoDatabase db = mongo.getDatabase(GeoIndexedAppConfig.GEO_DB);

			for (String coll : collectionsToDrop) {
				db.getCollection(coll).drop();
			}
		}
	}

	@Test
	public void testGeoLocation() {

		GeoLocation geo = new GeoLocation(new double[] { 40.714346, -74.005966 });
		template.insert(geo);

		boolean hasIndex = template.execute("geolocation", new CollectionCallback<Boolean>() {
			public Boolean doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {

				List<Document> indexes = new ArrayList<Document>();
				collection.listIndexes(Document.class).into(indexes);

				for (Document document : indexes) {
					if ("location".equals(document.get("name"))) {
						return true;
					}
				}
				return false;
			}
		});

		assertThat(hasIndex).isTrue();
	}
}
