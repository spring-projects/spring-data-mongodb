/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ObjectUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Integration tests for {@link DefaultIndexOperations}.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class DefaultIndexOperationsIntegrationTests {

	static final DBObject GEO_SPHERE_2D = new BasicDBObject("loaction", "2dsphere");

	@Autowired MongoTemplate template;
	DefaultIndexOperations indexOps;
	DBCollection collection;

	@Before
	public void setUp() {

		String collectionName = this.template.getCollectionName(DefaultIndexOperationsIntegrationTestsSample.class);

		this.collection = this.template.getDb().getCollection(collectionName);
		this.collection.dropIndexes();

		this.indexOps = new DefaultIndexOperations(template, collectionName);
	}

	/**
	 * @see DATAMONGO-1008
	 */
	@Test
	public void getIndexInfoShouldBeAbleToRead2dsphereIndex() {

		collection.createIndex(GEO_SPHERE_2D);

		IndexInfo info = findAndReturnIndexInfo(GEO_SPHERE_2D);
		assertThat(info.getIndexFields().get(0).isGeo(), is(true));
	}

	private IndexInfo findAndReturnIndexInfo(DBObject keys) {
		return findAndReturnIndexInfo(indexOps.getIndexInfo(), keys);
	}

	@SuppressWarnings("deprecation")
	private static IndexInfo findAndReturnIndexInfo(Iterable<IndexInfo> candidates, DBObject keys) {
		return findAndReturnIndexInfo(candidates, DBCollection.genIndexName(keys));
	}

	private static IndexInfo findAndReturnIndexInfo(Iterable<IndexInfo> candidates, String name) {

		for (IndexInfo info : candidates) {
			if (ObjectUtils.nullSafeEquals(name, info.getName())) {
				return info;
			}
		}
		throw new AssertionError(String.format("Index with %s was not found", name));
	}

	static class DefaultIndexOperationsIntegrationTestsSample {}
}
