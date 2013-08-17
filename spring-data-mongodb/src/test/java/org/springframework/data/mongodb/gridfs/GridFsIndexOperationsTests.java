/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.mongodb.gridfs;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Integration tests for {@link GridFsIndexOperations}.
 * 
 * @author Aparna Chaudhary
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:gridfs/customgridfs.xml")
public class GridFsIndexOperationsTests {

	private static final String BUCKET = "custom";

	Resource resource = new ClassPathResource("gridfs/customgridfs.xml");

	@Autowired
	MongoDbFactory factory;
	@Autowired
	MongoConverter converter;

	GridFsOperations operations;

	@Before
	public void setUp() {
		MongoTemplate mongoTemplate = new MongoTemplate(factory);
		mongoTemplate.dropCollection(BUCKET.concat(".files"));
		mongoTemplate.dropCollection(BUCKET.concat(".chunks"));

		operations = new GridFsTemplate(factory, converter, BUCKET);
	}

	@Test
	public void testEnsureIndex() throws IOException {
		// when
		operations.indexOps().ensureIndex(new Index().on("md5", Direction.ASC).unique());

		// then
		DBCollection coll = operations.getFilesCollection();
		List<DBObject> indexInfo = coll.getIndexInfo();
		assertThat(indexInfo.size(), is(3));
		String indexKey = null;
		boolean unique = false;
		for (DBObject ix : indexInfo) {
			if ("md5_1".equals(ix.get("name"))) {
				indexKey = ix.get("key").toString();
				unique = (Boolean) ix.get("unique");
			}
		}
		assertThat(indexKey, is("{ \"md5\" : 1}"));
		assertThat(unique, is(true));
	}

	@Test
	public void testEnsureIndexOnMetadata() throws IOException {
		// when
		operations.indexOps().ensureIndex(new Index().on("metadata.key", Direction.ASC));

		// then
		DBCollection coll = operations.getFilesCollection();
		List<DBObject> indexInfo = coll.getIndexInfo();
		assertThat(indexInfo.size(), is(3));
		String indexKey = null;
		for (DBObject ix : indexInfo) {
			if ("metadata.key_1".equals(ix.get("name"))) {
				indexKey = ix.get("key").toString();
			}
		}
		assertThat(indexKey, is("{ \"metadata.key\" : 1}"));
	}

	@Test
	public void testDropIndex() throws IOException {
		// given
		operations.indexOps().ensureIndex(new Index().on("md5", Direction.ASC).unique());

		DBCollection coll = operations.getFilesCollection();
		List<DBObject> indexInfo = coll.getIndexInfo();
		assertThat(indexInfo.size(), is(3));

		// when
		operations.indexOps().dropIndex("md5_1");

		// then
		indexInfo = coll.getIndexInfo();
		assertThat(indexInfo.size(), is(2));
	}
}
