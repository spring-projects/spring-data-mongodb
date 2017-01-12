/*
 * Copyright 2014-2017 the original author or authors.
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
import static org.junit.Assume.*;
import static org.springframework.data.mongodb.core.ReflectiveDBCollectionInvoker.*;
import static org.springframework.data.mongodb.core.index.PartialIndexFilter.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.util.Version;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ObjectUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
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

	private static final Version THREE_DOT_TWO = new Version(3, 2);
	private static Version mongoVersion;

	static final DBObject GEO_SPHERE_2D = new BasicDBObject("loaction", "2dsphere");

	@Autowired MongoTemplate template;
	DefaultIndexOperations indexOps;
	DBCollection collection;

	@Before
	public void setUp() {

		queryMongoVersionIfNecessary();
		String collectionName = this.template.getCollectionName(DefaultIndexOperationsIntegrationTestsSample.class);

		this.collection = this.template.getDb().getCollection(collectionName);
		this.collection.dropIndexes();

		this.indexOps = new DefaultIndexOperations(template, collectionName);
	}

	private void queryMongoVersionIfNecessary() {

		if (mongoVersion == null) {
			CommandResult result = template.executeCommand("{ buildInfo: 1 }");
			mongoVersion = Version.parse(result.get("version").toString());
		}
	}

	@Test // DATAMONGO-1008
	public void getIndexInfoShouldBeAbleToRead2dsphereIndex() {

		collection.createIndex(GEO_SPHERE_2D);

		IndexInfo info = findAndReturnIndexInfo(GEO_SPHERE_2D);
		assertThat(info.getIndexFields().get(0).isGeo(), is(true));
	}

	@Test // DATAMONGO-1467
	public void shouldApplyPartialFilterCorrectly() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO), is(true));

		IndexDefinition id = new Index().named("partial-with-criteria").on("k3y", Direction.ASC)
				.partial(of(where("q-t-y").gte(10)));

		indexOps.ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-criteria");
		assertThat(info.getPartialFilterExpression(), is(equalTo("{ \"q-t-y\" : { \"$gte\" : 10}}")));
	}

	@Test // DATAMONGO-1467
	public void shouldApplyPartialFilterWithMappedPropertyCorrectly() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO), is(true));

		IndexDefinition id = new Index().named("partial-with-mapped-criteria").on("k3y", Direction.ASC)
				.partial(of(where("quantity").gte(10)));

		indexOps.ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-mapped-criteria");
		assertThat(info.getPartialFilterExpression(), is(equalTo("{ \"qty\" : { \"$gte\" : 10}}")));
	}

	@Test // DATAMONGO-1467
	public void shouldApplyPartialDBOFilterCorrectly() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO), is(true));

		IndexDefinition id = new Index().named("partial-with-dbo").on("k3y", Direction.ASC)
				.partial(of(new BasicDBObject("qty", new BasicDBObject("$gte", 10))));

		indexOps.ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-dbo");
		assertThat(info.getPartialFilterExpression(), is(equalTo("{ \"qty\" : { \"$gte\" : 10}}")));
	}

	@Test // DATAMONGO-1467
	public void shouldFavorExplicitMappingHintViaClass() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO), is(true));

		IndexDefinition id = new Index().named("partial-with-inheritance").on("k3y", Direction.ASC)
				.partial(of(where("age").gte(10)));

		indexOps = new DefaultIndexOperations(template,
				this.template.getCollectionName(DefaultIndexOperationsIntegrationTestsSample.class),
				MappingToSameCollection.class);

		indexOps.ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-inheritance");
		assertThat(info.getPartialFilterExpression(), is(equalTo("{ \"a_g_e\" : { \"$gte\" : 10}}")));
	}

	private IndexInfo findAndReturnIndexInfo(DBObject keys) {
		return findAndReturnIndexInfo(indexOps.getIndexInfo(), keys);
	}

	private static IndexInfo findAndReturnIndexInfo(Iterable<IndexInfo> candidates, DBObject keys) {
		return findAndReturnIndexInfo(candidates, generateIndexName(keys));
	}

	private static IndexInfo findAndReturnIndexInfo(Iterable<IndexInfo> candidates, String name) {

		for (IndexInfo info : candidates) {
			if (ObjectUtils.nullSafeEquals(name, info.getName())) {
				return info;
			}
		}
		throw new AssertionError(String.format("Index with %s was not found", name));
	}

	@Document(collection = "default-index-operations-tests")
	static class DefaultIndexOperationsIntegrationTestsSample {

		@Field("qty") Integer quantity;
	}

	@Document(collection = "default-index-operations-tests")
	static class MappingToSameCollection extends DefaultIndexOperationsIntegrationTestsSample {

		@Field("a_g_e") Integer age;
	}
}
