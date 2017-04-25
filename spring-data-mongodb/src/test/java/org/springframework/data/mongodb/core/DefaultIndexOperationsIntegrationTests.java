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

import static org.assertj.core.api.Assertions.*;
import static org.hamcrest.core.Is.*;
import static org.junit.Assume.*;
import static org.springframework.data.mongodb.core.index.PartialIndexFilter.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.Collation.ICUCaseFirst;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.util.Version;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ObjectUtils;

import com.mongodb.client.MongoCollection;

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
	private static final Version THREE_DOT_FOUR = new Version(3, 4);
	private static Version mongoVersion;
	static final org.bson.Document GEO_SPHERE_2D = new org.bson.Document("loaction", "2dsphere");

	@Autowired MongoTemplate template;
	DefaultIndexOperations indexOps;
	MongoCollection<org.bson.Document> collection;

	@Before
	public void setUp() {

		queryMongoVersionIfNecessary();
		String collectionName = this.template.getCollectionName(DefaultIndexOperationsIntegrationTestsSample.class);

		this.collection = this.template.getDb().getCollection(collectionName, Document.class);
		this.collection.dropIndexes();
		this.indexOps = new DefaultIndexOperations(template.getMongoDbFactory(), collectionName,
				new QueryMapper(template.getConverter()));
	}

	private void queryMongoVersionIfNecessary() {

		if (mongoVersion == null) {
			Document result = template.executeCommand("{ buildInfo: 1 }");
			mongoVersion = Version.parse(result.get("version").toString());
		}
	}

	@Test // DATAMONGO-1008
	public void getIndexInfoShouldBeAbleToRead2dsphereIndex() {

		collection.createIndex(GEO_SPHERE_2D);

		IndexInfo info = findAndReturnIndexInfo(GEO_SPHERE_2D);
		assertThat(info.getIndexFields().get(0).isGeo()).isEqualTo(true);
	}

	@Test // DATAMONGO-1467
	public void shouldApplyPartialFilterCorrectly() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO), is(true));

		IndexDefinition id = new Index().named("partial-with-criteria").on("k3y", Direction.ASC)
				.partial(of(where("q-t-y").gte(10)));

		indexOps.ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-criteria");
		assertThat(info.getPartialFilterExpression()).isEqualTo("{ \"q-t-y\" : { \"$gte\" : 10 } }");
	}

	@Test // DATAMONGO-1467
	public void shouldApplyPartialFilterWithMappedPropertyCorrectly() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO), is(true));

		IndexDefinition id = new Index().named("partial-with-mapped-criteria").on("k3y", Direction.ASC)
				.partial(of(where("quantity").gte(10)));

		indexOps.ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-mapped-criteria");
		assertThat(info.getPartialFilterExpression()).isEqualTo("{ \"qty\" : { \"$gte\" : 10 } }");
	}

	@Test // DATAMONGO-1467
	public void shouldApplyPartialDBOFilterCorrectly() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO), is(true));

		IndexDefinition id = new Index().named("partial-with-dbo").on("k3y", Direction.ASC)
				.partial(of(new org.bson.Document("qty", new org.bson.Document("$gte", 10))));

		indexOps.ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-dbo");
		assertThat(info.getPartialFilterExpression()).isEqualTo("{ \"qty\" : { \"$gte\" : 10 } }");
	}

	@Test // DATAMONGO-1467
	public void shouldFavorExplicitMappingHintViaClass() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO), is(true));

		IndexDefinition id = new Index().named("partial-with-inheritance").on("k3y", Direction.ASC)
				.partial(of(where("age").gte(10)));

		indexOps = new DefaultIndexOperations(template.getMongoDbFactory(),
				this.template.getCollectionName(DefaultIndexOperationsIntegrationTestsSample.class),
				new QueryMapper(template.getConverter()), MappingToSameCollection.class);

		indexOps.ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-inheritance");
		assertThat(info.getPartialFilterExpression()).isEqualTo("{ \"a_g_e\" : { \"$gte\" : 10 } }");
	}

	@Test // DATAMONGO-1518
	public void shouldCreateIndexWithCollationCorrectly() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_FOUR), is(true));

		IndexDefinition id = new Index().named("with-collation").on("xyz", Direction.ASC)
				.collation(Collation.of("de_AT").caseFirst(ICUCaseFirst.off()));

		new DefaultIndexOperations(template.getMongoDbFactory(),
				this.template.getCollectionName(DefaultIndexOperationsIntegrationTestsSample.class),
				new QueryMapper(template.getConverter()), MappingToSameCollection.class);

		indexOps.ensureIndex(id);

		Document expected = new Document("locale", "de_AT") //
				.append("caseLevel", false) //
				.append("caseFirst", "off") //
				.append("strength", 3) //
				.append("numericOrdering", false) //
				.append("alternate", "non-ignorable") //
				.append("maxVariable", "punct") //
				.append("normalization", false) //
				.append("backwards", false);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "with-collation");

		assertThat(info.getCollation()).isPresent();

		// version is set by MongoDB server - we remove it to avoid errors when upgrading server version.
		Document result = info.getCollation().get();
		result.remove("version");

		assertThat(result).isEqualTo(expected);
	}

	private IndexInfo findAndReturnIndexInfo(org.bson.Document keys) {
		return findAndReturnIndexInfo(indexOps.getIndexInfo(), keys);
	}

	private static IndexInfo findAndReturnIndexInfo(Iterable<IndexInfo> candidates, org.bson.Document keys) {
		return findAndReturnIndexInfo(candidates, genIndexName(keys));
	}

	private static IndexInfo findAndReturnIndexInfo(Iterable<IndexInfo> candidates, String name) {

		for (IndexInfo info : candidates) {
			if (ObjectUtils.nullSafeEquals(name, info.getName())) {
				return info;
			}
		}
		throw new AssertionError(String.format("Index with %s was not found", name));
	}

	private static String genIndexName(Document keys) {

		StringBuilder name = new StringBuilder();

		for (String s : keys.keySet()) {

			if (name.length() > 0) {
				name.append('_');
			}

			name.append(s).append('_');
			Object val = keys.get(s);

			if (val instanceof Number || val instanceof String) {
				name.append(val.toString().replace(' ', '_'));
			}
		}

		return name.toString();
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = "default-index-operations-tests")
	static class DefaultIndexOperationsIntegrationTestsSample {

		@Field("qty") Integer quantity;
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = "default-index-operations-tests")
	static class MappingToSameCollection extends DefaultIndexOperationsIntegrationTestsSample {

		@Field("a_g_e") Integer age;
	}
}
