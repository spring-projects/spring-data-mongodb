/*
 * Copyright 2014-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.index.PartialIndexFilter.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Collation.CaseFirst;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;
import org.springframework.util.ObjectUtils;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;

/**
 * Integration tests for {@link DefaultIndexOperations}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 */
@ExtendWith(MongoTemplateExtension.class)
public class DefaultIndexOperationsIntegrationTests {

	static final String COLLECTION_NAME = "default-index-operations-tests";
	static final org.bson.Document GEO_SPHERE_2D = new org.bson.Document("loaction", "2dsphere");

	@Template //
	static MongoTestTemplate template;

	MongoCollection<org.bson.Document> collection = template.getCollection(COLLECTION_NAME);
	IndexOperations indexOps = template.indexOps(COLLECTION_NAME);

	@BeforeEach
	public void setUp() {
		template.dropIndexes(COLLECTION_NAME);
	}

	@Test // DATAMONGO-1008
	public void getIndexInfoShouldBeAbleToRead2dsphereIndex() {

		template.getCollection(COLLECTION_NAME).createIndex(GEO_SPHERE_2D);

		IndexInfo info = findAndReturnIndexInfo(GEO_SPHERE_2D);
		assertThat(info.getIndexFields().get(0).isGeo()).isEqualTo(true);
	}

	@Test // DATAMONGO-1467, DATAMONGO-2198
	public void shouldApplyPartialFilterCorrectly() {

		IndexDefinition id = new Index().named("partial-with-criteria").on("k3y", Direction.ASC)
				.partial(of(where("q-t-y").gte(10)));

		indexOps.ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-criteria");
		assertThat(Document.parse(info.getPartialFilterExpression()))
				.isEqualTo(Document.parse("{ \"q-t-y\" : { \"$gte\" : 10 } }"));
	}

	@Test // DATAMONGO-1467, DATAMONGO-2198
	public void shouldApplyPartialFilterWithMappedPropertyCorrectly() {

		IndexDefinition id = new Index().named("partial-with-mapped-criteria").on("k3y", Direction.ASC)
				.partial(of(where("quantity").gte(10)));

		template.indexOps(DefaultIndexOperationsIntegrationTestsSample.class).ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-mapped-criteria");
		assertThat(Document.parse(info.getPartialFilterExpression()))
				.isEqualTo(Document.parse("{ \"qty\" : { \"$gte\" : 10 } }"));
	}

	@Test // DATAMONGO-1467, DATAMONGO-2198
	public void shouldApplyPartialDBOFilterCorrectly() {

		IndexDefinition id = new Index().named("partial-with-dbo").on("k3y", Direction.ASC)
				.partial(of(new org.bson.Document("qty", new org.bson.Document("$gte", 10))));

		indexOps.ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-dbo");
		assertThat(Document.parse(info.getPartialFilterExpression()))
				.isEqualTo(Document.parse("{ \"qty\" : { \"$gte\" : 10 } }"));
	}

	@Test // DATAMONGO-1467, DATAMONGO-2198
	public void shouldFavorExplicitMappingHintViaClass() {

		IndexDefinition id = new Index().named("partial-with-inheritance").on("k3y", Direction.ASC)
				.partial(of(where("age").gte(10)));

		indexOps = new DefaultIndexOperations(template, COLLECTION_NAME, MappingToSameCollection.class);

		indexOps.ensureIndex(id);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-inheritance");
		assertThat(Document.parse(info.getPartialFilterExpression()))
				.isEqualTo(Document.parse("{ \"a_g_e\" : { \"$gte\" : 10 } }"));
	}

	@Test // DATAMONGO-2388
	public void shouldReadIndexWithPartialFilterContainingDbRefCorrectly() {

		BsonDocument partialFilter = BsonDocument.parse(
				"{ \"the-ref\" : { \"$ref\" : \"other-collection\", \"$id\" : { \"$oid\" : \"59ce08baf264b906810fe8c5\"} } }");
		IndexOptions indexOptions = new IndexOptions();
		indexOptions.name("partial-with-dbref");
		indexOptions.partialFilterExpression(partialFilter);

		collection.createIndex(BsonDocument.parse("{ \"key-1\" : 1, \"key-2\": 1}"), indexOptions);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "partial-with-dbref");
		assertThat(BsonDocument.parse(info.getPartialFilterExpression())).isEqualTo(partialFilter);
	}

	@Test // DATAMONGO-1518
	public void shouldCreateIndexWithCollationCorrectly() {

		IndexDefinition id = new Index().named("with-collation").on("xyz", Direction.ASC)
				.collation(Collation.of("de_AT").caseFirst(CaseFirst.off()));

		new DefaultIndexOperations(template, COLLECTION_NAME, MappingToSameCollection.class);

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

	@Test // GH-4348
	void indexShouldNotBeHiddenByDefault() {

		IndexDefinition index = new Index().named("my-index").on("a", Direction.ASC);

		indexOps = new DefaultIndexOperations(template, COLLECTION_NAME, MappingToSameCollection.class);
		indexOps.ensureIndex(index);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "my-index");
		assertThat(info.isHidden()).isFalse();
	}

	@Test // GH-4348
	void shouldCreateHiddenIndex() {

		IndexDefinition index = new Index().named("my-hidden-index").on("a", Direction.ASC).hidden();

		indexOps = new DefaultIndexOperations(template, COLLECTION_NAME, MappingToSameCollection.class);
		indexOps.ensureIndex(index);

		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "my-hidden-index");
		assertThat(info.isHidden()).isTrue();
	}

	@Test // GH-4348
	void alterIndexShouldAllowHiding() {

		collection.createIndex(new Document("a", 1), new IndexOptions().name("my-index"));

		indexOps.alterIndex("my-index", org.springframework.data.mongodb.core.index.IndexOptions.hidden());
		IndexInfo info = findAndReturnIndexInfo(indexOps.getIndexInfo(), "my-index");
		assertThat(info.isHidden()).isTrue();
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
