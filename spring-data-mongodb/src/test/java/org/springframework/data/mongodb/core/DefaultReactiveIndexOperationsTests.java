/*
 * Copyright 2017-2023 the original author or authors.
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

import reactor.test.StepVerifier;

import java.util.function.Predicate;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Collation.CaseFirst;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.ReactiveMongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

import com.mongodb.client.model.IndexOptions;
import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Mathieu Ouellet
 */
@ExtendWith(MongoTemplateExtension.class)
public class DefaultReactiveIndexOperationsTests {

	@Template(initialEntitySet = DefaultIndexOperationsIntegrationTestsSample.class) //
	static ReactiveMongoTestTemplate template;

	String collectionName = template.getCollectionName(DefaultIndexOperationsIntegrationTestsSample.class);

	DefaultReactiveIndexOperations indexOps = new DefaultReactiveIndexOperations(template, collectionName,
			new QueryMapper(template.getConverter()));

	@BeforeEach
	public void setUp() {
		template.getCollection(collectionName).flatMapMany(MongoCollection::dropIndexes) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1518
	public void shouldCreateIndexWithCollationCorrectly() {

		IndexDefinition id = new Index().named("with-collation").on("xyz", Direction.ASC)
				.collation(Collation.of("de_AT").caseFirst(CaseFirst.off()));

		indexOps.ensureIndex(id).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		Document expected = new Document("locale", "de_AT") //
				.append("caseLevel", false) //
				.append("caseFirst", "off") //
				.append("strength", 3) //
				.append("numericOrdering", false) //
				.append("alternate", "non-ignorable") //
				.append("maxVariable", "punct") //
				.append("normalization", false) //
				.append("backwards", false);

		indexOps.getIndexInfo().filter(this.indexByName("with-collation")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {

					assertThat(indexInfo.getCollation()).isPresent();

					// version is set by MongoDB server - we remove it to avoid errors when upgrading server version.
					Document result = indexInfo.getCollation().get();
					result.remove("version");

					assertThat(result).isEqualTo(expected);
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1682, DATAMONGO-2198
	public void shouldApplyPartialFilterCorrectly() {

		IndexDefinition id = new Index().named("partial-with-criteria").on("k3y", Direction.ASC)
				.partial(of(where("q-t-y").gte(10)));

		indexOps.ensureIndex(id).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		indexOps.getIndexInfo().filter(this.indexByName("partial-with-criteria")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {
					assertThat(Document.parse(indexInfo.getPartialFilterExpression()))
							.isEqualTo(Document.parse("{ \"q-t-y\" : { \"$gte\" : 10 } }"));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1682, DATAMONGO-2198
	public void shouldApplyPartialFilterWithMappedPropertyCorrectly() {

		IndexDefinition id = new Index().named("partial-with-mapped-criteria").on("k3y", Direction.ASC)
				.partial(of(where("quantity").gte(10)));

		indexOps.ensureIndex(id).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		indexOps.getIndexInfo().filter(this.indexByName("partial-with-mapped-criteria")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {
					assertThat(Document.parse(indexInfo.getPartialFilterExpression()))
							.isEqualTo(Document.parse("{ \"qty\" : { \"$gte\" : 10 } }"));
				}).verifyComplete();
	}

	@Test // DATAMONGO-1682, DATAMONGO-2198
	public void shouldApplyPartialDBOFilterCorrectly() {

		IndexDefinition id = new Index().named("partial-with-dbo").on("k3y", Direction.ASC)
				.partial(of(new org.bson.Document("qty", new org.bson.Document("$gte", 10))));

		indexOps.ensureIndex(id).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		indexOps.getIndexInfo().filter(this.indexByName("partial-with-dbo")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {
					assertThat(Document.parse(indexInfo.getPartialFilterExpression()))
							.isEqualTo(Document.parse("{ \"qty\" : { \"$gte\" : 10 } }"));
				}) //
				.verifyComplete();

	}

	@Test // DATAMONGO-1682, DATAMONGO-2198
	public void shouldFavorExplicitMappingHintViaClass() {

		IndexDefinition id = new Index().named("partial-with-inheritance").on("k3y", Direction.ASC)
				.partial(of(where("age").gte(10)));

		indexOps = new DefaultReactiveIndexOperations(template,
				this.template.getCollectionName(DefaultIndexOperationsIntegrationTestsSample.class),
				new QueryMapper(template.getConverter()), MappingToSameCollection.class);

		indexOps.ensureIndex(id).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		indexOps.getIndexInfo().filter(this.indexByName("partial-with-inheritance")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {
					assertThat(Document.parse(indexInfo.getPartialFilterExpression()))
							.isEqualTo(Document.parse("{ \"a_g_e\" : { \"$gte\" : 10 } }"));
				}) //
				.verifyComplete();
	}

	@Test // GH-4348
	void indexShouldNotBeHiddenByDefault() {

		IndexDefinition index = new Index().named("my-index").on("a", Direction.ASC);

		indexOps.ensureIndex(index).then().as(StepVerifier::create).verifyComplete();

		indexOps.getIndexInfo().filter(this.indexByName("my-index")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {
					assertThat(indexInfo.isHidden()).isFalse();
				}) //
				.verifyComplete();
	}

	@Test // GH-4348
	void shouldCreateHiddenIndex() {

		IndexDefinition index = new Index().named("my-hidden-index").on("a", Direction.ASC).hidden();

		indexOps.ensureIndex(index).then().as(StepVerifier::create).verifyComplete();

		indexOps.getIndexInfo().filter(this.indexByName("my-hidden-index")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {
					assertThat(indexInfo.isHidden()).isTrue();
				}) //
				.verifyComplete();
	}

	@Test // GH-4348
	void alterIndexShouldAllowHiding() {

		template.execute(collectionName, collection -> {
			return collection.createIndex(new Document("a", 1), new IndexOptions().name("my-index"));
		}).then().as(StepVerifier::create).verifyComplete();

		indexOps.alterIndex("my-index", org.springframework.data.mongodb.core.index.IndexOptions.hidden())
				.as(StepVerifier::create).verifyComplete();
		indexOps.getIndexInfo().filter(this.indexByName("my-index")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {
					assertThat(indexInfo.isHidden()).isTrue();
				}) //
				.verifyComplete();
	}

	Predicate<IndexInfo> indexByName(String name) {
		return indexInfo -> indexInfo.getName().equals(name);
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = "default-index-operations-tests")
	static class DefaultIndexOperationsIntegrationTestsSample {

		@Field("qty") Integer quantity;
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = "default-index-operations-tests")
	static class MappingToSameCollection
			extends DefaultIndexOperationsIntegrationTests.DefaultIndexOperationsIntegrationTestsSample {

		@Field("a_g_e") Integer age;
	}

}
