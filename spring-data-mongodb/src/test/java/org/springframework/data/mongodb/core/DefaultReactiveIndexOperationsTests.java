/*
 * Copyright 2017-2019 the original author or authors.
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
import static org.junit.Assume.*;
import static org.springframework.data.mongodb.core.index.PartialIndexFilter.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import reactor.test.StepVerifier;

import java.util.function.Predicate;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Collation.CaseFirst;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.util.Version;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class DefaultReactiveIndexOperationsTests {

	@Configuration
	static class Config extends AbstractReactiveMongoConfiguration {

		@Override
		public MongoClient reactiveMongoClient() {
			return MongoTestUtils.reactiveClient();
		}

		@Override
		protected String getDatabaseName() {
			return "index-ops-tests";
		}
	}

	private static final Version THREE_DOT_TWO = new Version(3, 2);
	private static final Version THREE_DOT_FOUR = new Version(3, 4);
	private static Version mongoVersion;

	@Autowired ReactiveMongoTemplate template;

	MongoCollection<Document> collection;
	DefaultReactiveIndexOperations indexOps;

	@Before
	public void setUp() {

		queryMongoVersionIfNecessary();
		String collectionName = this.template.getCollectionName(DefaultIndexOperationsIntegrationTestsSample.class);

		this.collection = this.template.getMongoDatabase().getCollection(collectionName, Document.class);
		this.indexOps = new DefaultReactiveIndexOperations(template, collectionName,
				new QueryMapper(template.getConverter()));

		StepVerifier.create(this.collection.dropIndexes()).verifyComplete();
	}

	private void queryMongoVersionIfNecessary() {

		if (mongoVersion == null) {
			Document result = template.executeCommand("{ buildInfo: 1 }").block();
			mongoVersion = Version.parse(result.get("version").toString());
		}
	}

	@Test // DATAMONGO-1518
	public void shouldCreateIndexWithCollationCorrectly() {

		assumeTrue(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_FOUR));

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

		assumeTrue(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO));

		IndexDefinition id = new Index().named("partial-with-criteria").on("k3y", Direction.ASC)
				.partial(of(where("q-t-y").gte(10)));

		indexOps.ensureIndex(id).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		indexOps.getIndexInfo().filter(this.indexByName("partial-with-criteria")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {
					assertThat(Document.parse(indexInfo.getPartialFilterExpression())).isEqualTo(Document.parse("{ \"q-t-y\" : { \"$gte\" : 10 } }"));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1682, DATAMONGO-2198
	public void shouldApplyPartialFilterWithMappedPropertyCorrectly() {

		assumeTrue(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO));

		IndexDefinition id = new Index().named("partial-with-mapped-criteria").on("k3y", Direction.ASC)
				.partial(of(where("quantity").gte(10)));

		indexOps.ensureIndex(id).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		indexOps.getIndexInfo().filter(this.indexByName("partial-with-mapped-criteria")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {
					assertThat(Document.parse(indexInfo.getPartialFilterExpression())).isEqualTo(Document.parse("{ \"qty\" : { \"$gte\" : 10 } }"));
				}).verifyComplete();
	}

	@Test // DATAMONGO-1682, DATAMONGO-2198
	public void shouldApplyPartialDBOFilterCorrectly() {

		assumeTrue(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO));

		IndexDefinition id = new Index().named("partial-with-dbo").on("k3y", Direction.ASC)
				.partial(of(new org.bson.Document("qty", new org.bson.Document("$gte", 10))));

		indexOps.ensureIndex(id).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		indexOps.getIndexInfo().filter(this.indexByName("partial-with-dbo")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {
					assertThat(Document.parse(indexInfo.getPartialFilterExpression())).isEqualTo(Document.parse("{ \"qty\" : { \"$gte\" : 10 } }"));
				}) //
				.verifyComplete();

	}

	@Test // DATAMONGO-1682, DATAMONGO-2198
	public void shouldFavorExplicitMappingHintViaClass() {

		assumeTrue(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_TWO));

		IndexDefinition id = new Index().named("partial-with-inheritance").on("k3y", Direction.ASC)
				.partial(of(where("age").gte(10)));

		indexOps = new DefaultReactiveIndexOperations(template,
				this.template.getCollectionName(DefaultIndexOperationsIntegrationTestsSample.class),
				new QueryMapper(template.getConverter()), MappingToSameCollection.class);

		indexOps.ensureIndex(id).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		indexOps.getIndexInfo().filter(this.indexByName("partial-with-inheritance")).as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {
					assertThat(Document.parse(indexInfo.getPartialFilterExpression())).isEqualTo(Document.parse("{ \"a_g_e\" : { \"$gte\" : 10 } }"));
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
