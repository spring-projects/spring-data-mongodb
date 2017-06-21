/*
 * Copyright 2017 the original author or authors.
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

import reactor.test.StepVerifier;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Collation.CaseFirst;
import org.springframework.data.mongodb.core.DefaultIndexOperationsIntegrationTests.DefaultIndexOperationsIntegrationTestsSample;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexDefinition;
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
		public MongoClient mongoClient() {
			return MongoClients.create();
		}

		@Override
		protected String getDatabaseName() {
			return "index-ops-tests";
		}
	}

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
		this.collection.dropIndexes();
		this.indexOps = new DefaultReactiveIndexOperations(template, collectionName);
	}

	private void queryMongoVersionIfNecessary() {

		if (mongoVersion == null) {
			Document result = template.executeCommand("{ buildInfo: 1 }").block();
			mongoVersion = Version.parse(result.get("version").toString());
		}
	}

	@Test // DATAMONGO-1518
	public void shouldCreateIndexWithCollationCorrectly() {

		assumeThat(mongoVersion.isGreaterThanOrEqualTo(THREE_DOT_FOUR), is(true));

		IndexDefinition id = new Index().named("with-collation").on("xyz", Direction.ASC)
				.collation(Collation.of("de_AT").caseFirst(CaseFirst.off()));

		indexOps.ensureIndex(id).subscribe();

		Document expected = new Document("locale", "de_AT") //
				.append("caseLevel", false) //
				.append("caseFirst", "off") //
				.append("strength", 3) //
				.append("numericOrdering", false) //
				.append("alternate", "non-ignorable") //
				.append("maxVariable", "punct") //
				.append("normalization", false) //
				.append("backwards", false);

		StepVerifier.create(indexOps.getIndexInfo().filter(val -> val.getName().equals("with-collation")))
				.consumeNextWith(indexInfo -> {

					assertThat(indexInfo.getCollation()).isPresent();

					// version is set by MongoDB server - we remove it to avoid errors when upgrading server version.
					Document result = indexInfo.getCollation().get();
					result.remove("version");

					assertThat(result).isEqualTo(expected);
				}) //
				.verifyComplete();
	}

}
