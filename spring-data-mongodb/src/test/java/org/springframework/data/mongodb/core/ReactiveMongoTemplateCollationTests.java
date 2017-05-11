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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import org.bson.Document;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.util.Version;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.reactivestreams.client.MongoClients;

/**
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class ReactiveMongoTemplateCollationTests {

	public static @ClassRule MongoVersionRule REQUIRES_AT_LEAST_3_4_0 = MongoVersionRule.atLeast(Version.parse("3.4.0"));
	public static final String COLLECTION_NAME = "collation-1";

	@Configuration
	static class Config extends AbstractReactiveMongoConfiguration {

		@Override
		public com.mongodb.reactivestreams.client.MongoClient mongoClient() {
			return MongoClients.create();
		}

		@Override
		protected String getDatabaseName() {
			return "collation-tests";
		}
	}

	@Autowired ReactiveMongoTemplate template;

	@Before
	public void setUp() {
		StepVerifier.create(template.dropCollection(COLLECTION_NAME)).verifyComplete();
	}

	@Test // DATAMONGO-1693
	public void createCollectionWithCollation() {

		StepVerifier.create(template.createCollection(COLLECTION_NAME, CollectionOptions.just(Collation.of("en_US")))) //
				.expectNextCount(1) //
				.verifyComplete();

		Mono<Document> collation = getCollationInfo(COLLECTION_NAME);
		StepVerifier.create(collation) //
				.consumeNextWith(document -> assertThat(document.get("locale")).isEqualTo("en_US")) //
				.verifyComplete();

	}

	private Mono<Document> getCollationInfo(String collectionName) {

		return getCollectionInfo(collectionName) //
				.map(it -> it.get("options", Document.class)) //
				.map(it -> it.get("collation", Document.class));
	}

	@SuppressWarnings("unchecked")
	private Mono<Document> getCollectionInfo(String collectionName) {

		return template.execute(db -> {

			return Flux
					.from(db.runCommand(new Document() //
							.append("listCollections", 1) //
							.append("filter", new Document("name", collectionName)))) //
					.map(it -> it.get("cursor", Document.class))
					.flatMapIterable(it -> (List<Document>) it.get("firstBatch", List.class));
		}).next();
	}

}
