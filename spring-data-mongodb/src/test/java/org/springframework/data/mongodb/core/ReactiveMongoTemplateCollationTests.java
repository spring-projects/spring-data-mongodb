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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
@ContextConfiguration
public class ReactiveMongoTemplateCollationTests {

	public static final String COLLECTION_NAME = "collation-1";
	static @Client MongoClient mongoClient;

	@Configuration
	static class Config extends AbstractReactiveMongoConfiguration {

		@Override
		public MongoClient reactiveMongoClient() {
			return mongoClient;
		}

		@Override
		protected String getDatabaseName() {
			return "collation-tests";
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet()  {
			return Collections.emptySet();
		}
	}

	@Autowired ReactiveMongoTemplate template;

	@BeforeEach
	public void setUp() {
		template.dropCollection(COLLECTION_NAME).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1693
	public void createCollectionWithCollation() {

		template.createCollection(COLLECTION_NAME, CollectionOptions.just(Collation.of("en_US"))).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Mono<Document> collation = getCollationInfo(COLLECTION_NAME);
		collation.as(StepVerifier::create) //
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

			return Flux.from(db.runCommand(new Document() //
					.append("listCollections", 1) //
					.append("filter", new Document("name", collectionName)))) //
					.map(it -> it.get("cursor", Document.class))
					.flatMapIterable(it -> (List<Document>) it.get("firstBatch", List.class));
		}).next();
	}

}
