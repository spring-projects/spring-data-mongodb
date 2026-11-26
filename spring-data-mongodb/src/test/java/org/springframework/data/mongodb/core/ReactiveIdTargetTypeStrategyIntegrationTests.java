/*
 * Copyright 2026-present the original author or authors.
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

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import reactor.test.StepVerifier;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.bulk.Bulk;
import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import org.springframework.data.mongodb.core.IdTargetTypeStrategyIntegrationTests.GeneratedId;
import org.springframework.data.mongodb.core.IdTargetTypeStrategyIntegrationTests.PlainId;
import org.springframework.data.mongodb.core.mapping.IdTargetTypeStrategy;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.ReactiveMongoTestTemplate;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Reactive integration tests for {@link IdTargetTypeStrategy}.
 *
 * @author Christoph Strobl
 */
class ReactiveIdTargetTypeStrategyIntegrationTests {

	static final String DB_NAME = "reactive-id-target-type-strategy-tests";
	static final String COLLECTION = "testcol";

	static @Client MongoClient client;

	ReactiveMongoTestTemplate template = new ReactiveMongoTestTemplate(cfg -> {

		cfg.configureDatabaseFactory(it -> {
			it.client(client);
			it.defaultDb(DB_NAME);
		});
		cfg.configureConversion(it -> it.customConverters(adapter -> {
			adapter.idTargetTypeStrategy(IdTargetTypeStrategy.IMPLICIT);
		}));
	});

	@AfterEach
	void cleanUp() {
		template.flush(COLLECTION).as(StepVerifier::create).verifyComplete();
		template.flush(PlainId.class, GeneratedId.class).as(StepVerifier::create).verifyComplete();
	}

	@Test // GH-4466
	void retainsHexStringIdOfPlainDocumentInQueries() {

		ObjectId objectId = ObjectId.get();
		template.insert(new Document("_id", objectId.toHexString()), COLLECTION).as(StepVerifier::create)
				.expectNextCount(1).verifyComplete();

		template.count(query(where("_id").is(objectId)), COLLECTION).as(StepVerifier::create).expectNext(0L)
				.verifyComplete();
		template.count(query(where("_id").is(objectId.toHexString())), COLLECTION).as(StepVerifier::create)
				.expectNext(1L).verifyComplete();
	}

	@Test // GH-4466
	void storesHexStringIdOfUnannotatedIdPropertyAsString() {

		PlainId source = new PlainId(ObjectId.get().toHexString());
		template.save(source).as(StepVerifier::create).expectNext(source).verifyComplete();

		template.execute(PlainId.class, collection -> collection.find().first()) //
				.map(it -> it.get("_id")) //
				.as(StepVerifier::create) //
				.expectNext(source.id()) //
				.verifyComplete();
		template.findById(source.id(), PlainId.class).as(StepVerifier::create).expectNext(source).verifyComplete();
	}

	@Test // GH-4466
	void storesGeneratedIdOfUnannotatedIdPropertyAsString() {

		template.insert(new GeneratedId()) //
				.flatMap(it -> template.findById(it.id, GeneratedId.class)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		template.save(new GeneratedId()) //
				.flatMap(it -> template.findById(it.id, GeneratedId.class)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		template.insertAll(java.util.List.of(new GeneratedId())) //
				.flatMap(it -> template.findById(it.id, GeneratedId.class)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // GH-4466
	void storesGeneratedIdsOfUnannotatedIdPropertyAsStringWithBulkOperations() {

		template.bulkOps(BulkMode.ORDERED, GeneratedId.class).insert(java.util.List.of(new GeneratedId())).execute()
				.as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.execute(GeneratedId.class, collection -> collection.find().first()) //
				.map(it -> it.get("_id")) //
				.as(StepVerifier::create) //
				.assertNext(it -> org.assertj.core.api.Assertions.assertThat(it).isInstanceOf(String.class)) //
				.verifyComplete();
	}

	@Test // GH-4466
	@EnableIfMongoServerVersion(isGreaterThanEqual = "8.0")
	void storesGeneratedIdOfUnannotatedIdPropertyAsStringWithBulkWrite() {

		template.bulkWrite(Bulk.builder().inCollection(GeneratedId.class, ops -> ops.insert(new GeneratedId())).build(),
				BulkWriteOptions.ordered()).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.execute(GeneratedId.class, collection -> collection.find().first()) //
				.map(it -> it.get("_id")) //
				.as(StepVerifier::create) //
				.assertNext(it -> org.assertj.core.api.Assertions.assertThat(it).isInstanceOf(String.class)) //
				.verifyComplete();
	}

}
