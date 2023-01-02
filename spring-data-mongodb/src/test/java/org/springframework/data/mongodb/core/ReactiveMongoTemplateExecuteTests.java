/*
 * Copyright 2016-2023 the original author or authors.
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
import static org.assertj.core.data.Offset.offset;
import static org.junit.Assume.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.util.Version;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Integration test for {@link ReactiveMongoTemplate} execute methods.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class ReactiveMongoTemplateExecuteTests {

	private static final Version THREE = Version.parse("3.0");

	@Autowired SimpleReactiveMongoDatabaseFactory factory;
	@Autowired ReactiveMongoOperations operations;

	Version mongoVersion;

	@Before
	public void setUp() {

		Flux<Void> cleanup = operations.dropCollection("person") //
				.mergeWith(operations.dropCollection("execute_test")) //
				.mergeWith(operations.dropCollection("execute_test1")) //
				.mergeWith(operations.dropCollection("execute_test2"));

		cleanup.as(StepVerifier::create).verifyComplete();

		if (mongoVersion == null) {
			mongoVersion = operations.executeCommand("{ buildInfo: 1 }") //
					.map(it -> it.get("version").toString())//
					.map(Version::parse) //
					.block();
		}
	}

	@Test // DATAMONGO-1444
	public void executeCommandJsonCommandShouldReturnSingleResponse() {

		operations.executeCommand("{ buildInfo: 1 }").as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual).containsKey("version");
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void executeCommandDocumentCommandShouldReturnSingleResponse() {

		operations.executeCommand(new Document("buildInfo", 1)).as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual).containsKey("version");
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void executeCommandJsonCommandShouldReturnMultipleResponses() {

		assumeTrue(mongoVersion.isGreaterThan(THREE));

		operations.executeCommand("{ insert: 'execute_test', documents: [{},{},{}]}").as(StepVerifier::create)
				.expectNextCount(1).verifyComplete();

		operations.executeCommand("{ find: 'execute_test'}").as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.get("ok", Double.class)).isCloseTo(1D, offset(0D));
					assertThat(actual).containsKey("cursor");
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void executeCommandJsonCommandShouldTranslateExceptions() {

		operations.executeCommand("{ unknown: 1 }").as(StepVerifier::create) //
				.expectError(InvalidDataAccessApiUsageException.class) //
				.verify();
	}

	@Test // DATAMONGO-1444
	public void executeCommandDocumentCommandShouldTranslateExceptions() {

		operations.executeCommand(new Document("unknown", 1)).as(StepVerifier::create) //
				.expectError(InvalidDataAccessApiUsageException.class) //
				.verify();

	}

	@Test // DATAMONGO-1444
	public void executeCommandWithReadPreferenceCommandShouldTranslateExceptions() {

		operations.executeCommand(new Document("unknown", 1), ReadPreference.nearest()).as(StepVerifier::create) //
				.expectError(InvalidDataAccessApiUsageException.class) //
				.verify();
	}

	@Test // DATAMONGO-1444
	public void executeOnDatabaseShouldExecuteCommand() {

		Flux<Document> documentFlux = operations.executeCommand("{ insert: 'execute_test', documents: [{},{},{}]}")
				.mergeWith(operations.executeCommand("{ insert: 'execute_test1', documents: [{},{},{}]}"))
				.mergeWith(operations.executeCommand("{ insert: 'execute_test2', documents: [{},{},{}]}"));

		documentFlux.as(StepVerifier::create).expectNextCount(3).verifyComplete();

		Flux<Document> execute = operations.execute(MongoDatabase::listCollections);

		execute.filter(document -> document.getString("name").startsWith("execute_test")).as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void executeOnDatabaseShouldDeferExecution() {

		operations.execute(db -> {
			throw new MongoException(50, "hi there");
		});

		// the assertion here is that the exception is not thrown
	}

	@Test // DATAMONGO-1444
	public void executeOnDatabaseShouldShouldTranslateExceptions() {

		Flux<Document> execute = operations.execute(db -> {
			throw new MongoException(50, "hi there");
		});

		execute.as(StepVerifier::create).expectError(UncategorizedMongoDbException.class).verify();
	}

	@Test // DATAMONGO-1444
	public void executeOnCollectionWithTypeShouldReturnFindResults() {

		operations.executeCommand("{ insert: 'person', documents: [{},{},{}]}").as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		operations.execute(Person.class, MongoCollection::find).as(StepVerifier::create).expectNextCount(3)
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void executeOnCollectionWithNameShouldReturnFindResults() {

		operations.executeCommand("{ insert: 'execute_test', documents: [{},{},{}]}").as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		operations.execute("execute_test", MongoCollection::find).as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();
	}
}
