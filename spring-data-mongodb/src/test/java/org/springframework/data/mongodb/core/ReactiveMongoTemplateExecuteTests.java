/*
 * Copyright 2016-2018 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.util.Version;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Integration test for {@link ReactiveMongoTemplate} execute methods.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class ReactiveMongoTemplateExecuteTests {

	private static final Version THREE = Version.parse("3.0");

	@Rule public ExpectedException thrown = ExpectedException.none();

	@Autowired SimpleReactiveMongoDatabaseFactory factory;
	@Autowired ReactiveMongoOperations operations;

	Version mongoVersion;

	@Before
	public void setUp() {

		Flux<Void> cleanup = operations.dropCollection("person") //
				.mergeWith(operations.dropCollection("execute_test")) //
				.mergeWith(operations.dropCollection("execute_test1")) //
				.mergeWith(operations.dropCollection("execute_test2"));

		StepVerifier.create(cleanup).verifyComplete();

		if (mongoVersion == null) {
			mongoVersion = operations.executeCommand("{ buildInfo: 1 }") //
					.map(it -> it.get("version").toString())//
					.map(Version::parse) //
					.block();
		}
	}

	@Test // DATAMONGO-1444
	public void executeCommandJsonCommandShouldReturnSingleResponse() {

		StepVerifier.create(operations.executeCommand("{ buildInfo: 1 }")).consumeNextWith(actual -> {

			assertThat(actual, hasKey("version"));
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void executeCommandDocumentCommandShouldReturnSingleResponse() {

		StepVerifier.create(operations.executeCommand(new Document("buildInfo", 1))).consumeNextWith(actual -> {

			assertThat(actual, hasKey("version"));
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void executeCommandJsonCommandShouldReturnMultipleResponses() {

		assumeTrue(mongoVersion.isGreaterThan(THREE));

		StepVerifier.create(operations.executeCommand("{ insert: 'execute_test', documents: [{},{},{}]}"))
				.expectNextCount(1).verifyComplete();

		StepVerifier.create(operations.executeCommand("{ find: 'execute_test'}")) //
				.consumeNextWith(actual -> {

					assertThat(actual.get("ok", Double.class), is(closeTo(1D, 0D)));
					assertThat(actual, hasKey("cursor"));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void executeCommandJsonCommandShouldTranslateExceptions() {

		StepVerifier.create(operations.executeCommand("{ unknown: 1 }")) //
				.expectError(InvalidDataAccessApiUsageException.class) //
				.verify();
	}

	@Test // DATAMONGO-1444
	public void executeCommandDocumentCommandShouldTranslateExceptions() {

		StepVerifier.create(operations.executeCommand(new Document("unknown", 1))) //
				.expectError(InvalidDataAccessApiUsageException.class) //
				.verify();

	}

	@Test // DATAMONGO-1444
	public void executeCommandWithReadPreferenceCommandShouldTranslateExceptions() {

		StepVerifier.create(operations.executeCommand(new Document("unknown", 1), ReadPreference.nearest())) //
				.expectError(InvalidDataAccessApiUsageException.class) //
				.verify();
	}

	@Test // DATAMONGO-1444
	public void executeOnDatabaseShouldExecuteCommand() {

		Flux<Document> documentFlux = operations.executeCommand("{ insert: 'execute_test', documents: [{},{},{}]}")
				.mergeWith(operations.executeCommand("{ insert: 'execute_test1', documents: [{},{},{}]}"))
				.mergeWith(operations.executeCommand("{ insert: 'execute_test2', documents: [{},{},{}]}"));

		StepVerifier.create(documentFlux).expectNextCount(3).verifyComplete();

		Flux<Document> execute = operations.execute(MongoDatabase::listCollections);

		StepVerifier.create(execute.filter(document -> document.getString("name").startsWith("execute_test"))) //
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

		StepVerifier.create(execute).expectError(UncategorizedMongoDbException.class).verify();
	}

	@Test // DATAMONGO-1444
	public void executeOnCollectionWithTypeShouldReturnFindResults() {

		StepVerifier.create(operations.executeCommand("{ insert: 'person', documents: [{},{},{}]}")) //
				.expectNextCount(1) //
				.verifyComplete();

		StepVerifier.create(operations.execute(Person.class, MongoCollection::find)).expectNextCount(3).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void executeOnCollectionWithNameShouldReturnFindResults() {

		StepVerifier.create(operations.executeCommand("{ insert: 'execute_test', documents: [{},{},{}]}")) //
				.expectNextCount(1) //
				.verifyComplete();

		StepVerifier.create(operations.execute("execute_test", MongoCollection::find)) //
				.expectNextCount(3) //
				.verifyComplete();
	}
}
