/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.mongodb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.EnableIfReplicaSetAvailable;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Integration tests for reactive transaction management.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ExtendWith(MongoClientExtension.class)
@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
@EnableIfReplicaSetAvailable
@DisabledIfSystemProperty(named = "user.name", matches = "jenkins")
public class ReactiveTransactionIntegrationTests {

	private static final String DATABASE = "rxtx-test";

	static @Client MongoClient mongoClient;
	static GenericApplicationContext context;

	PersonService personService;
	ReactiveMongoOperations operations;

	@BeforeAll
	public static void init() {
		context = new AnnotationConfigApplicationContext(TestMongoConfig.class, PersonService.class);
	}

	@AfterAll
	public static void after() {
		context.close();
	}

	@BeforeEach
	public void setUp() {

		personService = context.getBean(PersonService.class);
		operations = context.getBean(ReactiveMongoOperations.class);

		try (MongoClient client = MongoTestUtils.reactiveClient()) {

			Flux.merge( //
					MongoTestUtils.createOrReplaceCollection(DATABASE, operations.getCollectionName(Person.class), client),
					MongoTestUtils.createOrReplaceCollection(DATABASE, operations.getCollectionName(EventLog.class), client) //
			).then().as(StepVerifier::create).verifyComplete();
		}
	}

	@Test // DATAMONGO-2265
	public void shouldRollbackAfterException() {

		personService.savePersonErrors(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.verifyError(RuntimeException.class);

		operations.count(new Query(), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethod() {

		personService.declarativeSavePersonErrors(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.verifyError(RuntimeException.class);

		operations.count(new Query(), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void commitShouldPersistTxEntries() {

		personService.savePerson(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		operations.count(new Query(), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethod() {

		personService.declarativeSavePerson(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		operations.count(new Query(), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void commitShouldPersistTxEntriesAcrossCollections() {

		personService.saveWithLogs(new Person(null, "Walter", "White")) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		operations.count(new Query(), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		operations.count(new Query(), EventLog.class) //
				.as(StepVerifier::create) //
				.expectNext(4L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void rollbackShouldAbortAcrossCollections() {

		personService.saveWithErrorLogs(new Person(null, "Walter", "White")) //
				.then() //
				.as(StepVerifier::create) //
				.verifyError();

		operations.count(new Query(), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();

		operations.count(new Query(), EventLog.class) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void countShouldWorkInsideTransaction() {

		personService.countDuringTx(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void emitMultipleElementsDuringTransaction() {

		personService.saveWithLogs(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.expectNextCount(4L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void errorAfterTxShouldNotAffectPreviousStep() {

		personService.savePerson(new Person(null, "Walter", "White")) //
				.delayElement(Duration.ofMillis(10)) //
				.then(Mono.error(new RuntimeException("my big bad evil error"))).as(StepVerifier::create) //
				.expectError() //
				.verify();

		operations.count(new Query(), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Configuration
	static class TestMongoConfig extends AbstractReactiveMongoConfiguration {

		@Override
		public MongoClient reactiveMongoClient() {
			return mongoClient;
		}

		@Override
		protected String getDatabaseName() {
			return DATABASE;
		}

		@Bean
		public ReactiveMongoTransactionManager transactionManager(ReactiveMongoDatabaseFactory factory) {
			return new ReactiveMongoTransactionManager(factory);
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Collections.singleton(Person.class);
		}
	}

	@RequiredArgsConstructor
	static class PersonService {

		final ReactiveMongoOperations operations;
		final ReactiveMongoTransactionManager manager;

		public Mono<Person> savePersonErrors(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(manager,
					new DefaultTransactionDefinition());

			return operations.save(person) //
					.<Person> flatMap(it -> Mono.error(new RuntimeException("poof"))) //
					.as(transactionalOperator::transactional);
		}

		public Mono<Person> savePerson(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(manager,
					new DefaultTransactionDefinition());

			return operations.save(person) //
					.flatMap(Mono::just) //
					.as(transactionalOperator::transactional);
		}

		public Mono<Long> countDuringTx(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(manager,
					new DefaultTransactionDefinition());

			return operations.save(person) //
					.then(operations.count(new Query(), Person.class)) //
					.as(transactionalOperator::transactional);
		}

		public Flux<EventLog> saveWithLogs(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(manager,
					new DefaultTransactionDefinition());

			return Flux.merge(operations.save(new EventLog(new ObjectId(), "beforeConvert")), //
					operations.save(new EventLog(new ObjectId(), "afterConvert")), //
					operations.save(new EventLog(new ObjectId(), "beforeInsert")), //
					operations.save(person), //
					operations.save(new EventLog(new ObjectId(), "afterInsert"))) //
					.thenMany(operations.query(EventLog.class).all()) //
					.as(transactionalOperator::transactional);
		}

		public Flux<Void> saveWithErrorLogs(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(manager,
					new DefaultTransactionDefinition());

			return Flux.merge(operations.save(new EventLog(new ObjectId(), "beforeConvert")), //
					operations.save(new EventLog(new ObjectId(), "afterConvert")), //
					operations.save(new EventLog(new ObjectId(), "beforeInsert")), //
					operations.save(person), //
					operations.save(new EventLog(new ObjectId(), "afterInsert"))) //
					.<Void> flatMap(it -> Mono.error(new RuntimeException("poof"))) //
					.as(transactionalOperator::transactional);
		}

		@Transactional
		public Flux<Person> declarativeSavePerson(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(manager,
					new DefaultTransactionDefinition());

			return transactionalOperator.execute(reactiveTransaction -> {
				return operations.save(person);
			});
		}

		@Transactional
		public Flux<Person> declarativeSavePersonErrors(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(manager,
					new DefaultTransactionDefinition());

			return transactionalOperator.execute(reactiveTransaction -> {

				return operations.save(person) //
						.<Person> flatMap(it -> Mono.error(new RuntimeException("poof")));
			});
		}
	}

	@Data
	@AllArgsConstructor
	@Document("person-rx")
	static class Person {

		ObjectId id;
		String firstname, lastname;
	}

	@Data
	@AllArgsConstructor
	static class EventLog {

		ObjectId id;
		String action;
	}
}
