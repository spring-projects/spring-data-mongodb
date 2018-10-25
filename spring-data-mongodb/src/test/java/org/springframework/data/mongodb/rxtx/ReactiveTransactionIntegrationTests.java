/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.rxtx;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.bson.types.ObjectId;
import org.junit.Test;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * Integration tests for reactive transaction management.
 *
 * @author Mark Paluch
 */
public class ReactiveTransactionIntegrationTests {

	@Configuration
	static class TestMongoConfig extends AbstractReactiveMongoConfiguration {

		@Override
		public MongoClient reactiveMongoClient() {
			return MongoClients.create("mongodb://localhost");
		}

		@Override
		protected String getDatabaseName() {
			return "test";
		}

		@Bean
		public ReactiveMongoTransactionManager transactionManager(ReactiveMongoDatabaseFactory factory) {
			return new ReactiveMongoTransactionManager(factory);
		}
	}

	@Service
	@RequiredArgsConstructor
	static class PersonService {

		final ReactiveMongoOperations operations;
		final ReactiveMongoTransactionManager manager;

		public Mono<Person> savePerson(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(manager,
					new DefaultTransactionDefinition());

			return operations.save(person).<Person> flatMap(it -> {
				return Mono.error(new RuntimeException("poof!"));
			}).as(transactionalOperator::transactional);
		}
	}

	@Test
	public void shouldRollbackAfterException() {

		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestMongoConfig.class,
				PersonService.class);

		ReactiveMongoOperations operations = context.getBean(ReactiveMongoOperations.class);

		MongoTemplate template = new MongoTemplate(new com.mongodb.MongoClient("localhost"), "test");

		template.dropCollection(Person.class);
		template.dropCollection(EventLog.class);

		template.createCollection(Person.class);
		template.createCollection(EventLog.class);

		PersonService personService = context.getBean(PersonService.class);

		personService.savePerson(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.verifyError(RuntimeException.class);

		operations.count(new Query(), Person.class).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@Data
	@AllArgsConstructor
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
