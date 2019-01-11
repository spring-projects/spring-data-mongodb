/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.LinkedHashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.repository.Address;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.QAddress;
import org.springframework.data.mongodb.repository.QPerson;
import org.springframework.data.mongodb.repository.QUser;
import org.springframework.data.mongodb.repository.User;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.mongodb.MongoException;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Tests for {@link ReactiveQuerydslMongoPredicateExecutor}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class ReactiveQuerydslMongoPredicateExecutorIntegrationTests {

	@Autowired ReactiveMongoOperations operations;
	@Autowired ReactiveMongoDatabaseFactory dbFactory;

	ReactiveQuerydslMongoPredicateExecutor<Person> repository;

	Person dave, oliver, carter;
	QPerson person;

	@Before
	public void setup() {

		ReactiveMongoRepositoryFactory factory = new ReactiveMongoRepositoryFactory(operations);
		MongoEntityInformation<Person, String> entityInformation = factory.getEntityInformation(Person.class);
		repository = new ReactiveQuerydslMongoPredicateExecutor<>(entityInformation, operations);

		operations.dropCollection(Person.class) //
				.as(StepVerifier::create) //
				.verifyComplete();

		dave = new Person("Dave", "Matthews", 42);
		oliver = new Person("Oliver August", "Matthews", 4);
		carter = new Person("Carter", "Beauford", 49);

		person = new QPerson("person");

		operations.insertAll(Arrays.asList(oliver, dave, carter)).as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2182
	public void shouldSupportExistsWithPredicate() {

		repository.exists(person.firstname.eq("Dave")) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		repository.exists(person.firstname.eq("Unknown")) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2182
	public void shouldSupportFindAllWithPredicateAndSort() {

		repository.findAll(person.lastname.isNotNull(), Sort.by(Direction.ASC, "firstname")) //
				.as(StepVerifier::create) //
				.expectNext(carter, dave, oliver) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2182
	public void findOneWithPredicateReturnsResultCorrectly() {

		repository.findOne(person.firstname.eq(dave.getFirstname())) //
				.as(StepVerifier::create) //
				.expectNext(dave) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2182
	public void findOneWithPredicateReturnsEmptyWhenNoDataFound() {

		repository.findOne(person.firstname.eq("batman")) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2182
	public void findOneWithPredicateThrowsExceptionForNonUniqueResults() {

		repository.findOne(person.firstname.contains("e")) //
				.as(StepVerifier::create) //
				.expectError(IncorrectResultSizeDataAccessException.class) //
				.verify();
	}

	@Test // DATAMONGO-2182
	public void findUsingAndShouldWork() {

		repository
				.findAll(person.lastname.startsWith(oliver.getLastname()).and(person.firstname.startsWith(dave.getFirstname()))) //
				.as(StepVerifier::create) //
				.expectNext(dave) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2182
	public void queryShouldTerminateWithUnsupportedOperationWithJoinOnDBref() {

		User user1 = new User();
		user1.setUsername("user-1");

		User user2 = new User();
		user2.setUsername("user-2");

		User user3 = new User();
		user3.setUsername("user-3");

		operations.insertAll(Arrays.asList(user1, user2, user3)) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		Person person1 = new Person("Max", "The Mighty");
		person1.setCoworker(user1);

		Person person2 = new Person("Jack", "The Ripper");
		person2.setCoworker(user2);

		Person person3 = new Person("Bob", "The Builder");
		person3.setCoworker(user3);

		operations.save(person1) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		operations.save(person2)//
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		operations.save(person3) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Flux<Person> result = new ReactiveSpringDataMongodbQuery<>(operations, Person.class).where()
				.join(person.coworker, QUser.user).on(QUser.user.username.eq("user-2")).fetch();

		result.as(StepVerifier::create) //
				.expectError(UnsupportedOperationException.class) //
				.verify();
	}

	@Test // DATAMONGO-2182
	public void queryShouldTerminateWithUnsupportedOperationOnJoinWithNoResults() {

		User user1 = new User();
		user1.setUsername("user-1");

		User user2 = new User();
		user2.setUsername("user-2");

		operations.insertAll(Arrays.asList(user1, user2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		Person person1 = new Person("Max", "The Mighty");
		person1.setCoworker(user1);

		Person person2 = new Person("Jack", "The Ripper");
		person2.setCoworker(user2);

		operations.save(person1) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		;
		operations.save(person2) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		;

		Flux<Person> result = new ReactiveSpringDataMongodbQuery<>(operations, Person.class).where()
				.join(person.coworker, QUser.user).on(QUser.user.username.eq("does-not-exist")).fetch();

		result.as(StepVerifier::create) //
				.expectError(UnsupportedOperationException.class) //
				.verify();
	}

	@Test // DATAMONGO-2182
	public void springDataMongodbQueryShouldAllowElemMatchOnArrays() {

		Address adr1 = new Address("Hauptplatz", "4020", "Linz");
		Address adr2 = new Address("Stephansplatz", "1010", "Wien");
		Address adr3 = new Address("Tower of London", "EC3N 4AB", "London");

		Person person1 = new Person("Max", "The Mighty");
		person1.setShippingAddresses(new LinkedHashSet<>(Arrays.asList(adr1, adr2)));

		Person person2 = new Person("Jack", "The Ripper");
		person2.setShippingAddresses(new LinkedHashSet<>(Arrays.asList(adr2, adr3)));

		operations.insertAll(Arrays.asList(person1, person2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		Flux<Person> result = new ReactiveSpringDataMongodbQuery<>(operations, Person.class).where()
				.anyEmbedded(person.shippingAddresses, QAddress.address).on(QAddress.address.city.eq("London")).fetch();

		result.as(StepVerifier::create) //
				.expectNext(person2) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2182
	public void translatesExceptionsCorrectly() {

		ReactiveMongoOperations ops = new ReactiveMongoTemplate(dbFactory) {

			@Override
			protected MongoDatabase doGetDatabase() {
				throw new MongoException(18, "Authentication Failed");
			}
		};

		ReactiveMongoRepositoryFactory factory = new ReactiveMongoRepositoryFactory(ops);
		MongoEntityInformation<Person, String> entityInformation = factory.getEntityInformation(Person.class);
		repository = new ReactiveQuerydslMongoPredicateExecutor<>(entityInformation, ops);

		repository.findOne(person.firstname.contains("batman")) //
				.as(StepVerifier::create) //
				.expectError(PermissionDeniedDataAccessException.class) //
				.verify();
	}
}
