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
package org.springframework.data.mongodb.repository.support;

import static org.assertj.core.api.Assertions.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.repository.Address;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.QAddress;
import org.springframework.data.mongodb.repository.QPerson;
import org.springframework.data.mongodb.repository.QUser;
import org.springframework.data.mongodb.repository.User;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.mongodb.test.util.ReactiveMongoClientClosingTestConfiguration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.mongodb.MongoException;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Tests for {@link ReactiveQuerydslMongoPredicateExecutor}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Rocco Lagrotteria
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class ReactiveQuerydslMongoPredicateExecutorTests {

	@Autowired ReactiveMongoOperations operations;
	@Autowired ReactiveMongoDatabaseFactory dbFactory;

	ReactiveQuerydslMongoPredicateExecutor<Person> repository;

	Person dave, oliver, carter;
	QPerson person;

	@Configuration
	static class Config extends ReactiveMongoClientClosingTestConfiguration {

		@Override
		public MongoClient reactiveMongoClient() {
			return MongoTestUtils.reactiveClient();
		}

		@Override
		protected String getDatabaseName() {
			return "reactive";
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Collections.singleton(Person.class);
		}
	}

	@BeforeClass
	public static void cleanDb() {

		try (MongoClient client = MongoTestUtils.reactiveClient()) {
			MongoTestUtils.createOrReplaceCollectionNow("reactive", "person", client);
			MongoTestUtils.createOrReplaceCollectionNow("reactive", "user", client);
		}
	}

	@Before
	public void setup() {

		ReactiveMongoRepositoryFactory factory = new ReactiveMongoRepositoryFactory(operations);
		MongoEntityInformation<Person, String> entityInformation = factory.getEntityInformation(Person.class);
		repository = new ReactiveQuerydslMongoPredicateExecutor<>(entityInformation, operations);

		dave = new Person("Dave", "Matthews", 42);
		oliver = new Person("Oliver August", "Matthews", 4);
		carter = new Person("Carter", "Beauford", 49);

		person = new QPerson("person");

		Flux.merge(operations.insert(oliver), operations.insert(dave), operations.insert(carter)).then() //
				.as(StepVerifier::create).verifyComplete();
	}

	@After
	public void tearDown() {
		operations.remove(new BasicQuery("{}"), "person").then().as(StepVerifier::create).verifyComplete();
		operations.remove(new BasicQuery("{}"), "uer").then().as(StepVerifier::create).verifyComplete();
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
	public void shouldSupportCountWithPredicate() {

		repository.count(person.firstname.eq("Dave")) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		repository.count(person.firstname.eq("Unknown")) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
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

		Flux.merge(operations.save(user1), operations.save(user2), operations.save(user3)) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete(); //

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

		operations.save(person2) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

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

	@Test // DATAMONGO-2182, DATAMONGO-2265
	public void translatesExceptionsCorrectly() {

		ReactiveMongoOperations ops = new ReactiveMongoTemplate(dbFactory) {

			@Override
			protected Mono<MongoDatabase> doGetDatabase() {
				return Mono.error(new MongoException(18, "Authentication Failed"));
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

	@Test // GH-3757
	public void findByShouldReturnFirstResult() {

		repository.findBy(person.firstname.eq(oliver.getFirstname()), FluentQuery.ReactiveFluentQuery::first) //
				.as(StepVerifier::create) //
				.expectNext(oliver) //
				.verifyComplete();
	}

	@Test // GH-3757
	public void findByShouldReturnOneResult() {

		repository.findBy(person.firstname.eq(oliver.getFirstname()), FluentQuery.ReactiveFluentQuery::one) //
				.as(StepVerifier::create) //
				.expectNext(oliver) //
				.verifyComplete();

		repository.findBy(person.lastname.eq(oliver.getLastname()), FluentQuery.ReactiveFluentQuery::one) //
				.as(StepVerifier::create) //
				.verifyError(IncorrectResultSizeDataAccessException.class);
	}

	@Test // GH-3757
	public void findByShouldReturnAll() {

		repository.findBy(person.lastname.eq(oliver.getLastname()), FluentQuery.ReactiveFluentQuery::all) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // GH-3757
	public void findByShouldApplySortAll() {

		repository.findBy(person.lastname.eq(oliver.getLastname()), it -> it.sortBy(Sort.by("firstname")).all()) //
				.as(StepVerifier::create) //
				.expectNext(dave, oliver) //
				.verifyComplete();

		repository
				.findBy(person.lastname.eq(oliver.getLastname()), it -> it.sortBy(Sort.by(Direction.DESC, "firstname")).all()) //
				.as(StepVerifier::create) //
				.expectNext(oliver, dave) //
				.verifyComplete();
	}

	@Test // GH-3757
	public void findByShouldApplyProjection() {

		repository.findBy(person.lastname.eq(oliver.getLastname()), it -> it.project("firstname").first()) //
				.as(StepVerifier::create) //
				.assertNext(it -> {

					assertThat(it.getFirstname()).isNotNull();
					assertThat(it.getLastname()).isNull();
				}).verifyComplete();
	}

	@Test // GH-3757
	public void findByShouldApplyPagination() {

		repository
				.findBy(person.lastname.eq(oliver.getLastname()), it -> it.page(PageRequest.of(0, 1, Sort.by("firstname")))) //
				.as(StepVerifier::create) //
				.assertNext(it -> {

					assertThat(it.getTotalElements()).isEqualTo(2);
					assertThat(it.getContent()).containsOnly(dave);
				}).verifyComplete();

		repository
				.findBy(person.lastname.eq(oliver.getLastname()), it -> it.page(PageRequest.of(1, 1, Sort.by("firstname")))) //
				.as(StepVerifier::create) //
				.assertNext(it -> {

					assertThat(it.getTotalElements()).isEqualTo(2);
					assertThat(it.getContent()).containsOnly(oliver);
				}).verifyComplete();
	}

	@Test // GH-3757
	public void findByShouldCount() {

		repository.findBy(person.lastname.eq(oliver.getLastname()), FluentQuery.ReactiveFluentQuery::count) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		repository.findBy(person.lastname.eq("foo"), FluentQuery.ReactiveFluentQuery::count) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // GH-3757
	public void findByShouldReportExists() {

		repository.findBy(person.lastname.eq(oliver.getLastname()), FluentQuery.ReactiveFluentQuery::exists) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		repository.findBy(person.lastname.eq("foo"), FluentQuery.ReactiveFluentQuery::exists) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}
}
