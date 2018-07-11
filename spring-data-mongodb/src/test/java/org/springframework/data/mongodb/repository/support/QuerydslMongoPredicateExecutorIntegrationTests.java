/*
 * Copyright 2015-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.Address;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.QAddress;
import org.springframework.data.mongodb.repository.QPerson;
import org.springframework.data.mongodb.repository.QUser;
import org.springframework.data.mongodb.repository.User;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;

/**
 * Integration test for {@link QuerydslMongoPredicateExecutor}.
 *
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ContextConfiguration(
		locations = "/org/springframework/data/mongodb/repository/PersonRepositoryIntegrationTests-context.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class QuerydslMongoPredicateExecutorIntegrationTests {

	@Autowired MongoOperations operations;
	@Autowired MongoDbFactory dbFactory;

	QuerydslMongoPredicateExecutor<Person> repository;

	Person dave, oliver, carter;
	QPerson person;

	@Before
	public void setup() {

		MongoRepositoryFactory factory = new MongoRepositoryFactory(operations);
		MongoEntityInformation<Person, String> entityInformation = factory.getEntityInformation(Person.class);
		repository = new QuerydslMongoPredicateExecutor<>(entityInformation, operations);

		operations.dropCollection(Person.class);

		dave = new Person("Dave", "Matthews", 42);
		oliver = new Person("Oliver August", "Matthews", 4);
		carter = new Person("Carter", "Beauford", 49);

		person = new QPerson("person");

		operations.insertAll(Arrays.asList(oliver, dave, carter));
	}

	@Test // DATAMONGO-1146
	public void shouldSupportExistsWithPredicate() throws Exception {

		assertThat(repository.exists(person.firstname.eq("Dave"))).isTrue();
		assertThat(repository.exists(person.firstname.eq("Unknown"))).isFalse();
	}

	@Test // DATAMONGO-1167
	public void shouldSupportFindAllWithPredicateAndSort() {

		List<Person> users = repository.findAll(person.lastname.isNotNull(), Sort.by(Direction.ASC, "firstname"));

		assertThat(users).containsExactly(carter, dave, oliver);
	}

	@Test // DATAMONGO-1690
	public void findOneWithPredicateReturnsResultCorrectly() {
		assertThat(repository.findOne(person.firstname.eq(dave.getFirstname()))).contains(dave);
	}

	@Test // DATAMONGO-1690
	public void findOneWithPredicateReturnsOptionalEmptyWhenNoDataFound() {
		assertThat(repository.findOne(person.firstname.eq("batman"))).isNotPresent();
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class) // DATAMONGO-1690
	public void findOneWithPredicateThrowsExceptionForNonUniqueResults() {
		repository.findOne(person.firstname.contains("e"));
	}

	@Test // DATAMONGO-1848
	public void findUsingAndShouldWork() {

		assertThat(repository.findAll(
				person.lastname.startsWith(oliver.getLastname()).and(person.firstname.startsWith(dave.getFirstname()))))
						.containsExactly(dave);
	}

	@Test // DATAMONGO-362, DATAMONGO-1848
	public void springDataMongodbQueryShouldAllowJoinOnDBref() {

		User user1 = new User();
		user1.setUsername("user-1");

		User user2 = new User();
		user2.setUsername("user-2");

		User user3 = new User();
		user3.setUsername("user-3");

		operations.save(user1);
		operations.save(user2);
		operations.save(user3);

		Person person1 = new Person("Max", "The Mighty");
		person1.setCoworker(user1);

		Person person2 = new Person("Jack", "The Ripper");
		person2.setCoworker(user2);

		Person person3 = new Person("Bob", "The Builder");
		person3.setCoworker(user3);

		operations.save(person1);
		operations.save(person2);
		operations.save(person3);

		List<Person> result = new SpringDataMongodbQuery<>(operations, Person.class).where()
				.join(person.coworker, QUser.user).on(QUser.user.username.eq("user-2")).fetch();

		assertThat(result).containsExactly(person2);
	}

	@Test // DATAMONGO-362, DATAMONGO-1848
	public void springDataMongodbQueryShouldReturnEmptyOnJoinWithNoResults() {

		User user1 = new User();
		user1.setUsername("user-1");

		User user2 = new User();
		user2.setUsername("user-2");

		operations.save(user1);
		operations.save(user2);

		Person person1 = new Person("Max", "The Mighty");
		person1.setCoworker(user1);

		Person person2 = new Person("Jack", "The Ripper");
		person2.setCoworker(user2);

		operations.save(person1);
		operations.save(person2);

		List<Person> result = new SpringDataMongodbQuery<>(operations, Person.class).where()
				.join(person.coworker, QUser.user).on(QUser.user.username.eq("does-not-exist")).fetch();

		assertThat(result).isEmpty();
	}

	@Test // DATAMONGO-595, DATAMONGO-1848
	public void springDataMongodbQueryShouldAllowElemMatchOnArrays() {

		Address adr1 = new Address("Hauptplatz", "4020", "Linz");
		Address adr2 = new Address("Stephansplatz", "1010", "Wien");
		Address adr3 = new Address("Tower of London", "EC3N 4AB", "London");

		Person person1 = new Person("Max", "The Mighty");
		person1.setShippingAddresses(new LinkedHashSet<>(Arrays.asList(adr1, adr2)));

		Person person2 = new Person("Jack", "The Ripper");
		person2.setShippingAddresses(new LinkedHashSet<>(Arrays.asList(adr2, adr3)));

		operations.save(person1);
		operations.save(person2);

		List<Person> result = new SpringDataMongodbQuery<>(operations, Person.class).where()
				.anyEmbedded(person.shippingAddresses, QAddress.address).on(QAddress.address.city.eq("London")).fetch();

		assertThat(result).containsExactly(person2);
	}

	@Test(expected = PermissionDeniedDataAccessException.class) // DATAMONGO-1434, DATAMONGO-1848
	public void translatesExceptionsCorrectly() {

		MongoOperations ops = new MongoTemplate(dbFactory) {

			@Override
			protected MongoDatabase doGetDatabase() {
				throw new MongoException(18, "Authentication Failed");
			}
		};

		MongoRepositoryFactory factory = new MongoRepositoryFactory(ops);
		MongoEntityInformation<Person, String> entityInformation = factory.getEntityInformation(Person.class);
		repository = new QuerydslMongoPredicateExecutor<>(entityInformation, ops);

		repository.findOne(person.firstname.contains("batman"));
	}
}
