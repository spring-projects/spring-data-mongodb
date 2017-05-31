/*
 * Copyright 2015-2017 the original author or authors.
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
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.QPerson;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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
	QuerydslMongoPredicateExecutor<Person> repository;

	Person dave, oliver, carter;
	QPerson person;

	@Before
	public void setup() {

		MongoRepositoryFactory factory = new MongoRepositoryFactory(operations);
		MongoEntityInformation<Person, String> entityInformation = factory.getEntityInformation(Person.class);
		repository = new QuerydslMongoPredicateExecutor<Person>(entityInformation, operations);

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
}
