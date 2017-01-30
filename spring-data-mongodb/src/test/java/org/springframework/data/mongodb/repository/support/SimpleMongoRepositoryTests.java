/*
 * Copyright 2010-2017 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.domain.ExampleMatcher.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.ExampleMatcher.*;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.Address;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.User;
import org.springframework.data.mongodb.repository.Person.Sex;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author <a href="mailto:kowsercse@gmail.com">A. B. M. Kowser</a>
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class SimpleMongoRepositoryTests {

	@Autowired private MongoTemplate template;

	private Person oliver, dave, carter, boyd, stefan, leroi, alicia;
	private List<Person> all;

	private MongoEntityInformation<Person, String> personEntityInformation = new CustomizedPersonInformation();
	private SimpleMongoRepository<Person, String> repository;

	@Before
	public void setUp() {
		repository = new SimpleMongoRepository<Person, String>(personEntityInformation, template);
		repository.deleteAll();

		oliver = new Person("Oliver August", "Matthews", 4);
		dave = new Person("Dave", "Matthews", 42);
		carter = new Person("Carter", "Beauford", 49);
		boyd = new Person("Boyd", "Tinsley", 45);
		stefan = new Person("Stefan", "Lessard", 34);
		leroi = new Person("Leroi", "Moore", 41);
		alicia = new Person("Alicia", "Keys", 30, Sex.FEMALE);

		all = repository.save(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia));
	}

	@Test
	public void findALlFromCustomCollectionName() {
		List<Person> result = repository.findAll();
		assertThat(result, hasSize(all.size()));
	}

	@Test
	public void findOneFromCustomCollectionName() {
		Person result = repository.findOne(dave.getId()).get();
		assertThat(result, is(dave));
	}

	@Test
	public void deleteFromCustomCollectionName() {
		repository.delete(dave);
		List<Person> result = repository.findAll();

		assertThat(result, hasSize(all.size() - 1));
		assertThat(result, not(hasItem(dave)));
	}

	@Test
	public void deleteByIdFromCustomCollectionName() {
		repository.delete(dave.getId());
		List<Person> result = repository.findAll();

		assertThat(result, hasSize(all.size() - 1));
		assertThat(result, not(hasItem(dave)));
	}

	@Test // DATAMONGO-1054
	public void shouldInsertSingle() {

		String randomId = UUID.randomUUID().toString();

		Person person1 = new Person("First1" + randomId, "Last2" + randomId, 42);
		person1 = repository.insert(person1);

		Person saved = repository.findOne(person1.getId()).get();

		assertThat(saved, is(equalTo(person1)));
	}

	@Test // DATAMONGO-1054
	public void shouldInsertMultipleFromList() {

		String randomId = UUID.randomUUID().toString();
		Map<String, Person> idToPerson = new HashMap<String, Person>();
		List<Person> persons = new ArrayList<Person>();

		for (int i = 0; i < 10; i++) {
			Person person = new Person("First" + i + randomId, "Last" + randomId + i, 42 + i);
			idToPerson.put(person.getId(), person);
			persons.add(person);
		}

		List<Person> saved = repository.insert(persons);

		assertThat(saved, hasSize(persons.size()));
		assertThatAllReferencePersonsWereStoredCorrectly(idToPerson, saved);
	}

	@Test // DATAMONGO-1054
	public void shouldInsertMutlipleFromSet() {

		String randomId = UUID.randomUUID().toString();
		Map<String, Person> idToPerson = new HashMap<String, Person>();
		Set<Person> persons = new HashSet<Person>();

		for (int i = 0; i < 10; i++) {
			Person person = new Person("First" + i + randomId, "Last" + i + randomId, 42 + i);
			idToPerson.put(person.getId(), person);
			persons.add(person);
		}

		List<Person> saved = repository.insert(persons);

		assertThat(saved, hasSize(persons.size()));
		assertThatAllReferencePersonsWereStoredCorrectly(idToPerson, saved);
	}

	@Test // DATAMONGO-1245, DATAMONGO-1464
	public void findByExampleShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		Page<Person> result = repository.findAll(Example.of(sample), new PageRequest(0, 10));

		assertThat(result.getContent(), hasItems(dave, oliver));
		assertThat(result.getContent(), hasSize(2));
		assertThat(result.getTotalPages(), is(1));
	}

	@Test // DATAMONGO-1464
	public void findByExampleMultiplePagesShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		Page<Person> result = repository.findAll(Example.of(sample), new PageRequest(0, 1));

		assertThat(result.getContent(), hasSize(1));
		assertThat(result.getTotalPages(), is(2));
	}

	@Test // DATAMONGO-1245
	public void findAllByExampleShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		List<Person> result = repository.findAll(Example.of(sample));

		assertThat(result, containsInAnyOrder(dave, oliver));
		assertThat(result, hasSize(2));
	}

	@Test // DATAMONGO-1245
	public void findAllByExampleShouldLookUpEntriesCorrectlyWhenUsingNestedObject() {

		dave.setAddress(new Address("1600 Pennsylvania Ave NW", "20500", "Washington"));
		repository.save(dave);

		oliver.setAddress(new Address("East Capitol St NE & First St SE", "20004", "Washington"));
		repository.save(oliver);

		Person sample = new Person();
		sample.setAddress(dave.getAddress());
		trimDomainType(sample, "id", "createdAt", "email");

		List<Person> result = repository.findAll(Example.of(sample));

		assertThat(result, hasItem(dave));
		assertThat(result, hasSize(1));
	}

	@Test // DATAMONGO-1245
	public void findAllByExampleShouldLookUpEntriesCorrectlyWhenUsingPartialNestedObject() {

		dave.setAddress(new Address("1600 Pennsylvania Ave NW", "20500", "Washington"));
		repository.save(dave);

		oliver.setAddress(new Address("East Capitol St NE & First St SE", "20004", "Washington"));
		repository.save(oliver);

		Person sample = new Person();
		sample.setAddress(new Address(null, null, "Washington"));
		trimDomainType(sample, "id", "createdAt", "email");

		List<Person> result = repository.findAll(Example.of(sample));

		assertThat(result, hasItems(dave, oliver));
		assertThat(result, hasSize(2));
	}

	@Test // DATAMONGO-1245
	public void findAllByExampleShouldNotFindEntriesWhenUsingPartialNestedObjectInStrictMode() {

		dave.setAddress(new Address("1600 Pennsylvania Ave NW", "20500", "Washington"));
		repository.save(dave);

		Person sample = new Person();
		sample.setAddress(new Address(null, null, "Washington"));
		trimDomainType(sample, "id", "createdAt", "email");

		Example<Person> example = Example.of(sample, matching().withIncludeNullValues());
		List<Person> result = repository.findAll(example);

		assertThat(result, empty());
	}

	@Test // DATAMONGO-1245
	public void findAllByExampleShouldLookUpEntriesCorrectlyWhenUsingNestedObjectInStrictMode() {

		dave.setAddress(new Address("1600 Pennsylvania Ave NW", "20500", "Washington"));
		repository.save(dave);

		Person sample = new Person();
		sample.setAddress(dave.getAddress());
		trimDomainType(sample, "id", "createdAt", "email");

		Example<Person> example = Example.of(sample, matching().withIncludeNullValues());
		List<Person> result = repository.findAll(example);

		assertThat(result, hasItem(dave));
		assertThat(result, hasSize(1));
	}

	@Test // DATAMONGO-1245
	public void findAllByExampleShouldRespectStringMatchMode() {

		Person sample = new Person();
		sample.setLastname("Mat");
		trimDomainType(sample, "id", "createdAt", "email");

		Example<Person> example = Example.of(sample, matching().withStringMatcher(StringMatcher.STARTING));
		List<Person> result = repository.findAll(example);

		assertThat(result, hasItems(dave, oliver));
		assertThat(result, hasSize(2));
	}

	@Test // DATAMONGO-1245
	public void findAllByExampleShouldResolveDbRefCorrectly() {

		User user = new User();
		user.setId("c0nf1ux");
		user.setUsername("conflux");
		template.save(user);

		Person megan = new Person("megan", "tarash");
		megan.setCreator(user);

		repository.save(megan);

		Person sample = new Person();
		sample.setCreator(user);
		trimDomainType(sample, "id", "createdAt", "email");

		List<Person> result = repository.findAll(Example.of(sample));

		assertThat(result, hasItem(megan));
		assertThat(result, hasSize(1));
	}

	@Test // DATAMONGO-1245
	public void findAllByExampleShouldResolveLegacyCoordinatesCorrectly() {

		Person megan = new Person("megan", "tarash");
		megan.setLocation(new Point(41.85003D, -87.65005D));

		repository.save(megan);

		Person sample = new Person();
		sample.setLocation(megan.getLocation());
		trimDomainType(sample, "id", "createdAt", "email");

		List<Person> result = repository.findAll(Example.of(sample));

		assertThat(result, hasItem(megan));
		assertThat(result, hasSize(1));
	}

	@Test // DATAMONGO-1245
	public void findAllByExampleShouldResolveGeoJsonCoordinatesCorrectly() {

		Person megan = new Person("megan", "tarash");
		megan.setLocation(new GeoJsonPoint(41.85003D, -87.65005D));

		repository.save(megan);

		Person sample = new Person();
		sample.setLocation(megan.getLocation());
		trimDomainType(sample, "id", "createdAt", "email");

		List<Person> result = repository.findAll(Example.of(sample));

		assertThat(result, hasItem(megan));
		assertThat(result, hasSize(1));
	}

	@Test // DATAMONGO-1245
	public void findAllByExampleShouldProcessInheritanceCorrectly() {

		PersonExtended reference = new PersonExtended();
		reference.setLastname("Matthews");

		repository.save(reference);

		PersonExtended sample = new PersonExtended();
		sample.setLastname("Matthews");

		trimDomainType(sample, "id", "createdAt", "email");

		List<PersonExtended> result = repository.findAll(Example.of(sample));

		assertThat(result, hasSize(1));
		assertThat(result, hasItem(reference));
	}

	@Test // DATAMONGO-1245
	public void findOneByExampleShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setFirstname("Dave");
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		Person result = repository.findOne(Example.of(sample));

		assertThat(result, is(equalTo(dave)));
	}

	@Test // DATAMONGO-1245
	public void existsByExampleShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setFirstname("Dave");
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		boolean result = repository.exists(Example.of(sample));

		assertThat(result, is(true));
	}

	@Test // DATAMONGO-1245
	public void countByExampleShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		long result = repository.count(Example.of(sample));

		assertThat(result, is(equalTo(2L)));
	}

	private void assertThatAllReferencePersonsWereStoredCorrectly(Map<String, Person> references, List<Person> saved) {

		for (Person person : saved) {
			Person reference = references.get(person.getId());
			assertThat(person, is(equalTo(reference)));
		}
	}

	private void trimDomainType(Object source, String... attributes) {

		for (String attribute : attributes) {
			ReflectionTestUtils.setField(source, attribute, null);
		}
	}

	private static class CustomizedPersonInformation implements MongoEntityInformation<Person, String> {

		@Override
		public boolean isNew(Person entity) {
			return entity.getId() == null;
		}

		@Override
		public Optional<String> getId(Person entity) {
			return Optional.ofNullable(entity.getId());
		}

		@Override
		public Class<String> getIdType() {
			return String.class;
		}

		@Override
		public Class<Person> getJavaType() {
			return Person.class;
		}

		@Override
		public String getCollectionName() {
			return "customizedPerson";
		}

		@Override
		public String getIdAttribute() {
			return "id";
		}
	}

	@Document
	static class PersonExtended extends Person {}
}
