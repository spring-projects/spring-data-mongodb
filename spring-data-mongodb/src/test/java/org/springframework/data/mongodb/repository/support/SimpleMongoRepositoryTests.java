/*
 * Copyright 2010-2021 the original author or authors.
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

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.domain.ExampleMatcher.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.repository.Address;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.Person.Sex;
import org.springframework.data.mongodb.repository.User;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.EnableIfReplicaSetAvailable;
import org.springframework.data.mongodb.test.util.MongoServerCondition;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @author A. B. M. Kowser
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jens Schauder
 */
@ExtendWith({ MongoTemplateExtension.class, MongoServerCondition.class })
class SimpleMongoRepositoryTests {

	@Template(initialEntitySet = Person.class) //
	private static MongoTestTemplate template;

	private Person oliver, dave, carter, boyd, stefan, leroi, alicia;
	private List<Person> all;

	private MongoEntityInformation<Person, String> personEntityInformation = new CustomizedPersonInformation();
	private SimpleMongoRepository<Person, String> repository = new SimpleMongoRepository<>(personEntityInformation,
			template);

	@BeforeEach
	void setUp() {

		repository.deleteAll();

		oliver = new Person("Oliver August", "Matthews", 4);
		dave = new Person("Dave", "Matthews", 42);
		carter = new Person("Carter", "Beauford", 49);
		boyd = new Person("Boyd", "Tinsley", 45);
		stefan = new Person("Stefan", "Lessard", 34);
		leroi = new Person("Leroi", "Moore", 41);
		alicia = new Person("Alicia", "Keys", 30, Sex.FEMALE);

		all = repository.saveAll(asList(oliver, dave, carter, boyd, stefan, leroi, alicia));
	}

	@Test
	void findAllFromCustomCollectionName() {
		assertThat(repository.findAll()).hasSameSizeAs(all);
	}

	@Test
	void findOneFromCustomCollectionName() {
		assertThat(repository.findById(dave.getId())).contains(dave);
	}

	@Test
	void deleteFromCustomCollectionName() {

		repository.delete(dave);

		assertThat(repository.findAll()).hasSize(all.size() - 1).doesNotContain(dave);
	}

	@Test
	void deleteByIdFromCustomCollectionName() {

		repository.deleteById(dave.getId());

		assertThat(repository.findAll()).hasSize(all.size() - 1).doesNotContain(dave);
	}

	@Test // DATAMONGO-1054
	void shouldInsertSingle() {

		String randomId = UUID.randomUUID().toString();

		Person person1 = new Person("First1" + randomId, "Last2" + randomId, 42);
		person1 = repository.insert(person1);

		assertThat(repository.findById(person1.getId())).contains(person1);
	}

	@Test // DATAMONGO-1054
	void shouldInsertMultipleFromList() {

		String randomId = UUID.randomUUID().toString();
		Map<String, Person> idToPerson = new HashMap<String, Person>();
		List<Person> persons = new ArrayList<Person>();

		for (int i = 0; i < 10; i++) {
			Person person = new Person("First" + i + randomId, "Last" + randomId + i, 42 + i);
			idToPerson.put(person.getId(), person);
			persons.add(person);
		}

		List<Person> saved = repository.insert(persons);

		assertThat(saved).hasSameSizeAs(persons);
		assertThatAllReferencePersonsWereStoredCorrectly(idToPerson, saved);
	}

	@Test // DATAMONGO-1054
	void shouldInsertMutlipleFromSet() {

		String randomId = UUID.randomUUID().toString();
		Map<String, Person> idToPerson = new HashMap<String, Person>();
		Set<Person> persons = new HashSet<Person>();

		for (int i = 0; i < 10; i++) {
			Person person = new Person("First" + i + randomId, "Last" + i + randomId, 42 + i);
			idToPerson.put(person.getId(), person);
			persons.add(person);
		}

		List<Person> saved = repository.insert(persons);

		assertThat(saved).hasSameSizeAs(persons);
		assertThatAllReferencePersonsWereStoredCorrectly(idToPerson, saved);
	}

	@Test // DATAMONGO-1245, DATAMONGO-1464
	void findByExampleShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		Page<Person> result = repository.findAll(Example.of(sample), PageRequest.of(0, 10));

		assertThat(result.getContent()).hasSize(2).contains(dave, oliver);
		assertThat(result.getTotalPages()).isEqualTo(1);
	}

	@Test // GH-3751
	void findByExampleShouldReturnUnpagedResults() {

		Person sample = new Person();
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		Page<Person> result = repository.findAll(Example.of(sample), Pageable.unpaged());

		assertThat(result.getContent()).hasSize(2).contains(dave, oliver);
		assertThat(result.getTotalPages()).isEqualTo(1);
	}

	@Test // DATAMONGO-1464
	void findByExampleMultiplePagesShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		Page<Person> result = repository.findAll(Example.of(sample), PageRequest.of(0, 1));

		assertThat(result.getContent()).hasSize(1);
		assertThat(result.getTotalPages()).isEqualTo(2);
	}

	@Test // DATAMONGO-1245
	void findAllByExampleShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		assertThat(repository.findAll(Example.of(sample))).hasSize(2).contains(dave, oliver);
	}

	@Test // DATAMONGO-1245
	void findAllByExampleShouldLookUpEntriesCorrectlyWhenUsingNestedObject() {

		dave.setAddress(new Address("1600 Pennsylvania Ave NW", "20500", "Washington"));
		repository.save(dave);

		oliver.setAddress(new Address("East Capitol St NE & First St SE", "20004", "Washington"));
		repository.save(oliver);

		Person sample = new Person();
		sample.setAddress(dave.getAddress());
		trimDomainType(sample, "id", "createdAt", "email");

		assertThat(repository.findAll(Example.of(sample))).hasSize(1).contains(dave);
	}

	@Test // DATAMONGO-1245
	void findAllByExampleShouldLookUpEntriesCorrectlyWhenUsingPartialNestedObject() {

		dave.setAddress(new Address("1600 Pennsylvania Ave NW", "20500", "Washington"));
		repository.save(dave);

		oliver.setAddress(new Address("East Capitol St NE & First St SE", "20004", "Washington"));
		repository.save(oliver);

		Person sample = new Person();
		sample.setAddress(new Address(null, null, "Washington"));
		trimDomainType(sample, "id", "createdAt", "email");

		assertThat(repository.findAll(Example.of(sample))).hasSize(2).contains(dave, oliver);
	}

	@Test // DATAMONGO-1245
	void findAllByExampleShouldNotFindEntriesWhenUsingPartialNestedObjectInStrictMode() {

		dave.setAddress(new Address("1600 Pennsylvania Ave NW", "20500", "Washington"));
		repository.save(dave);

		Person sample = new Person();
		sample.setAddress(new Address(null, null, "Washington"));
		trimDomainType(sample, "id", "createdAt", "email");

		Example<Person> example = Example.of(sample, matching().withIncludeNullValues());

		assertThat(repository.findAll(example)).isEmpty();
	}

	@Test // DATAMONGO-1245
	void findAllByExampleShouldLookUpEntriesCorrectlyWhenUsingNestedObjectInStrictMode() {

		dave.setAddress(new Address("1600 Pennsylvania Ave NW", "20500", "Washington"));
		repository.save(dave);

		Person sample = new Person();
		sample.setAddress(dave.getAddress());
		trimDomainType(sample, "id", "createdAt", "email");

		Example<Person> example = Example.of(sample, matching().withIncludeNullValues());

		assertThat(repository.findAll(example)).hasSize(1).contains(dave);
	}

	@Test // DATAMONGO-1245
	void findAllByExampleShouldRespectStringMatchMode() {

		Person sample = new Person();
		sample.setLastname("Mat");
		trimDomainType(sample, "id", "createdAt", "email");

		Example<Person> example = Example.of(sample, matching().withStringMatcher(StringMatcher.STARTING));

		assertThat(repository.findAll(example)).hasSize(2).contains(dave, oliver);
	}

	@Test // DATAMONGO-1245
	void findAllByExampleShouldResolveDbRefCorrectly() {

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

		assertThat(repository.findAll(Example.of(sample))).hasSize(1).contains(megan);
	}

	@Test // DATAMONGO-1245
	void findAllByExampleShouldResolveLegacyCoordinatesCorrectly() {

		Person megan = new Person("megan", "tarash");
		megan.setLocation(new Point(41.85003D, -87.65005D));

		repository.save(megan);

		Person sample = new Person();
		sample.setLocation(megan.getLocation());
		trimDomainType(sample, "id", "createdAt", "email");

		assertThat(repository.findAll(Example.of(sample))).hasSize(1).contains(megan);
	}

	@Test // DATAMONGO-1245
	void findAllByExampleShouldResolveGeoJsonCoordinatesCorrectly() {

		Person megan = new Person("megan", "tarash");
		megan.setLocation(new GeoJsonPoint(41.85003D, -87.65005D));

		repository.save(megan);

		Person sample = new Person();
		sample.setLocation(megan.getLocation());
		trimDomainType(sample, "id", "createdAt", "email");

		assertThat(repository.findAll(Example.of(sample))).hasSize(1).contains(megan);
	}

	@Test // DATAMONGO-1245
	void findAllByExampleShouldProcessInheritanceCorrectly() {

		PersonExtended reference = new PersonExtended();
		reference.setLastname("Matthews");

		repository.save(reference);

		PersonExtended sample = new PersonExtended();
		sample.setLastname("Matthews");

		trimDomainType(sample, "id", "createdAt", "email");

		assertThat(repository.findAll(Example.of(sample))).hasSize(1).contains(reference);
	}

	@Test // DATAMONGO-1245
	void findOneByExampleShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setFirstname("Dave");
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		assertThat(repository.findOne(Example.of(sample))).isPresent().contains(dave);
	}

	@Test // DATAMONGO-1245
	void existsByExampleShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setFirstname("Dave");
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		assertThat(repository.exists(Example.of(sample))).isTrue();
	}

	@Test // DATAMONGO-1245
	void countByExampleShouldLookUpEntriesCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");
		trimDomainType(sample, "id", "createdAt", "email");

		assertThat(repository.count(Example.of(sample))).isEqualTo(2L);
	}

	@Test // DATAMONGO-1896
	void saveAllUsesEntityCollection() {

		Person first = new PersonExtended();
		first.setEmail("foo@bar.com");
		ReflectionTestUtils.setField(first, "id", null);

		Person second = new PersonExtended();
		second.setEmail("bar@foo.com");
		ReflectionTestUtils.setField(second, "id", null);

		repository.deleteAll();

		repository.saveAll(asList(first, second));

		assertThat(repository.findAll()).containsExactlyInAnyOrder(first, second);
	}

	@Test // DATAMONGO-2130
	@EnableIfReplicaSetAvailable
	@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
	void countShouldBePossibleInTransaction() {

		MongoTransactionManager txmgr = new MongoTransactionManager(template.getMongoDbFactory());
		TransactionTemplate tt = new TransactionTemplate(txmgr);
		tt.afterPropertiesSet();

		long countPreTx = repository.count();

		long count = tt.execute(status -> {

			Person sample = new Person();
			sample.setLastname("Matthews");

			repository.save(sample);

			return repository.count();
		});

		assertThat(count).isEqualTo(countPreTx + 1);
	}

	@Test // DATAMONGO-2130
	@EnableIfReplicaSetAvailable
	@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
	void existsShouldBePossibleInTransaction() {

		MongoTransactionManager txmgr = new MongoTransactionManager(template.getMongoDbFactory());
		TransactionTemplate tt = new TransactionTemplate(txmgr);
		tt.afterPropertiesSet();

		boolean exists = tt.execute(status -> {

			Person sample = new Person();
			sample.setLastname("Matthews");

			repository.save(sample);

			return repository.existsById(sample.getId());
		});

		assertThat(exists).isTrue();
	}

	@Test // DATAMONGO-2652
	void deleteAllByIds() {

		repository.deleteAllById(asList(dave.getId(), carter.getId()));

		assertThat(repository.findAll()) //
				.hasSize(all.size() - 2).doesNotContain(dave, carter);
	}

	private void assertThatAllReferencePersonsWereStoredCorrectly(Map<String, Person> references, List<Person> saved) {

		for (Person person : saved) {
			Person reference = references.get(person.getId());
			assertThat(person).isEqualTo(reference);
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
		public String getId(Person entity) {
			return entity.getId();
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

		@Override
		public Collation getCollation() {
			return null;
		}
	}

	@Document
	static class PersonExtended extends Person {}
}
