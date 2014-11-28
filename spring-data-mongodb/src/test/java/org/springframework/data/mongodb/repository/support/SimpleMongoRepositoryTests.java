/*
 * Copyright 2010-2014 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.Person.Sex;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author <a href="mailto:kowsercse@gmail.com">A. B. M. Kowser</a>
 * @author Thomas Darimont
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
		Person result = repository.findOne(dave.getId());
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

	/**
	 * @see DATAMONGO-1054
	 */
	@Test
	public void shouldInsertSingle() {

		String randomId = UUID.randomUUID().toString();

		Person person1 = new Person("First1" + randomId, "Last2" + randomId, 42);
		person1 = repository.insert(person1);

		Person saved = repository.findOne(person1.getId());

		assertThat(saved, is(equalTo(person1)));
	}

	/**
	 * @see DATAMONGO-1054
	 */
	@Test
	public void shouldInsertMutlipleFromList() {

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

	/**
	 * @see DATAMONGO-1054
	 */
	@Test
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

	private void assertThatAllReferencePersonsWereStoredCorrectly(Map<String, Person> references, List<Person> saved) {

		for (Person person : saved) {
			Person reference = references.get(person.getId());
			assertThat(person, is(equalTo(reference)));
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
	}

}
