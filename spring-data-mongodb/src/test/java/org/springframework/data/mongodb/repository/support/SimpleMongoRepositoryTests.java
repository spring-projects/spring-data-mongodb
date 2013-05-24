/*
 * Copyright 2010-2012 the original author or authors.
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
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

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
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class SimpleMongoRepositoryTests {

	@Autowired
	private MongoTemplate template;

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
