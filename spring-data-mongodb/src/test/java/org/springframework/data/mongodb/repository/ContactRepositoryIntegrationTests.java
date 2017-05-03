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
package org.springframework.data.mongodb.repository;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Example;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests for {@link ContactRepository}. Mostly related to mapping inheritance.
 * 
 * @author Oliver Gierke
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("config/MongoNamespaceIntegrationTests-context.xml")
public class ContactRepositoryIntegrationTests {

	@Autowired ContactRepository repository;

	@Before
	public void setUp() throws Exception {
		repository.deleteAll();
	}

	@Test
	public void readsAndWritesContactCorrectly() {

		Person person = new Person("Oliver", "Gierke");
		Contact result = repository.save(person);

		assertTrue(repository.findById(result.getId().toString()).get() instanceof Person);
	}

	@Test // DATAMONGO-1245
	public void findsContactByTypedExample() {

		Person person = repository.save(new Person("Oliver", "Gierke"));

		assertThat(repository.findOne(Example.of(person)), instanceOf(Person.class));
	}
}
