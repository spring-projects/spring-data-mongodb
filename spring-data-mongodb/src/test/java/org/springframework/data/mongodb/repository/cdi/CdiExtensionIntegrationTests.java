/*
 * Copyright 2012-2018 the original author or authors.
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
package org.springframework.data.mongodb.repository.cdi;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.mongodb.repository.Person;

/**
 * Integration tests for {@link MongoRepositoryExtension}.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class CdiExtensionIntegrationTests {

	static SeContainer container;

	@BeforeClass
	public static void setUp() {

		container = SeContainerInitializer.newInstance() //
				.disableDiscovery() //
				.addPackages(CdiExtensionIntegrationTests.class) //
				.initialize();
	}

	@AfterClass
	public static void tearDown() {
		container.close();
	}

	@Test // DATAMONGO-356, DATAMONGO-1785
	public void bootstrapsRepositoryCorrectly() {

		RepositoryClient client = container.select(RepositoryClient.class).get();
		CdiPersonRepository repository = client.getRepository();

		assertThat(repository, is(notNullValue()));

		repository.deleteAll();

		Person person = new Person("Dave", "Matthews");
		Person result = repository.save(person);

		assertThat(result, is(notNullValue()));
		assertThat(repository.findById(person.getId()).get().getId(), is(result.getId()));
	}

	@Test // DATAMONGO-1017, DATAMONGO-1785
	public void returnOneFromCustomImpl() {

		RepositoryClient repositoryConsumer = container.select(RepositoryClient.class).get();
		assertThat(repositoryConsumer.getSamplePersonRepository().returnOne(), is(1));
	}
}
