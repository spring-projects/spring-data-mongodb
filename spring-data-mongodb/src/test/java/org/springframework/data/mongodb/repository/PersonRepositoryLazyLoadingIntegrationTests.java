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
import static org.springframework.data.mongodb.core.convert.LazyLoadingTestUtils.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test for {@link PersonRepository} for lazy loading support.
 * 
 * @author Thomas Darimont
 * @author Oliver Gierke
 */
@ContextConfiguration(locations = "PersonRepositoryIntegrationTests-context.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class PersonRepositoryLazyLoadingIntegrationTests {

	@Autowired PersonRepository repository;
	@Autowired MongoOperations operations;

	@Before
	public void setUp() throws InterruptedException {

		repository.deleteAll();
		operations.remove(new org.springframework.data.mongodb.core.query.Query(), User.class);
	}

	@Test // DATAMONGO-348
	public void shouldLoadAssociationWithDbRefOnInterfaceAndLazyLoadingEnabled() throws Exception {

		User thomas = new User();
		thomas.username = "Thomas";
		operations.save(thomas);

		Person person = new Person();
		person.setFirstname("Oliver");
		person.setFans(Arrays.asList(thomas));
		person.setRealFans(new ArrayList<User>(Arrays.asList(thomas)));
		repository.save(person);

		Person oliver = repository.findById(person.id).get();
		List<User> fans = oliver.getFans();

		assertProxyIsResolved(fans, false);

		User user = fans.get(0);
		assertProxyIsResolved(fans, true);
		assertThat(user.getUsername(), is(thomas.getUsername()));
	}

	@Test // DATAMONGO-348
	public void shouldLoadAssociationWithDbRefOnConcreteCollectionAndLazyLoadingEnabled() throws Exception {

		User thomas = new User();
		thomas.username = "Thomas";
		operations.save(thomas);

		Person person = new Person();
		person.setFirstname("Oliver");
		person.setFans(Arrays.asList(thomas));
		person.setRealFans(new ArrayList<User>(Arrays.asList(thomas)));
		repository.save(person);

		Person oliver = repository.findById(person.id).get();
		List<User> realFans = oliver.getRealFans();

		assertProxyIsResolved(realFans, false);
		User realFan = realFans.get(0);
		assertProxyIsResolved(realFans, true);
		assertThat(realFan.getUsername(), is(thomas.getUsername()));

		realFans = oliver.getRealFans();
		assertProxyIsResolved(realFans, true);

		realFan = realFans.get(0);
		assertThat(realFan.getUsername(), is(thomas.getUsername()));
	}

	@Test // DATAMONGO-348
	public void shouldLoadAssociationWithDbRefOnConcreteDomainClassAndLazyLoadingEnabled() throws Exception {

		User thomas = new User();
		thomas.username = "Thomas";
		operations.save(thomas);

		Person person = new Person();
		person.setFirstname("Oliver");
		person.setCoworker(thomas);
		repository.save(person);

		Person oliver = repository.findById(person.id).get();

		User coworker = oliver.getCoworker();

		assertProxyIsResolved(coworker, false);
		assertThat(coworker.getUsername(), is(thomas.getUsername()));
		assertProxyIsResolved(coworker, true);
		assertThat(coworker.getUsername(), is(thomas.getUsername()));
	}
}
