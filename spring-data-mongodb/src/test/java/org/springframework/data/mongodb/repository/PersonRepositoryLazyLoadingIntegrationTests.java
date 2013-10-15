/*
 * Copyright 2010-2013 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.aop.Advisor;
import org.springframework.aop.framework.Advised;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter.LazyLoadingInterceptor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test for {@link PersonRepository} for lazy loading support.
 * 
 * @author Thomas Darimont
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

	/**
	 * @see DATAMONGO-348
	 */
	@Test
	public void shouldLoadAssociationWithDbRefOnInterfaceAndLazyLoadingEnabled() throws Exception {

		User thomas = new User();
		thomas.username = "Thomas";
		operations.save(thomas);

		Person person = new Person();
		person.setFirstname("Oliver");
		person.setFans(Arrays.asList(thomas));
		person.setRealFans(new ArrayList<User>(Arrays.asList(thomas)));
		repository.save(person);

		Person oliver = repository.findOne(person.id);
		List<User> fans = oliver.getFans();
		LazyLoadingInterceptor interceptor = extractInterceptor(fans);

		assertThat(interceptor.getResult(), is(nullValue()));
		assertThat(interceptor.isResolved(), is(false));

		User user = fans.get(0);
		assertThat(interceptor.getResult(), is(notNullValue()));
		assertThat(interceptor.isResolved(), is(true));
		assertThat(user.getUsername(), is(thomas.getUsername()));
	}

	/**
	 * @see DATAMONGO-348
	 */
	@Test
	public void shouldLoadAssociationWithDbRefOnConcreteCollectionAndLazyLoadingEnabled() throws Exception {

		User thomas = new User();
		thomas.username = "Thomas";
		operations.save(thomas);

		Person person = new Person();
		person.setFirstname("Oliver");
		person.setFans(Arrays.asList(thomas));
		person.setRealFans(new ArrayList<User>(Arrays.asList(thomas)));
		repository.save(person);

		Person oliver = repository.findOne(person.id);
		List<User> realFans = oliver.getRealFans();
		LazyLoadingInterceptor interceptor = extractInterceptor(realFans);

		assertThat(interceptor.getResult(), is(nullValue()));
		assertThat(interceptor.isResolved(), is(false));

		User realFan = realFans.get(0);
		assertThat(interceptor.getResult(), is(notNullValue()));
		assertThat(interceptor.isResolved(), is(true));
		assertThat(realFan.getUsername(), is(thomas.getUsername()));

		realFans = oliver.getRealFans();
		assertThat(interceptor.getResult(), is(notNullValue()));
		assertThat(interceptor.isResolved(), is(true));

		realFan = realFans.get(0);
		assertThat(realFan.getUsername(), is(thomas.getUsername()));
	}

	/**
	 * @see DATAMONGO-348
	 */
	@Test
	public void shouldLoadAssociationWithDbRefOnConcreteDomainClassAndLazyLoadingEnabled() throws Exception {

		User thomas = new User();
		thomas.username = "Thomas";
		operations.save(thomas);

		Person person = new Person();
		person.setFirstname("Oliver");
		person.setCoworker(thomas);
		repository.save(person);

		Person oliver = repository.findOne(person.id);

		User coworker = oliver.getCoworker();
		LazyLoadingInterceptor interceptor = extractInterceptor(coworker);

		assertThat(interceptor.getResult(), is(nullValue()));
		assertThat(interceptor.isResolved(), is(false));
		assertThat(coworker.getUsername(), is(thomas.getUsername()));
		assertThat(interceptor.isResolved(), is(true));
		assertThat(coworker.getUsername(), is(thomas.getUsername()));
	}

	private LazyLoadingInterceptor extractInterceptor(Object proxy) {
		return (LazyLoadingInterceptor) ((Advisor) ((Advised) proxy).getAdvisors()[0]).getAdvice();
	}
}
