/*
 * Copyright 2011-2016 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.QPerson;
import org.springframework.data.mongodb.repository.User;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Unit tests for {@link QuerydslRepositorySupport}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class QuerydslRepositorySupportTests {

	@Autowired MongoOperations operations;
	Person person;
	QuerydslRepositorySupport repoSupport;

	@Before
	public void setUp() {

		operations.remove(new Query(), Person.class);
		person = new Person("Dave", "Matthews");
		operations.save(person);

		repoSupport = new QuerydslRepositorySupport(operations) {};
	}

	@Test
	public void providesMongoQuery() {

		QPerson p = QPerson.person;
		QuerydslRepositorySupport support = new QuerydslRepositorySupport(operations) {};
		SpringDataMongodbQuery<Person> query = support.from(p).where(p.lastname.eq("Matthews"));
		assertThat(query.fetchOne(), is(person));
	}

	/**
	 * @see DATAMONGO-1063
	 */
	@Test
	public void shouldAllowAny() {

		person.setSkills(Arrays.asList("vocalist", "songwriter", "guitarist"));

		operations.save(person);

		QPerson p = QPerson.person;

		SpringDataMongodbQuery<Person> query = repoSupport.from(p).where(p.skills.any().in("guitarist"));

		assertThat(query.fetchOne(), is(person));
	}

	/**
	 * @see DATAMONGO-1394
	 */
	@Test
	public void shouldAllowDbRefAgainstIdProperty() {

		User bart = new User();
		bart.setUsername("bart@simpson.com");
		operations.save(bart);

		person.setCoworker(bart);
		operations.save(person);

		QPerson p = QPerson.person;

		SpringDataMongodbQuery<Person> queryUsingIdField = repoSupport.from(p).where(p.coworker.id.eq(bart.getId()));
		SpringDataMongodbQuery<Person> queryUsingRefObject = repoSupport.from(p).where(p.coworker.eq(bart));

		assertThat(queryUsingIdField.fetchOne(), equalTo(person));
		assertThat(queryUsingIdField.fetchOne(), equalTo(queryUsingRefObject.fetchOne()));
	}
}
