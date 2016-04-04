/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;

/**
 * Integration test for {@link MongoTemplate}.
 * 
 * @author Oliver Gierke
 * @author Thomas Risberg
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:template-mapping.xml")
public class MongoTemplateMappingTests {

	@Autowired @Qualifier("mongoTemplate1") MongoTemplate template1;

	@Autowired @Qualifier("mongoTemplate2") MongoTemplate template2;

	@Rule public ExpectedException thrown = ExpectedException.none();

	@Before
	public void setUp() {
		template1.dropCollection(template1.getCollectionName(Person.class));
	}

	@Test
	public void insertsEntityCorrectly1() throws Exception {

		addAndRetrievePerson(template1);
		checkPersonPersisted(template1);

	}

	@Test
	public void insertsEntityCorrectly2() throws Exception {

		addAndRetrievePerson(template2);
		checkPersonPersisted(template2);

	}

	private void addAndRetrievePerson(MongoTemplate template) {
		Person person = new Person("Oliver");
		person.setAge(25);
		template.insert(person);

		Person result = template.findById(person.getId(), Person.class);
		assertThat(result.getFirstName(), is("Oliver"));
		assertThat(result.getAge(), is(25));
	}

	private void checkPersonPersisted(MongoTemplate template) {
		template.execute(Person.class, new CollectionCallback<Object>() {
			public Object doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
				Document dbo = collection.find(new Document()).first();
				assertThat((String) dbo.get("name"), is("Oliver"));
				return null;
			}
		});
	}
}
