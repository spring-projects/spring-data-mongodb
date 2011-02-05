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
package org.springframework.data.document.mongodb;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.document.mongodb.builder.Query;
import org.springframework.data.document.mongodb.builder.Update;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.WriteConcern;

/**
 * Integration test for {@link MongoTemplate}.
 * 
 * @author Oliver Gierke
 * @author Thomas Risberg
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class MongoTemplateTests {

	@Autowired
	MongoTemplate template;

	@Rule
	public ExpectedException thrown = ExpectedException.none();
	
	@Before
	public void setUp() {
		template.dropCollection(template.getDefaultCollectionName());
	}

	@Test
	public void insertsSimpleEntityCorrectly() throws Exception {

		Person person = new Person("Oliver");
		person.setAge(25);
		template.insert(person);
		
		MongoConverter converter = template.getConverter();

		List<Person> result = template.find(Query.startQueryWithCriteria("_id").is(converter.convertObjectId(person.getId())).end(), Person.class);
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(person));
	}

	@Test
	public void updateFailure() throws Exception {

		MongoTemplate mongoTemplate = new MongoTemplate(template.getDb().getMongo(), "test", "people", 
				new WriteConcern(), WriteResultChecking.EXCEPTION);
		
		Person person = new Person("Oliver2");
		person.setAge(25);
		mongoTemplate.insert(person);
		
		Query q = Query.startQueryWithCriteria("BOGUS").gt(22).end();
		Update u = Update.startUpdate().set("firstName", "Sven");
		thrown.expect(DataIntegrityViolationException.class);
		thrown.expectMessage( endsWith("0 documents updated") );
		mongoTemplate.updateFirst(q, u);
		
	}

	@Test
	public void simpleQuery() throws Exception {
		Query.startQueryWithCriteria("name").is("Mary").and("age").lt(33).gt(22).end().skip(22).limit(20);
		// TODO: more tests
	}
}
