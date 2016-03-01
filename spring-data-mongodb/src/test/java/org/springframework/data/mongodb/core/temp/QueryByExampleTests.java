/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.mongodb.core.temp;

import java.net.UnknownHostException;
import java.util.List;

import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.MongoClient;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class QueryByExampleTests {

	MongoTemplate template;

	@Before
	public void setUp() throws UnknownHostException {

		template = new MongoTemplate(new MongoClient(), "query-by-example");
		template.remove(new Query(), Person.class);
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void findByExampleShouldWorkForSimpleProperty() {

		init();

		Person sample = new Person();
		sample.lastname = "stark";

		List<Person> result = template.findByExample(sample);
		Assert.assertThat(result.size(), Is.is(2));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void findByExampleShouldWorkForMultipleProperties() {

		init();

		Person sample = new Person();
		sample.lastname = "stark";
		sample.firstname = "arya";

		List<Person> result = template.findByExample(sample);
		Assert.assertThat(result.size(), Is.is(1));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void findByExampleShouldWorkForIdProperty() {

		init();

		Person p4 = new Person();
		template.save(p4);

		Person sample = new Person();
		sample.id = p4.id;

		List<Person> result = template.findByExample(sample);
		Assert.assertThat(result.size(), Is.is(1));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void findByExampleShouldReturnEmptyListIfNotMatching() {

		init();

		Person sample = new Person();
		sample.firstname = "jon";
		sample.firstname = "stark";

		List<Person> result = template.findByExample(sample);
		Assert.assertThat(result.size(), Is.is(0));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void findByExampleShouldReturnEverythingWhenSampleIsEmpty() {

		init();

		Person sample = new Person();

		List<Person> result = template.findByExample(sample);
		Assert.assertThat(result.size(), Is.is(3));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void findByExampleWithCriteria() {

		init();

		Person sample = new Person();
		sample.lastname = "stark";

		Query query = new Query(new Criteria().alike(Example.of(sample)).and("firstname").regex("^ary*"));

		List<Person> result = template.find(query, Person.class);
		Assert.assertThat(result.size(), Is.is(1));
	}

	public void init() {

		Person p1 = new Person();
		p1.firstname = "bran";
		p1.lastname = "stark";

		Person p2 = new Person();
		p2.firstname = "jon";
		p2.lastname = "snow";

		Person p3 = new Person();
		p3.firstname = "arya";
		p3.lastname = "stark";

		template.save(p1);
		template.save(p2);
		template.save(p3);
	}

	@Document(collection = "dramatis-personae")
	static class Person {

		@Id String id;
		String firstname;

		@Field("last_name") String lastname;

		@Override
		public String toString() {
			return "Person [id=" + id + ", firstname=" + firstname + ", lastname=" + lastname + "]";
		}
	}
}
