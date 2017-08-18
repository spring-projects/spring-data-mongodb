/*
 * Copyright 2016-2017 the original author or authors.
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

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.net.UnknownHostException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.MongoClient;

/**
 * Integration tests for Query-by-example.
 * 
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Oliver Gierke
 */
public class QueryByExampleTests {

	MongoOperations operations;
	Person p1, p2, p3;

	@Before
	public void setUp() throws UnknownHostException {

		operations = new MongoTemplate(new MongoClient(), "query-by-example");
		operations.remove(new Query(), Person.class);

		p1 = new Person();
		p1.firstname = "bran";
		p1.middlename = "a";
		p1.lastname = "stark";

		p2 = new Person();
		p2.firstname = "jon";
		p2.lastname = "snow";

		p3 = new Person();
		p3.firstname = "arya";
		p3.lastname = "stark";

		operations.save(p1);
		operations.save(p2);
		operations.save(p3);
	}

	@Test // DATAMONGO-1245
	public void findByExampleShouldWorkForSimpleProperty() {

		Person sample = new Person();
		sample.lastname = "stark";

		Query query = new Query(new Criteria().alike(Example.of(sample)));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result, hasSize(2));
		assertThat(result, hasItems(p1, p3));
	}

	@Test // DATAMONGO-1245
	public void findByExampleShouldWorkForMultipleProperties() {

		Person sample = new Person();
		sample.lastname = "stark";
		sample.firstname = "arya";

		Query query = new Query(new Criteria().alike(Example.of(sample)));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result, hasSize(1));
		assertThat(result, hasItem(p3));
	}

	@Test // DATAMONGO-1245
	public void findByExampleShouldWorkForIdProperty() {

		Person p4 = new Person();
		operations.save(p4);

		Person sample = new Person();
		sample.id = p4.id;

		Query query = new Query(new Criteria().alike(Example.of(sample)));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result, hasSize(1));
		assertThat(result, hasItem(p4));
	}

	@Test // DATAMONGO-1245
	public void findByExampleShouldReturnEmptyListIfNotMatching() {

		Person sample = new Person();
		sample.firstname = "jon";
		sample.firstname = "stark";

		Query query = new Query(new Criteria().alike(Example.of(sample)));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result, is(empty()));
	}

	@Test // DATAMONGO-1245
	public void findByExampleShouldReturnEverythingWhenSampleIsEmpty() {

		Person sample = new Person();

		Query query = new Query(new Criteria().alike(Example.of(sample)));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result, hasSize(3));
		assertThat(result, hasItems(p1, p2, p3));
	}

	@Test // DATAMONGO-1245
	public void findByExampleWithCriteria() {

		Person sample = new Person();
		sample.lastname = "stark";

		Query query = new Query(new Criteria().alike(Example.of(sample)).and("firstname").regex("^ary*"));

		List<Person> result = operations.find(query, Person.class);
		assertThat(result.size(), is(1));
	}

	@Test // DATAMONGO-1459
	public void findsExampleUsingAnyMatch() {

		Person probe = new Person();
		probe.lastname = "snow";
		probe.middlename = "a";

		Query query = Query.query(Criteria.byExample(Example.of(probe, ExampleMatcher.matchingAny())));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result, hasSize(2));
		assertThat(result, hasItems(p1, p2));
	}

	@Test // DATAMONGO-1768
	public void typedExampleMatchesNothingIfTypesDoNotMatch() {

		NotAPersonButStillMatchingFields probe = new NotAPersonButStillMatchingFields();
		probe.lastname = "stark";

		Query query = new Query(new Criteria().alike(Example.of(probe)));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result, hasSize(0));
	}

	@Test // DATAMONGO-1768
	public void untypedExampleMatchesCorrectly() {

		NotAPersonButStillMatchingFields probe = new NotAPersonButStillMatchingFields();
		probe.lastname = "stark";

		Query query = new Query(
				new Criteria().alike(Example.of(probe, ExampleMatcher.matching().withIgnorePaths("_class"))));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result, hasSize(2));
		assertThat(result, hasItems(p1, p3));
	}

	@Document(collection = "dramatis-personae")
	@EqualsAndHashCode
	@ToString
	static class Person {

		@Id String id;
		String firstname, middlename;
		@Field("last_name") String lastname;
	}

	@EqualsAndHashCode
	@ToString
	static class NotAPersonButStillMatchingFields {

		String firstname, middlename;
		@Field("last_name") String lastname;
	}
}
