/*
 * Copyright 2016-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UntypedExampleMatcher;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

/**
 * Integration tests for Query-by-example.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Oliver Gierke
 */
@ExtendWith(MongoTemplateExtension.class)
public class QueryByExampleTests {

	@Template(initialEntitySet = Person.class) //
	static MongoTestTemplate operations;

	Person p1, p2, p3;

	@BeforeEach
	public void setUp() {

		operations.flush();

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

		assertThat(result).containsExactlyInAnyOrder(p1, p3);
	}

	@Test // DATAMONGO-1245
	public void findByExampleShouldWorkForMultipleProperties() {

		Person sample = new Person();
		sample.lastname = "stark";
		sample.firstname = "arya";

		Query query = new Query(new Criteria().alike(Example.of(sample)));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result).containsExactly(p3);
	}

	@Test // DATAMONGO-1245
	public void findByExampleShouldWorkForIdProperty() {

		Person p4 = new Person();
		operations.save(p4);

		Person sample = new Person();
		sample.id = p4.id;

		Query query = new Query(new Criteria().alike(Example.of(sample)));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result).containsExactly(p4);
	}

	@Test // DATAMONGO-1245
	public void findByExampleShouldReturnEmptyListIfNotMatching() {

		Person sample = new Person();
		sample.firstname = "jon";
		sample.firstname = "stark";

		Query query = new Query(new Criteria().alike(Example.of(sample)));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result).isEmpty();
	}

	@Test // DATAMONGO-1245
	public void findByExampleShouldReturnEverythingWhenSampleIsEmpty() {

		Person sample = new Person();

		Query query = new Query(new Criteria().alike(Example.of(sample)));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result).containsExactlyInAnyOrder(p1, p2, p3);
	}

	@Test // DATAMONGO-1245, GH-3544
	public void findByExampleWithCriteria() {

		Person sample = new Person();
		sample.lastname = "stark";

		Query query = new Query(new Criteria().alike(Example.of(sample)).and("firstname").regex(".*n.*"));
		assertThat(operations.find(query, Person.class)).containsExactly(p1);
	}

	@Test // DATAMONGO-1459
	public void findsExampleUsingAnyMatch() {

		Person probe = new Person();
		probe.lastname = "snow";
		probe.middlename = "a";

		Query query = Query.query(Criteria.byExample(Example.of(probe, ExampleMatcher.matchingAny())));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result).containsExactlyInAnyOrder(p1, p2);
	}

	@Test // DATAMONGO-1768
	public void typedExampleMatchesNothingIfTypesDoNotMatch() {

		NotAPersonButStillMatchingFields probe = new NotAPersonButStillMatchingFields();
		probe.lastname = "stark";

		Query query = new Query(new Criteria().alike(Example.of(probe)));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result).isEmpty();
	}

	@Test // DATAMONGO-1768
	public void exampleIgnoringClassTypeKeyMatchesCorrectly() {

		NotAPersonButStillMatchingFields probe = new NotAPersonButStillMatchingFields();
		probe.lastname = "stark";

		Query query = new Query(
				new Criteria().alike(Example.of(probe, ExampleMatcher.matching().withIgnorePaths("_class"))));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result).containsExactlyInAnyOrder(p1, p3);
	}

	@Test // DATAMONGO-1768
	public void untypedExampleMatchesCorrectly() {

		NotAPersonButStillMatchingFields probe = new NotAPersonButStillMatchingFields();
		probe.lastname = "stark";

		Query query = new Query(new Criteria().alike(Example.of(probe, UntypedExampleMatcher.matching())));
		List<Person> result = operations.find(query, Person.class);

		assertThat(result).containsExactlyInAnyOrder(p1, p3);
	}

	@Test // DATAMONGO-2314
	public void alikeShouldWorkOnNestedProperties() {

		PersonWrapper source1 = new PersonWrapper();
		source1.id = "with-child-doc-1";
		source1.child = p1;

		PersonWrapper source2 = new PersonWrapper();
		source2.id = "with-child-doc-2";
		source2.child = p2;

		operations.save(source1);
		operations.save(source2);

		Query query = new Query(
				new Criteria("child").alike(Example.of(p1, ExampleMatcher.matching().withIgnorePaths("_class"))));
		List<PersonWrapper> result = operations.find(query, PersonWrapper.class);

		assertThat(result).containsExactly(source1);
	}

	@Document("dramatis-personae")
	static class Person {

		@Id String id;
		String firstname, middlename;
		@Field("last_name") String lastname;

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Person person = (Person) o;
			return Objects.equals(id, person.id) && Objects.equals(firstname, person.firstname)
					&& Objects.equals(middlename, person.middlename) && Objects.equals(lastname, person.lastname);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, firstname, middlename, lastname);
		}

		public String toString() {
			return "QueryByExampleTests.Person(id=" + this.id + ", firstname=" + this.firstname + ", middlename="
					+ this.middlename + ", lastname=" + this.lastname + ")";
		}
	}

	static class NotAPersonButStillMatchingFields {

		String firstname, middlename;
		@Field("last_name") String lastname;

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			NotAPersonButStillMatchingFields that = (NotAPersonButStillMatchingFields) o;
			return Objects.equals(firstname, that.firstname) && Objects.equals(middlename, that.middlename)
					&& Objects.equals(lastname, that.lastname);
		}

		@Override
		public int hashCode() {
			return Objects.hash(firstname, middlename, lastname);
		}

		public String toString() {
			return "QueryByExampleTests.NotAPersonButStillMatchingFields(firstname=" + this.firstname + ", middlename="
					+ this.middlename + ", lastname=" + this.lastname + ")";
		}
	}

	@Document("dramatis-personae")
	static class PersonWrapper {

		@Id String id;
		Person child;

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			PersonWrapper that = (PersonWrapper) o;
			return Objects.equals(id, that.id) && Objects.equals(child, that.child);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, child);
		}

		public String toString() {
			return "QueryByExampleTests.PersonWrapper(id=" + this.id + ", child=" + this.child + ")";
		}
	}
}
