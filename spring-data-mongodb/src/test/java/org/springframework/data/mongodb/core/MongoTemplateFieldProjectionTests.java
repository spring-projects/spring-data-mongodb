/*
 * Copyright 2021-2023 the original author or authors.
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

import java.util.Objects;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.aggregation.AggregationSpELExpression;
import org.springframework.data.mongodb.core.aggregation.StringOperators;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.Unwrapped;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

/**
 * Integration tests for {@link org.springframework.data.mongodb.core.query.Field}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MongoTemplateExtension.class)
@EnableIfMongoServerVersion(isGreaterThanEqual = "4.4")
class MongoTemplateFieldProjectionTests {

	private static @Template MongoTestTemplate template;

	private Person luke;

	@BeforeEach
	void beforeEach() {

		luke = new Person();
		luke.id = "luke";
		luke.firstname = "luke";
		luke.lastname = "skywalker";

		template.save(luke);
	}

	@AfterEach
	void afterEach() {
		template.flush(Person.class, Wrapper.class);
	}

	@Test // GH-3583
	void usesMongoExpressionAsIs() {

		Person result = findLuke(fields -> {
			fields.include("firstname").project(MongoExpression.create("'$toUpper' : '$last_name'"))
					.as("last_name");
		});

		assertThat(result).isEqualTo(luke.upperCaseLastnameClone());
	}

	@Test // GH-3583
	void usesMongoExpressionWithPlaceholdersAsIs() {

		Person result = findLuke(fields -> {
			fields.include("firstname").project(MongoExpression.create("'$toUpper' : '$?0'", "last_name"))
					.as("last_name");
		});

		assertThat(result).isEqualTo(luke.upperCaseLastnameClone());
	}

	@Test // GH-3583
	void mapsAggregationExpressionToDomainType() {

		Person result = findLuke(fields -> {
			fields.include("firstname").project(StringOperators.valueOf("lastname").toUpper()).as("last_name");
		});

		assertThat(result).isEqualTo(luke.upperCaseLastnameClone());
	}

	@Test // GH-3583
	void mapsAggregationSpELExpressionToDomainType() {

		Person result = findLuke(fields -> {
			fields.include("firstname").project(AggregationSpELExpression.expressionOf("toUpper(lastname)")).as("last_name");
		});

		assertThat(result).isEqualTo(luke.upperCaseLastnameClone());
	}

	@Test // GH-3583
	void mapsNestedPathAggregationExpressionToDomainType() {

		Wrapper wrapper = new Wrapper();
		wrapper.id = "wrapper";
		wrapper.person = luke;

		template.save(wrapper);

		Query query = Query.query(Criteria.where("id").is(wrapper.id));
		query.fields().include("person.firstname", "person.id")
				.project(StringOperators.valueOf("person.lastname").toUpper()).as("person.last_name");

		Wrapper result = template.findOne(query, Wrapper.class);
		assertThat(result.person).isEqualTo(luke.upperCaseLastnameClone());
	}

	@Test // GH-3583
	void mapsProjectionOnUnwrapped() {

		luke.address = new Address();
		luke.address.planet = "tatoine";

		template.save(luke);

		Person result = findLuke(fields -> {
			fields.project(StringOperators.valueOf("address.planet").toUpper()).as("planet");
		});

		assertThat(result.address.planet).isEqualTo("TATOINE");
	}

	private Person findLuke(Consumer<org.springframework.data.mongodb.core.query.Field> projection) {

		Query query = Query.query(Criteria.where("id").is(luke.id));
		projection.accept(query.fields());
		return template.findOne(query, Person.class);
	}

	static class Wrapper {

		@Id String id;
		Person person;

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Wrapper wrapper = (Wrapper) o;
			return Objects.equals(id, wrapper.id) && Objects.equals(person, wrapper.person);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, person);
		}

		public String toString() {
			return "MongoTemplateFieldProjectionTests.Wrapper(id=" + this.id + ", person=" + this.person + ")";
		}
	}

	static class Person {

		@Id String id;
		String firstname;

		@Field("last_name") //
		String lastname;

		@Unwrapped.Nullable Address address;

		Person toUpperCaseLastnameClone(Person source) {

			Person target = new Person();
			target.id = source.id;
			target.firstname = source.firstname;
			target.lastname = source.lastname.toUpperCase();
			target.address = source.address;

			return target;
		}

		Person upperCaseLastnameClone() {
			return toUpperCaseLastnameClone(this);
		}

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
					&& Objects.equals(lastname, person.lastname) && Objects.equals(address, person.address);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, firstname, lastname, address);
		}

		public String toString() {
			return "MongoTemplateFieldProjectionTests.Person(id=" + this.id + ", firstname=" + this.firstname + ", lastname="
					+ this.lastname + ", address=" + this.address + ")";
		}
	}

	static class Address {

		String planet;

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Address address = (Address) o;
			return Objects.equals(planet, address.planet);
		}

		@Override
		public int hashCode() {
			return Objects.hash(planet);
		}

		public String toString() {
			return "MongoTemplateFieldProjectionTests.Address(planet=" + this.planet + ")";
		}
	}
}
