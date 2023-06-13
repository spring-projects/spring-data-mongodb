/*
 * Copyright 2018-2023 the original author or authors.
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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.core.schema.JsonSchemaProperty.*;

import reactor.test.StepVerifier;

import java.util.Objects;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.schema.JsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MongoTemplateExtension.class)
public class JsonSchemaQueryTests {

	public static final String DATABASE_NAME = "json-schema-query-tests";

	static @Client com.mongodb.reactivestreams.client.MongoClient reactiveClient;

	@Template(database = DATABASE_NAME, initialEntitySet = Person.class) //
	static MongoTestTemplate template;

	Person jellyBelly, roseSpringHeart, kazmardBoombub;

	@BeforeEach
	public void setUp() {

		template.flush();

		jellyBelly = new Person();
		jellyBelly.id = "1";
		jellyBelly.name = "Jelly Belly";
		jellyBelly.gender = Gender.PIXY;
		jellyBelly.address = new Address();
		jellyBelly.address.city = "Candy Hill";
		jellyBelly.address.street = "Apple Mint Street";
		jellyBelly.value = 42;

		roseSpringHeart = new Person();
		roseSpringHeart.id = "2";
		roseSpringHeart.name = "Rose SpringHeart";
		roseSpringHeart.gender = Gender.UNICORN;
		roseSpringHeart.address = new Address();
		roseSpringHeart.address.city = "Rainbow Valley";
		roseSpringHeart.address.street = "Twinkle Ave.";
		roseSpringHeart.value = 42L;

		kazmardBoombub = new Person();
		kazmardBoombub.id = "3";
		kazmardBoombub.name = "Kazmard Boombub";
		kazmardBoombub.gender = Gender.GOBLIN;
		kazmardBoombub.value = "green";

		template.save(jellyBelly);
		template.save(roseSpringHeart);
		template.save(kazmardBoombub);

	}

	@Test // DATAMONGO-1835
	public void createsWorkingSchema() {

		try {
			template.dropCollection("person_schema");
		} catch (Exception e) {}

		MongoJsonSchema schema = MongoJsonSchemaCreator.create(template.getConverter()).createSchemaFor(Person.class);

		template.createCollection("person_schema", CollectionOptions.empty().schema(schema));
	}

	@Test // DATAMONGO-1835
	public void queriesBooleanType() {

		MongoJsonSchema schema = MongoJsonSchema.builder().properties(JsonSchemaProperty.bool("alive")).build();

		assertThat(template.find(query(matchingDocumentStructure(schema)), Person.class)).hasSize(3);
		assertThat(template.find(query(Criteria.where("alive").type(Type.BOOLEAN)), Person.class)).hasSize(3);
	}

	@Test // DATAMONGO-1835
	public void findsDocumentsWithRequiredFieldsCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder().required("address").build();

		assertThat(template.find(query(matchingDocumentStructure(schema)), Person.class))
				.containsExactlyInAnyOrder(jellyBelly, roseSpringHeart);
	}

	@Test // DATAMONGO-1835
	public void findsDocumentsWithRequiredFieldsReactively() {

		MongoJsonSchema schema = MongoJsonSchema.builder().required("address").build();

		new ReactiveMongoTemplate(reactiveClient, DATABASE_NAME)
				.find(query(matchingDocumentStructure(schema)), Person.class).as(StepVerifier::create).expectNextCount(2)
				.verifyComplete();
	}

	@Test // DATAMONGO-1835
	public void findsDocumentsWithBsonFieldTypesCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder().property(int32("value")).build();

		assertThat(template.find(query(matchingDocumentStructure(schema)), Person.class))
				.containsExactlyInAnyOrder(jellyBelly);
	}

	@Test // DATAMONGO-1835
	public void findsDocumentsWithJsonFieldTypesCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder().property(number("value")).build();

		assertThat(template.find(query(matchingDocumentStructure(schema)), Person.class))
				.containsExactlyInAnyOrder(jellyBelly, roseSpringHeart);
	}

	@Test // DATAMONGO-1835
	public void combineSchemaWithOtherCriteria() {

		MongoJsonSchema schema = MongoJsonSchema.builder().property(number("value")).build();

		assertThat(
				template.find(query(matchingDocumentStructure(schema).and("name").is(roseSpringHeart.name)), Person.class))
						.containsExactlyInAnyOrder(roseSpringHeart);
	}

	@Test // DATAMONGO-1835
	public void usesMappedFieldNameForRequiredProperties() {

		MongoJsonSchema schema = MongoJsonSchema.builder().required("name").build();

		assertThat(template.find(query(matchingDocumentStructure(schema)), Person.class))
				.containsExactlyInAnyOrder(jellyBelly, roseSpringHeart, kazmardBoombub);
	}

	@Test // DATAMONGO-1835
	public void usesMappedFieldNameForProperties() {

		MongoJsonSchema schema = MongoJsonSchema.builder().property(string("name").matching("^R.*")).build();

		assertThat(template.find(query(matchingDocumentStructure(schema)), Person.class))
				.containsExactlyInAnyOrder(roseSpringHeart);
	}

	@Test // DATAMONGO-1835
	public void mapsNestedFieldName() {

		MongoJsonSchema schema = MongoJsonSchema.builder() //
				.required("address") //
				.property(object("address").properties(string("street").matching("^Apple.*"))).build();

		assertThat(template.find(query(matchingDocumentStructure(schema)), Person.class))
				.containsExactlyInAnyOrder(jellyBelly);
	}

	@Test // DATAMONGO-1835
	public void mapsEnumValuesCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder()
				.property(untyped("gender").possibleValues(Gender.PIXY, Gender.GOBLIN)).build();

		assertThat(template.find(query(matchingDocumentStructure(schema)), Person.class))
				.containsExactlyInAnyOrder(jellyBelly, kazmardBoombub);
	}

	@Test // DATAMONGO-1835
	public void useTypeOperatorOnFieldLevel() {
		assertThat(template.find(query(where("value").type(Type.intType())), Person.class)).containsExactly(jellyBelly);
	}

	@Test // DATAMONGO-1835
	public void useTypeOperatorWithMultipleTypesOnFieldLevel() {

		assertThat(template.find(query(where("value").type(Type.intType(), Type.stringType())), Person.class))
				.containsExactlyInAnyOrder(jellyBelly, kazmardBoombub);
	}

	@Test // DATAMONGO-1835
	public void findsWithSchemaReturningRawDocument() {

		MongoJsonSchema schema = MongoJsonSchema.builder().required("address").build();

		assertThat(template.find(query(matchingDocumentStructure(schema)), Document.class,
				template.getCollectionName(Person.class))).hasSize(2);
	}

	static class Person {

		@Id String id;

		@Field("full_name") //
		String name;
		Gender gender;
		Address address;
		Object value;

		boolean alive;

		public String getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public Gender getGender() {
			return this.gender;
		}

		public Address getAddress() {
			return this.address;
		}

		public Object getValue() {
			return this.value;
		}

		public boolean isAlive() {
			return this.alive;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setGender(Gender gender) {
			this.gender = gender;
		}

		public void setAddress(Address address) {
			this.address = address;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public void setAlive(boolean alive) {
			this.alive = alive;
		}

		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Person person = (Person) o;
			return alive == person.alive && Objects.equals(id, person.id) && Objects.equals(name, person.name)
					&& gender == person.gender && Objects.equals(address, person.address) && Objects.equals(value, person.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name, gender, address, value, alive);
		}

		public String toString() {
			return "JsonSchemaQueryTests.Person(id=" + this.getId() + ", name=" + this.getName() + ", gender="
					+ this.getGender() + ", address=" + this.getAddress() + ", value=" + this.getValue() + ", alive="
					+ this.isAlive() + ")";
		}
	}

	static class Address {

		String city;

		@Field("str") //
		String street;

		public String getCity() {
			return this.city;
		}

		public String getStreet() {
			return this.street;
		}

		public void setCity(String city) {
			this.city = city;
		}

		public void setStreet(String street) {
			this.street = street;
		}

		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Address address = (Address) o;
			return Objects.equals(city, address.city) && Objects.equals(street, address.street);
		}

		@Override
		public int hashCode() {
			return Objects.hash(city, street);
		}

		public String toString() {
			return "JsonSchemaQueryTests.Address(city=" + this.getCity() + ", street=" + this.getStreet() + ")";
		}
	}

	static enum Gender {
		PIXY, UNICORN, GOBLIN
	}

}
