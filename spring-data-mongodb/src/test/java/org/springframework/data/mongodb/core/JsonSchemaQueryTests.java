/*
 * Copyright 2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.core.schema.JsonSchemaProperty.*;

import lombok.Data;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.util.Version;

import com.mongodb.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * @author Christoph Strobl
 */
public class JsonSchemaQueryTests {

	public static final String DATABASE_NAME = "json-schema-query-tests";

	public static @ClassRule MongoVersionRule REQUIRES_AT_LEAST_3_6_0 = MongoVersionRule.atLeast(Version.parse("3.6.0"));

	MongoTemplate template;
	Person jellyBelly, roseSpringHeart, kazmardBoombub;

	@Before
	public void setUp() {

		template = new MongoTemplate(new MongoClient(), DATABASE_NAME);

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
	public void findsDocumentsWithRequiredFieldsCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder().required("address").build();

		assertThat(template.find(query(matchingDocumentStructure(schema)), Person.class))
				.containsExactlyInAnyOrder(jellyBelly, roseSpringHeart);
	}

	@Test // DATAMONGO-1835
	public void findsDocumentsWithRequiredFieldsReactively() {

		MongoJsonSchema schema = MongoJsonSchema.builder().required("address").build();

		StepVerifier.create(new ReactiveMongoTemplate(MongoClients.create(), DATABASE_NAME)
				.find(query(matchingDocumentStructure(schema)), Person.class)).expectNextCount(2).verifyComplete();
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

	@Data
	static class Person {

		@Id String id;

		@Field("full_name") String name;
		Gender gender;
		Address address;
		Object value;
	}

	@Data
	static class Address {

		String city;

		@Field("str") String street;
	}

	static enum Gender {
		PIXY, UNICORN, GOBLIN
	}

}
