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
package org.springframework.data.mongodb.core.schema;

import static org.springframework.data.mongodb.test.util.Assertions.*;

import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoJsonSchemaMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.ReactiveMongoClientClosingTestConfiguration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Integration tests for {@link MongoJsonSchema} using reactive infrastructure.
 *
 * @author Mark Paluch
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
@ContextConfiguration
public class ReactiveMongoJsonSchemaTests {

	static @Client MongoClient mongoClient;

	@Configuration
	static class Config extends ReactiveMongoClientClosingTestConfiguration {

		@Override
		public MongoClient reactiveMongoClient() {
			return mongoClient;
		}

		@Override
		protected String getDatabaseName() {
			return "json-schema-tests";
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Collections.emptySet();
		}
	}

	@Autowired ReactiveMongoTemplate template;

	@BeforeEach
	public void setUp() {
		template.dropCollection(Person.class).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1835
	public void writeSchemaViaTemplate() {

		MongoJsonSchema schema = MongoJsonSchema.builder() //
				.required("firstname", "lastname") //
				.properties( //
						JsonSchemaProperty.string("firstname").possibleValues("luke", "han").maxLength(10), //
						JsonSchemaProperty.object("address") //
								.properties(JsonSchemaProperty.string("postCode").minLength(4).maxLength(5))

				).build();

		template.createCollection(Person.class, CollectionOptions.empty().schema(schema)).as(StepVerifier::create)
				.expectNextCount(1).verifyComplete();

		Document $jsonSchema = new MongoJsonSchemaMapper(template.getConverter()).mapSchema(schema.toDocument(),
				Person.class);

		Document fromDb = readSchemaFromDatabase("persons");
		assertThat(fromDb).isEqualTo($jsonSchema);
	}

	Document readSchemaFromDatabase(String collectionName) {

		Document collectionInfo = template
				.executeCommand(new Document("listCollections", 1).append("filter", new Document("name", collectionName)))
				.block(Duration.ofSeconds(5));

		if (collectionInfo == null) {
			throw new DataRetrievalFailureException(String.format("Collection %s was not found.", collectionName));
		}

		if (collectionInfo.containsKey("cursor")) {
			collectionInfo = (Document) collectionInfo.get("cursor", Document.class).get("firstBatch", List.class).iterator()
					.next();
		}

		if (!collectionInfo.containsKey("options")) {
			return new Document();
		}

		return collectionInfo.get("options", Document.class).get("validator", Document.class);
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = "persons")
	static class Person {

		@Field("first_name") String firstname;
		String lastname;
		Address address;

		public String getFirstname() {
			return this.firstname;
		}

		public String getLastname() {
			return this.lastname;
		}

		public Address getAddress() {
			return this.address;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public void setAddress(Address address) {
			this.address = address;
		}

		public String toString() {
			return "ReactiveMongoJsonSchemaTests.Person(firstname=" + this.getFirstname() + ", lastname=" + this.getLastname()
					+ ", address=" + this.getAddress() + ")";
		}
	}

	static class Address {

		String city;
		String street;

		@Field("post_code") String postCode;
	}
}
