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

import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoJsonSchemaMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.validation.Validator;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;
import com.mongodb.client.model.ValidationOptions;

/**
 * Integration tests for {@link MongoJsonSchema}.
 *
 * @author Christoph Strobl
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
@ContextConfiguration
public class MongoJsonSchemaTests {

	static @Client MongoClient mongoClient;

	@Configuration
	static class Config extends AbstractMongoClientConfiguration {

		@Override
		public MongoClient mongoClient() {
			return mongoClient;
		}

		@Override
		protected String getDatabaseName() {
			return "json-schema-tests";
		}

	}

	@Autowired MongoTemplate template;

	@BeforeEach
	public void setUp() {

		template.dropCollection(Person.class);
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

		template.createCollection(Person.class, CollectionOptions.empty().schema(schema));

		Document $jsonSchema = new MongoJsonSchemaMapper(template.getConverter()).mapSchema(schema.toDocument(),
				Person.class);

		Document fromDb = readSchemaFromDatabase("persons");
		assertThat(fromDb).isEqualTo($jsonSchema);
	}

	@Test // DATAMONGO-1835
	public void writeSchemaInDocumentValidatorCorrectly() {

		Document unmappedSchema = new Document("$jsonSchema",
				new Document("type", "object").append("required", Collections.singletonList("firstname")));

		Document mappedSchema = new Document("$jsonSchema",
				new Document("type", "object").append("required", Collections.singletonList("first_name")));

		template.createCollection(Person.class, CollectionOptions.empty().validator(Validator.document(unmappedSchema)));

		assertThat(readSchemaFromDatabase("persons")).isEqualTo(mappedSchema);
	}

	@Test // DATAMONGO-1835
	public void nonMappedSchema() {

		MongoJsonSchema schema = MongoJsonSchema.builder() //
				.required("firstname", "lastname") //
				.properties( //
						JsonSchemaProperty.string("firstname").possibleValues("luke", "han").maxLength(10), //
						JsonSchemaProperty.object("address") //
								.properties(JsonSchemaProperty.string("postCode").minLength(4).maxLength(5))

				).build();

		template.createCollection("persons", CollectionOptions.empty().schema(schema));

		Document fromDb = readSchemaFromDatabase("persons");
		assertThat(fromDb)
				.isNotEqualTo(new MongoJsonSchemaMapper(template.getConverter()).mapSchema(schema.toDocument(), Person.class));
	}

	@Test // DATAMONGO-1835
	public void writeSchemaManually() {

		MongoJsonSchema schema = MongoJsonSchema.builder() //
				.required("firstname", "lastname") //
				.properties( //
						JsonSchemaProperty.string("firstname").possibleValues("luke", "han").maxLength(10), //
						JsonSchemaProperty.object("address") //
								.properties(JsonSchemaProperty.string("postCode").minLength(4).maxLength(5))

				).build();

		Document $jsonSchema = new MongoJsonSchemaMapper(template.getConverter()).mapSchema(schema.toDocument(),
				Person.class);

		ValidationOptions options = new ValidationOptions();
		options.validationLevel(ValidationLevel.MODERATE);
		options.validationAction(ValidationAction.ERROR);
		options.validator($jsonSchema);

		CreateCollectionOptions cco = new CreateCollectionOptions();
		cco.validationOptions(options);

		MongoDatabase db = template.getDb();
		db.createCollection("persons", cco);

		Document fromDb = readSchemaFromDatabase("persons");
		assertThat(fromDb).isEqualTo($jsonSchema);
	}

	Document readSchemaFromDatabase(String collectionName) {

		Document collectionInfo = template
				.executeCommand(new Document("listCollections", 1).append("filter", new Document("name", collectionName)));

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
			return "MongoJsonSchemaTests.Person(firstname=" + this.getFirstname() + ", lastname=" + this.getLastname()
					+ ", address=" + this.getAddress() + ")";
		}
	}

	static class Address {

		String city;
		String street;

		@Field("post_code") String postCode;
	}
}
