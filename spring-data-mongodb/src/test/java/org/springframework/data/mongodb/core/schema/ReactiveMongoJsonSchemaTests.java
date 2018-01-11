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
package org.springframework.data.mongodb.core.schema;

import static org.springframework.data.mongodb.test.util.Assertions.*;

import lombok.Data;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import org.bson.Document;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoJsonSchemaMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.util.Version;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * Integration tests for {@link MongoJsonSchema} using reactive infrastructure.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ReactiveMongoJsonSchemaTests {

	public static @ClassRule MongoVersionRule REQUIRES_AT_LEAST_3_6_0 = MongoVersionRule.atLeast(Version.parse("3.6.0"));

	@Configuration
	static class Config extends AbstractReactiveMongoConfiguration {

		@Override
		public MongoClient reactiveMongoClient() {
			return MongoClients.create();
		}

		@Override
		protected String getDatabaseName() {
			return "json-schema-tests";
		}
	}

	@Autowired ReactiveMongoTemplate template;

	@Before
	public void setUp() {
		StepVerifier.create(template.dropCollection(Person.class)).verifyComplete();
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

		StepVerifier.create(template.createCollection(Person.class, CollectionOptions.empty().schema(schema)))
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

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = "persons")
	static class Person {

		@Field("first_name") String firstname;
		String lastname;
		Address address;

	}

	static class Address {

		String city;
		String street;

		@Field("post_code") String postCode;
	}
}
