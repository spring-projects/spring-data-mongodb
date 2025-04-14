/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.core.encryption;

import static org.springframework.data.mongodb.core.schema.JsonSchemaProperty.*;
import static org.springframework.data.mongodb.core.schema.QueryCharacteristics.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.bson.BsonBinary;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.schema.JsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for creating collections with encrypted fields.
 *
 * @author Christoph Strobl
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
@EnableIfMongoServerVersion(isGreaterThanEqual = "8.0")
@ContextConfiguration
public class MongoQueryableEncryptionCollectionCreationTests {

	public static final String COLLECTION_NAME = "enc-collection";
	static @Client MongoClient mongoClient;

	@Configuration
	static class Config extends AbstractMongoClientConfiguration {

		@Override
		public MongoClient mongoClient() {
			return mongoClient;
		}

		@Override
		protected String getDatabaseName() {
			return "encryption-schema-tests";
		}

	}

	@Autowired MongoTemplate template;

	@BeforeEach
	void beforeEach() {
		template.dropCollection(COLLECTION_NAME);
	}

	@ParameterizedTest // GH-4185
	@MethodSource("collectionOptions")
	public void createsCollectionWithEncryptedFieldsCorrectly(CollectionOptions collectionOptions) {

		template.createCollection(COLLECTION_NAME, collectionOptions);

		Document encryptedFields = readEncryptedFieldsFromDatabase(COLLECTION_NAME);
		assertThat(encryptedFields).containsKey("fields");

		List<Document> fields = encryptedFields.get("fields", List.of());
		assertThat(fields.get(0)).containsEntry("path", "encryptedInt") //
				.containsEntry("bsonType", "int") //
				.containsEntry("queries", List
						.of(Document.parse("{'queryType': 'range', 'contention': { '$numberLong' : '1' }, 'min': 5, 'max': 100}")));

		assertThat(fields.get(1)).containsEntry("path", "nested.encryptedLong") //
				.containsEntry("bsonType", "long") //
				.containsEntry("queries", List.of(Document.parse(
						"{'queryType': 'range', 'contention': { '$numberLong' : '0' }, 'min': { '$numberLong' : '-1' }, 'max': { '$numberLong' : '1' }}")));
	}

	private static Stream<Arguments> collectionOptions() {

		BsonBinary key1 = new BsonBinary(UUID.randomUUID(), UuidRepresentation.STANDARD);
		BsonBinary key2 = new BsonBinary(UUID.randomUUID(), UuidRepresentation.STANDARD);

		CollectionOptions manualOptions = CollectionOptions.encryptedCollection(options -> options //
				.queryable(encrypted(int32("encryptedInt")).keys(key1), range().min(5).max(100).contention(1)) //
				.queryable(encrypted(JsonSchemaProperty.int64("nested.encryptedLong")).keys(key2),
						range().min(-1L).max(1L).contention(0)));

		CollectionOptions schemaOptions = CollectionOptions.encryptedCollection(MongoJsonSchema.builder()
				.property(
						queryable(encrypted(int32("encryptedInt")).keyId(key1), List.of(range().min(5).max(100).contention(1))))
				.property(queryable(encrypted(int64("nested.encryptedLong")).keyId(key2),
						List.of(range().min(-1L).max(1L).contention(0))))
				.build());

		return Stream.of(Arguments.of(manualOptions), Arguments.of(schemaOptions));
	}

	Document readEncryptedFieldsFromDatabase(String collectionName) {

		Document collectionInfo = template
				.executeCommand(new Document("listCollections", 1).append("filter", new Document("name", collectionName)));

		if (collectionInfo.containsKey("cursor")) {
			collectionInfo = (Document) collectionInfo.get("cursor", Document.class).get("firstBatch", List.class).iterator()
					.next();
		}

		if (!collectionInfo.containsKey("options")) {
			return new Document();
		}

		return collectionInfo.get("options", Document.class).get("encryptedFields", Document.class);
	}
}
