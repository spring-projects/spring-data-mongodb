/*
 * Copyright 2018-2025 the original author or authors.
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

import static java.util.Arrays.asList;
import static org.springframework.data.mongodb.core.schema.JsonSchemaProperty.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.schema.IdentifiableJsonSchemaProperty.QueryableJsonSchemaProperty;

/**
 * Unit tests for {@link MongoJsonSchema}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class MongoJsonSchemaUnitTests {

	@Test // DATAMONGO-1835
	void toDocumentRendersSchemaCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder() //
				.required("firstname", "lastname") //
				.build();

		assertThat(schema.toDocument()).isEqualTo(new Document("$jsonSchema",
				new Document("type", "object").append("required", Arrays.asList("firstname", "lastname"))));
	}

	@Test // DATAMONGO-1835
	void rendersDocumentBasedSchemaCorrectly() {

		Document document = MongoJsonSchema.builder() //
				.required("firstname", "lastname") //
				.build().toDocument();

		MongoJsonSchema jsonSchema = MongoJsonSchema.of(document.get("$jsonSchema", Document.class));

		assertThat(jsonSchema.toDocument()).isEqualTo(new Document("$jsonSchema",
				new Document("type", "object").append("required", Arrays.asList("firstname", "lastname"))));
	}

	@Test // DATAMONGO-1849
	void rendersRequiredPropertiesCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder() //
				.required("firstname") //
				.properties( //
						JsonSchemaProperty.required(JsonSchemaProperty.string("lastname")) //
				).build();

		assertThat(schema.toDocument()).isEqualTo(new Document("$jsonSchema",
				new Document("type", "object").append("required", Arrays.asList("firstname", "lastname")).append("properties",
						new Document("lastname", new Document("type", "string")))));
	}

	@Test // DATAMONGO-2306
	void rendersEncryptedPropertyCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder().properties( //
				encrypted(string("ssn")) //
						.aead_aes_256_cbc_hmac_sha_512_deterministic() //
						.keyId("*key0_id") //
		).build();

		assertThat(schema.toDocument()).isEqualTo(new Document("$jsonSchema",
				new Document("type", "object").append("properties",
						new Document("ssn", new Document("encrypt", new Document("keyId", "*key0_id")
								.append("algorithm", "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").append("bsonType", "string"))))));
	}

	@Test // DATAMONGO-2306
	void rendersEncryptedPropertyWithKeyIdCorrectly() {

		UUID uuid = UUID.randomUUID();
		MongoJsonSchema schema = MongoJsonSchema.builder().properties( //
				encrypted(string("ssn")) //
						.aead_aes_256_cbc_hmac_sha_512_deterministic() //
						.keys(uuid) //
		).build();

		assertThat(schema.toDocument()).isEqualTo(new Document("$jsonSchema",
				new Document("type", "object").append("properties",
						new Document("ssn", new Document("encrypt", new Document("keyId", Collections.singletonList(uuid))
								.append("algorithm", "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").append("bsonType", "string"))))));
	}

	@Test // GH-4185
	void rendersQueryablePropertyCorrectly() {

		QueryCharacteristics characteristics = new QueryCharacteristics();
		characteristics.addQuery(QueryCharacteristics.range().contention(0).trimFactor(1).sparsity(1).min(0).max(200));

		QueryableJsonSchemaProperty property = queryable(encrypted(number("mypath")), characteristics);

		//Document document = MongoJsonSchema.builder().property(property).build().schemaDocument();
		Document document = property.toDocument();
		System.out.println(document.toJson(JsonWriterSettings.builder().indent(true).build()));


		BsonDocument encryptedFields = new BsonDocument().append("fields",
			new BsonArray(asList(
				new BsonDocument("keyId", BsonNull.VALUE).append("path", new BsonString("encryptedInt"))
					.append("bsonType", new BsonString("int"))
					.append("queries",
						new BsonDocument("queryType", new BsonString("range")).append("contention", new BsonInt64(0L))
							.append("trimFactor", new BsonInt32(1)).append("sparsity", new BsonInt64(1))
							.append("min", new BsonInt32(0)).append("max", new BsonInt32(200))),
				new BsonDocument("keyId", BsonNull.VALUE).append("path", new BsonString("encryptedLong"))
					.append("bsonType", new BsonString("long")).append("queries",
						new BsonDocument("queryType", new BsonString("range")).append("contention", new BsonInt64(0L))
							.append("trimFactor", new BsonInt32(1)).append("sparsity", new BsonInt64(1))
							.append("min", new BsonInt64(1000)).append("max", new BsonInt64(9999))))));

	}

	@Test // DATAMONGO-1835
	void throwsExceptionOnNullRoot() {
		assertThatIllegalArgumentException().isThrownBy(() -> MongoJsonSchema.of((JsonSchemaObject) null));
	}

	@Test // DATAMONGO-1835
	void throwsExceptionOnNullDocument() {
		assertThatIllegalArgumentException().isThrownBy(() -> MongoJsonSchema.of((Document) null));
	}
}
