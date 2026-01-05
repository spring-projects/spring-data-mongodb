/*
 * Copyright 2022-present the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.springframework.data.mongodb.core.CollectionOptions.EncryptedFieldsOptions;
import static org.springframework.data.mongodb.core.CollectionOptions.TimeSeriesOptions;
import static org.springframework.data.mongodb.core.CollectionOptions.emitChangedRevisions;
import static org.springframework.data.mongodb.core.CollectionOptions.empty;
import static org.springframework.data.mongodb.core.CollectionOptions.encryptedCollection;
import static org.springframework.data.mongodb.core.schema.JsonSchemaProperty.int32;
import static org.springframework.data.mongodb.core.schema.JsonSchemaProperty.queryable;

import java.time.Duration;
import java.util.List;

import org.bson.BsonNull;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.schema.JsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.mongodb.core.schema.QueryCharacteristics;
import org.springframework.data.mongodb.core.timeseries.Granularity;
import org.springframework.data.mongodb.core.timeseries.Span;
import org.springframework.data.mongodb.core.validation.Validator;

/**
 * @author Christoph Strobl
 */
class CollectionOptionsUnitTests {

	@Test // GH-4210
	void emptyEquals() {
		assertThat(empty()).isEqualTo(empty());
	}

	@Test // GH-4210
	void collectionProperties() {
		assertThat(empty().maxDocuments(10).size(1).disableValidation())
				.isEqualTo(empty().maxDocuments(10).size(1).disableValidation());
	}

	@Test // GH-4210
	void changedRevisionsEquals() {
		assertThat(emitChangedRevisions()).isNotEqualTo(empty()).isEqualTo(emitChangedRevisions());
	}

	@Test // GH-4210
	void cappedEquals() {
		assertThat(empty().capped()).isNotEqualTo(empty()).isEqualTo(empty().capped());
	}

	@Test // GH-4210
	void collationEquals() {

		assertThat(empty().collation(Collation.of("en_US"))) //
				.isEqualTo(empty().collation(Collation.of("en_US"))) //
				.isNotEqualTo(empty()) //
				.isNotEqualTo(empty().collation(Collation.of("de_AT")));
	}

	@Test // GH-4210
	void timeSeriesEquals() {

		assertThat(empty().timeSeries(TimeSeriesOptions.timeSeries("tf"))) //
				.isEqualTo(empty().timeSeries(TimeSeriesOptions.timeSeries("tf"))) //
				.isNotEqualTo(empty()) //
				.isNotEqualTo(empty().timeSeries(TimeSeriesOptions.timeSeries("other")));
	}

	@Test // GH-4985
	void timeSeriesValidatesGranularityAndSpanSettings() {

		assertThatNoException().isThrownBy(() -> empty().timeSeries(TimeSeriesOptions.timeSeries("tf").span(Span.of(Duration.ofSeconds(1))).granularity(Granularity.DEFAULT)));
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> TimeSeriesOptions.timeSeries("tf").granularity(Granularity.HOURS).span(Span.of(Duration.ofSeconds(1))));
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> TimeSeriesOptions.timeSeries("tf").span(Span.of(Duration.ofSeconds(1))).granularity(Granularity.HOURS));
	}

	@Test // GH-4210
	void validatorEquals() {

		assertThat(empty().validator(Validator.document(new Document("one", "two")))) //
				.isEqualTo(empty().validator(Validator.document(new Document("one", "two")))) //
				.isNotEqualTo(empty()) //
				.isNotEqualTo(empty().validator(Validator.document(new Document("three", "four"))))
				.isNotEqualTo(empty().validator(Validator.document(new Document("one", "two"))).moderateValidation());
	}

	@Test // GH-4185, GH-4988
	@SuppressWarnings("unchecked")
	void queryableEncryptionOptionsFromSchemaRenderCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder()
				.property(JsonSchemaProperty.object("spring")
						.properties(queryable(JsonSchemaProperty.encrypted(JsonSchemaProperty.int32("data")), List.of())))
				.property(queryable(JsonSchemaProperty.encrypted(JsonSchemaProperty.int64("mongodb")), List.of()))
				.property(JsonSchemaProperty.encrypted(JsonSchemaProperty.string("rocks"))).build();

		EncryptedFieldsOptions encryptionOptions = EncryptedFieldsOptions.fromSchema(schema);

		assertThat(encryptionOptions.toDocument().get("fields", List.class)).hasSize(3)
				.contains(new Document("path", "mongodb").append("bsonType", "long").append("queries", List.of())
						.append("keyId", BsonNull.VALUE))
				.contains(new Document("path", "spring.data").append("bsonType", "int").append("queries", List.of())
						.append("keyId", BsonNull.VALUE))
				.contains(new Document("path", "rocks").append("bsonType", "string").append("keyId", BsonNull.VALUE));

	}

	@Test // GH-4185
	@SuppressWarnings("unchecked")
	void queryableEncryptionPropertiesOverrideByPath() {

		CollectionOptions collectionOptions = encryptedCollection(options -> options //
				.queryable(JsonSchemaProperty.encrypted(JsonSchemaProperty.int32("spring")))
				.queryable(JsonSchemaProperty.encrypted(JsonSchemaProperty.int64("data")))

				// override first with data type long
				.queryable(JsonSchemaProperty.encrypted(JsonSchemaProperty.int64("spring"))));

		assertThat(collectionOptions.getEncryptedFieldsOptions()).map(EncryptedFieldsOptions::toDocument)
				.hasValueSatisfying(it -> {
					assertThat(it.get("fields", List.class)).hasSize(2).contains(new Document("path", "spring")
							.append("bsonType", "long").append("queries", List.of()).append("keyId", BsonNull.VALUE));
				});
	}

	@Test // GH-4185
	@SuppressWarnings("unchecked")
	void queryableEncryptionPropertiesOverridesPathFromSchema() {

		EncryptedFieldsOptions encryptionOptions = EncryptedFieldsOptions.fromSchema(MongoJsonSchema.builder()
				.property(queryable(JsonSchemaProperty.encrypted(JsonSchemaProperty.int32("spring")), List.of()))
				.property(queryable(JsonSchemaProperty.encrypted(JsonSchemaProperty.int64("data")), List.of())).build());

		// override spring from schema with data type long
		CollectionOptions collectionOptions = CollectionOptions.encryptedCollection(
				encryptionOptions.queryable(JsonSchemaProperty.encrypted(JsonSchemaProperty.int64("spring"))));

		assertThat(collectionOptions.getEncryptedFieldsOptions()).map(EncryptedFieldsOptions::toDocument)
				.hasValueSatisfying(it -> {
					assertThat(it.get("fields", List.class)).hasSize(2).contains(new Document("path", "spring")
							.append("bsonType", "long").append("queries", List.of()).append("keyId", BsonNull.VALUE));
				});
	}

	@Test // GH-4185
	void encryptionOptionsAreImmutable() {

		EncryptedFieldsOptions source = EncryptedFieldsOptions
				.fromProperties(List.of(queryable(int32("spring.data"), List.of(QueryCharacteristics.range().min(1)))));

		assertThat(source.queryable(queryable(int32("mongodb"), List.of(QueryCharacteristics.range().min(1)))))
				.isNotSameAs(source).satisfies(it -> {
					assertThat(it.toDocument().get("fields", List.class)).hasSize(2);
				});

		assertThat(source.toDocument().get("fields", List.class)).hasSize(1);
	}

	@Test // GH-4185
	@SuppressWarnings("unchecked")
	void queryableEncryptionPropertiesOverridesNestedPathFromSchema() {

		EncryptedFieldsOptions encryptionOptions = EncryptedFieldsOptions.fromSchema(MongoJsonSchema.builder()
				.property(JsonSchemaProperty.object("spring")
						.properties(queryable(JsonSchemaProperty.encrypted(JsonSchemaProperty.int32("data")), List.of())))
				.property(queryable(JsonSchemaProperty.encrypted(JsonSchemaProperty.int64("mongodb")), List.of())).build());

		// override spring from schema with data type long
		CollectionOptions collectionOptions = CollectionOptions.encryptedCollection(
				encryptionOptions.queryable(JsonSchemaProperty.encrypted(JsonSchemaProperty.int64("spring.data"))));

		assertThat(collectionOptions.getEncryptedFieldsOptions()).map(EncryptedFieldsOptions::toDocument)
				.hasValueSatisfying(it -> {
					assertThat(it.get("fields", List.class)).hasSize(2).contains(new Document("path", "spring.data")
							.append("bsonType", "long").append("queries", List.of()).append("keyId", BsonNull.VALUE));
				});
	}
}
