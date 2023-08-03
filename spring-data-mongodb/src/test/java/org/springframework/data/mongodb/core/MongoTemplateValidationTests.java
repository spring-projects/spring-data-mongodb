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
import static org.springframework.data.mongodb.core.validation.Validator.*;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.CollectionOptions.ValidationOptions;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.lang.Nullable;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;

/**
 * Integration tests for {@link CollectionOptions#validation(ValidationOptions)} using
 * {@link org.springframework.data.mongodb.core.validation.CriteriaValidator},
 * {@link org.springframework.data.mongodb.core.validation.DocumentValidator} and
 * {@link org.springframework.data.mongodb.core.validation.JsonSchemaValidator}.
 *
 * @author Andreas Zink
 * @author Christoph Strobl
 * @author Julia Lee
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
public class MongoTemplateValidationTests {

	static final String COLLECTION_NAME = "validation-1";
	static @Client MongoClient mongoClient;

	@Configuration
	static class Config extends AbstractMongoClientConfiguration {

		@Override
		public MongoClient mongoClient() {
			return mongoClient;
		}

		@Override
		protected String getDatabaseName() {
			return "validation-tests";
		}

		@Override
		protected boolean autoIndexCreation() {
			return false;
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.emptySet();
		}
	}

	@Autowired MongoTemplate template;

	@BeforeEach
	public void setUp() {
		template.dropCollection(COLLECTION_NAME);
	}

	@Test // DATAMONGO-1322
	public void testCollectionWithSimpleCriteriaBasedValidation() {

		Criteria criteria = where("nonNullString").ne(null).type(2).and("rangedInteger").ne(null).type(16).gte(0).lte(122);

		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().validator(criteria(criteria)));

		Document validator = getValidatorInfo(COLLECTION_NAME);

		assertThat(validator.get("nonNullString")).isEqualTo(new Document("$ne", null).append("$type", 2));
		assertThat(validator.get("rangedInteger"))
				.isEqualTo(new Document("$ne", null).append("$type", 16).append("$gte", 0).append("$lte", 122));

		template.save(new SimpleBean("hello", 101, null), COLLECTION_NAME);

		assertThatExceptionOfType(DataIntegrityViolationException.class)
				.isThrownBy(() -> template.save(new SimpleBean(null, 101, null), COLLECTION_NAME));

		assertThatExceptionOfType(DataIntegrityViolationException.class)
				.isThrownBy(() -> template.save(new SimpleBean("hello", -1, null), COLLECTION_NAME));
	}

	@Test // DATAMONGO-1322
	public void testCollectionValidationActionError() {

		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().schemaValidationAction(ValidationAction.ERROR)
				.validator(criteria(where("name").type(2))));

		assertThat(getValidationActionInfo(COLLECTION_NAME)).isEqualTo(ValidationAction.ERROR.getValue());
	}

	@Test // DATAMONGO-1322
	public void testCollectionValidationActionWarn() {

		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().schemaValidationAction(ValidationAction.WARN)
				.validator(criteria(where("name").type(2))));

		assertThat(getValidationActionInfo(COLLECTION_NAME)).isEqualTo(ValidationAction.WARN.getValue());
	}

	@Test // DATAMONGO-1322
	public void testCollectionValidationLevelOff() {

		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().schemaValidationLevel(ValidationLevel.OFF)
				.validator(criteria(where("name").type(2))));

		assertThat(getValidationLevelInfo(COLLECTION_NAME)).isEqualTo(ValidationLevel.OFF.getValue());
	}

	@Test // DATAMONGO-1322
	public void testCollectionValidationLevelModerate() {

		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().schemaValidationLevel(ValidationLevel.MODERATE)
				.validator(criteria(where("name").type(2))));

		assertThat(getValidationLevelInfo(COLLECTION_NAME)).isEqualTo(ValidationLevel.MODERATE.getValue());
	}

	@Test // DATAMONGO-1322
	public void testCollectionValidationLevelStrict() {

		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().schemaValidationLevel(ValidationLevel.STRICT)
				.validator(criteria(where("name").type(2))));

		assertThat(getValidationLevelInfo(COLLECTION_NAME)).isEqualTo(ValidationLevel.STRICT.getValue());
	}

	@Test // DATAMONGO-1322
	public void mapsFieldNameCorrectlyWhenGivenDomainTypeInformation() {

		template.createCollection(SimpleBean.class,
				CollectionOptions.empty().validator(criteria(where("customFieldName").type(8))));

		assertThat(getValidatorInfo(COLLECTION_NAME)).isEqualTo(new Document("customName", new Document("$type", 8)));
	}

	@Test // DATAMONGO-1322
	public void usesDocumentValidatorCorrectly() {

		Document rules = new Document("customFieldName", new Document("$type", "bool"));

		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().validator(document(rules)));

		assertThat(getValidatorInfo(COLLECTION_NAME))
				.isEqualTo(new Document("customFieldName", new Document("$type", "bool")));
	}

	@Test // DATAMONGO-1322
	public void mapsDocumentValidatorFieldsCorrectly() {

		Document rules = new Document("customFieldName", new Document("$type", "bool"));

		template.createCollection(SimpleBean.class, CollectionOptions.empty().validator(document(rules)));

		assertThat(getValidatorInfo(COLLECTION_NAME)).isEqualTo(new Document("customName", new Document("$type", "bool")));
	}

	@Test // GH-4454
	public void failsJsonSchemaValidationForEncryptedDomainEntityProperty() {

		MongoJsonSchema schema = MongoJsonSchemaCreator.create().createSchemaFor(BeanWithEncryptedDomainEntity.class);
		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().schema(schema));

		BeanWithEncryptedDomainEntity person = new BeanWithEncryptedDomainEntity();
		person.encryptedDomainEntity = new SimpleBean("some string", 100, null);

		assertThatExceptionOfType(DataIntegrityViolationException.class)
			.isThrownBy(() -> template.save(person))
			.withMessageContaining("Document failed validation");
	}

	private Document getCollectionOptions(String collectionName) {
		return getCollectionInfo(collectionName).get("options", Document.class);
	}

	private Document getValidatorInfo(String collectionName) {
		return getCollectionOptions(collectionName).get("validator", Document.class);
	}

	private String getValidationActionInfo(String collectionName) {
		return getCollectionOptions(collectionName).get("validationAction", String.class);
	}

	private String getValidationLevelInfo(String collectionName) {
		return getCollectionOptions(collectionName).get("validationLevel", String.class);
	}

	private Document getCollectionInfo(String collectionName) {

		return template.execute(db -> {
			Document result = db.runCommand(
					new Document().append("listCollections", 1).append("filter", new Document("name", collectionName)));
			return (Document) result.get("cursor", Document.class).get("firstBatch", List.class).get(0);
		});
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = COLLECTION_NAME)
	static class SimpleBean {

		private @Nullable String nonNullString;
		private @Nullable Integer rangedInteger;
		private @Field("customName") Object customFieldName;

		public SimpleBean(@Nullable String nonNullString, @Nullable Integer rangedInteger, Object customFieldName) {
			this.nonNullString = nonNullString;
			this.rangedInteger = rangedInteger;
			this.customFieldName = customFieldName;
		}

		@Nullable
		public String getNonNullString() {
			return this.nonNullString;
		}

		@Nullable
		public Integer getRangedInteger() {
			return this.rangedInteger;
		}

		public Object getCustomFieldName() {
			return this.customFieldName;
		}

		public void setNonNullString(@Nullable String nonNullString) {
			this.nonNullString = nonNullString;
		}

		public void setRangedInteger(@Nullable Integer rangedInteger) {
			this.rangedInteger = rangedInteger;
		}

		public void setCustomFieldName(Object customFieldName) {
			this.customFieldName = customFieldName;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SimpleBean that = (SimpleBean) o;
			return Objects.equals(nonNullString, that.nonNullString) && Objects.equals(rangedInteger, that.rangedInteger) && Objects.equals(customFieldName, that.customFieldName);
		}

		@Override
		public int hashCode() {
			return Objects.hash(nonNullString, rangedInteger, customFieldName);
		}

		public String toString() {
			return "MongoTemplateValidationTests.SimpleBean(nonNullString=" + this.getNonNullString() + ", rangedInteger=" + this.getRangedInteger() + ", customFieldName=" + this.getCustomFieldName() + ")";
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = COLLECTION_NAME)
	@Encrypted(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
	static class BeanWithEncryptedDomainEntity {
		@Encrypted SimpleBean encryptedDomainEntity;
	}
}
