/*
 * Copyright 2017 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.validation.CriteriaValidator;
import org.springframework.data.mongodb.core.validation.ValidationAction;
import org.springframework.data.mongodb.core.validation.ValidationLevel;
import org.springframework.data.mongodb.core.validation.ValidationOptions;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.util.Version;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.MongoClient;

/**
 * @author Andreas Zink
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class MongoTemplateValidationTests {

	public static @ClassRule MongoVersionRule REQUIRES_AT_LEAST_3_2_0 = MongoVersionRule.atLeast(Version.parse("3.2.0"));
	public static final String COLLECTION_NAME = "validation-1";

	@Configuration
	static class Config extends AbstractMongoConfiguration {

		@Override
		public MongoClient mongoClient() {
			return new MongoClient();
		}

		@Override
		protected String getDatabaseName() {
			return "validation-tests";
		}
	}

	@Autowired MongoTemplate template;

	@Before
	public void setUp() {
		template.dropCollection(COLLECTION_NAME);
	}

	@Test // DATAMONGO-1322
	public void testCollectionWithSimpleCriteriaBasedValidation() {
		Criteria criteria = Criteria.where("nonNullString").ne(null).type(2).and("rangedInteger").ne(null).type(16).gte(0)
				.lte(122);
		ValidationOptions validationOptions = ValidationOptions.validator(CriteriaValidator.fromCriteria(criteria));
		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().validation(validationOptions));

		Document validator = getValidatorInfo(COLLECTION_NAME);
		assertThat(validator.get("nonNullString")).isEqualTo(new Document("$ne", null).append("$type", 2));
		assertThat(validator.get("rangedInteger"))
				.isEqualTo(new Document("$ne", null).append("$type", 16).append("$gte", 0).append("$lte", 122));

		template.save(new SimpleBean("hello", 101), COLLECTION_NAME);
		try {
			template.save(new SimpleBean(null, 101), COLLECTION_NAME);
			Assert.fail("The collection validation was setup to check for non-null string");
		} catch (Exception e) {
			// ignore
		}
		try {
			template.save(new SimpleBean("hello", -1), COLLECTION_NAME);
			Assert.fail("The collection validation was setup to check for non-negative int");
		} catch (Exception e) {
			// ignore
		}
	}

	@Test // DATAMONGO-1322
	public void testCollectionValidationActionError() {
		Criteria criteria = Criteria.where("name").type(2);
		ValidationOptions validationOptions = ValidationOptions.validator(CriteriaValidator.fromCriteria(criteria))
				.validationAction(ValidationAction.ERROR);
		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().validation(validationOptions));

		String validationAction = getValidationActionInfo(COLLECTION_NAME);
		assertThat(ValidationAction.ERROR.getValue()).isEqualTo(validationAction);
	}

	@Test // DATAMONGO-1322
	public void testCollectionValidationActionWarn() {
		Criteria criteria = Criteria.where("name").type(2);
		ValidationOptions validationOptions = ValidationOptions.validator(CriteriaValidator.fromCriteria(criteria))
				.validationAction(ValidationAction.WARN);
		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().validation(validationOptions));

		String validationAction = getValidationActionInfo(COLLECTION_NAME);
		assertThat(ValidationAction.WARN.getValue()).isEqualTo(validationAction);
	}

	@Test // DATAMONGO-1322
	public void testCollectionValidationLevelOff() {
		Criteria criteria = Criteria.where("name").type(2);
		ValidationOptions validationOptions = ValidationOptions.validator(CriteriaValidator.fromCriteria(criteria))
				.validationLevel(ValidationLevel.OFF);
		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().validation(validationOptions));

		String validationAction = getValidationLevelInfo(COLLECTION_NAME);
		assertThat(ValidationLevel.OFF.getValue()).isEqualTo(validationAction);
	}

	@Test // DATAMONGO-1322
	public void testCollectionValidationLevelModerate() {
		Criteria criteria = Criteria.where("name").type(2);
		ValidationOptions validationOptions = ValidationOptions.validator(CriteriaValidator.fromCriteria(criteria))
				.validationLevel(ValidationLevel.MODERATE);
		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().validation(validationOptions));

		String validationAction = getValidationLevelInfo(COLLECTION_NAME);
		assertThat(ValidationLevel.MODERATE.getValue()).isEqualTo(validationAction);
	}

	@Test // DATAMONGO-1322
	public void testCollectionValidationLevelStrict() {
		Criteria criteria = Criteria.where("name").type(2);
		ValidationOptions validationOptions = ValidationOptions.validator(CriteriaValidator.fromCriteria(criteria))
				.validationLevel(ValidationLevel.STRICT);
		template.createCollection(COLLECTION_NAME, CollectionOptions.empty().validation(validationOptions));

		String validationAction = getValidationLevelInfo(COLLECTION_NAME);
		assertThat(ValidationLevel.STRICT.getValue()).isEqualTo(validationAction);
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

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class SimpleBean {
		@NotNull private String nonNullString;
		@NotNull @Min(0) @Max(122) private Integer rangedInteger;
	}

}
