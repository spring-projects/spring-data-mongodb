/*
 * Copyright 2019-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link SetOperation}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class SetOperationUnitTests {

	@Test // DATAMONGO-2331
	void raisesErrorOnNullField() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new SetOperation(null, "value"));
	}

	@Test // DATAMONGO-2331
	void rendersFieldReferenceCorrectly() {

		assertThat(new SetOperation("name", "value").toPipelineStages(contextFor(Scores.class)))
				.containsExactly(Document.parse("{\"$set\" : {\"name\":\"value\"}}"));
	}

	@Test // DATAMONGO-2331
	void rendersMappedFieldReferenceCorrectly() {

		assertThat(new SetOperation("student", "value").toPipelineStages(contextFor(ScoresWithMappedField.class)))
				.containsExactly(Document.parse("{\"$set\" : {\"student_name\":\"value\"}}"));
	}

	@Test // DATAMONGO-2331
	void rendersNestedMappedFieldReferenceCorrectly() {

		assertThat(
				new SetOperation("scoresWithMappedField.student", "value").toPipelineStages(contextFor(ScoresWrapper.class)))
						.containsExactly(Document.parse("{\"$set\" : {\"scoresWithMappedField.student_name\":\"value\"}}"));
	}

	@Test // DATAMONGO-2331
	void rendersTargetValueFieldReferenceCorrectly() {

		assertThat(new SetOperation("name", Fields.field("value")).toPipelineStages(contextFor(Scores.class)))
				.containsExactly(Document.parse("{\"$set\" : {\"name\":\"$value\"}}"));
	}

	@Test // DATAMONGO-2331
	void rendersMappedTargetValueFieldReferenceCorrectly() {

		assertThat(
				new SetOperation("student", Fields.field("homework")).toPipelineStages(contextFor(ScoresWithMappedField.class)))
						.containsExactly(Document.parse("{\"$set\" : {\"student_name\":\"$home_work\"}}"));
	}

	@Test // DATAMONGO-2331
	void rendersNestedMappedTargetValueFieldReferenceCorrectly() {

		assertThat(new SetOperation("scoresWithMappedField.student", Fields.field("scoresWithMappedField.homework"))
				.toPipelineStages(contextFor(ScoresWrapper.class)))
						.containsExactly(Document
								.parse("{\"$set\" : {\"scoresWithMappedField.student_name\":\"$scoresWithMappedField.home_work\"}}"));
	}

	@Test // DATAMONGO-2363
	void appliesSpelExpressionCorrectly() {

		SetOperation operation = SetOperation.builder().set("totalHomework").withValueOfExpression("sum(homework) * [0]",
				2);

		assertThat(operation.toPipelineStages(contextFor(AddFieldsOperationUnitTests.ScoresWrapper.class))).contains(
				Document.parse("{\"$set\" : {\"totalHomework\": { $multiply : [{ \"$sum\" : [\"$homework\"] }, 2] }}}"));
	}

	@Test // DATAMONGO-2331
	void rendersTargetValueExpressionCorrectly() {

		assertThat(SetOperation.builder().set("totalHomework").toValueOf(ArithmeticOperators.valueOf("homework").sum())
				.toPipelineStages(contextFor(Scores.class)))
						.containsExactly(Document.parse("{\"$set\" : {\"totalHomework\": { \"$sum\" : \"$homework\" }}}"));
	}

	@Test // DATAMONGO-2331
	void exposesFieldsCorrectly() {

		ExposedFields fields = SetOperation.builder().set("totalHomework").toValue("A+") //
				.and() //
				.set("totalQuiz").toValue("B-") //
				.getFields();

		assertThat(fields.getField("totalHomework")).isNotNull();
		assertThat(fields.getField("totalQuiz")).isNotNull();
		assertThat(fields.getField("does-not-exist")).isNull();
	}

	private static AggregationOperationContext contextFor(@Nullable Class<?> type) {

		if (type == null) {
			return Aggregation.DEFAULT_CONTEXT;
		}

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE,
				new MongoMappingContext());
		mongoConverter.afterPropertiesSet();

		return new RelaxedTypeBasedAggregationOperationContext(type, mongoConverter.getMappingContext(),
				new QueryMapper(mongoConverter));
	}

	static class Scores {

		String student;
		List<Integer> homework;
	}

	static class ScoresWithMappedField {

		@Field("student_name") String student;
		@Field("home_work") List<Integer> homework;
	}

	static class ScoresWrapper {

		Scores scores;
		ScoresWithMappedField scoresWithMappedField;
	}
}
