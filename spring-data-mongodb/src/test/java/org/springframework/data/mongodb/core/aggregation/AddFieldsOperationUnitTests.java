/*
 * Copyright 2020-2023 the original author or authors.
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
 * Unit tests for {@link AddFieldsOperation}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class AddFieldsOperationUnitTests {

	@Test // DATAMONGO-2363
	void raisesErrorOnNullField() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new AddFieldsOperation(null, "value"));
	}

	@Test // DATAMONGO-2363
	void rendersFieldReferenceCorrectly() {

		assertThat(new AddFieldsOperation("name", "value").toPipelineStages(contextFor(Scores.class)))
				.containsExactly(Document.parse("{\"$addFields\" : {\"name\":\"value\"}}"));
	}

	@Test // DATAMONGO-2363
	void rendersMultipleEntriesCorrectly() {

		assertThat(new AddFieldsOperation("name", "value").addField("field-2", "value2")
				.toPipelineStages(contextFor(Scores.class)))
						.containsExactly(Document.parse("{\"$addFields\" : {\"name\":\"value\", \"field-2\":\"value2\"}}"));
	}

	@Test // DATAMONGO-2363
	void rendersMappedFieldReferenceCorrectly() {

		assertThat(new AddFieldsOperation("student", "value").toPipelineStages(contextFor(ScoresWithMappedField.class)))
				.containsExactly(Document.parse("{\"$addFields\" : {\"student_name\":\"value\"}}"));
	}

	@Test // DATAMONGO-2363
	void rendersNestedMappedFieldReferenceCorrectly() {

		assertThat(new AddFieldsOperation("scoresWithMappedField.student", "value")
				.toPipelineStages(contextFor(ScoresWrapper.class)))
						.containsExactly(Document.parse("{\"$addFields\" : {\"scoresWithMappedField.student_name\":\"value\"}}"));
	}

	@Test // DATAMONGO-2363
	void rendersTargetValueFieldReferenceCorrectly() {

		assertThat(new AddFieldsOperation("name", Fields.field("value")).toPipelineStages(contextFor(Scores.class)))
				.containsExactly(Document.parse("{\"$addFields\" : {\"name\":\"$value\"}}"));
	}

	@Test // DATAMONGO-2363
	void rendersMappedTargetValueFieldReferenceCorrectly() {

		assertThat(new AddFieldsOperation("student", Fields.field("homework"))
				.toPipelineStages(contextFor(ScoresWithMappedField.class)))
						.containsExactly(Document.parse("{\"$addFields\" : {\"student_name\":\"$home_work\"}}"));
	}

	@Test // DATAMONGO-2363
	void rendersNestedMappedTargetValueFieldReferenceCorrectly() {

		assertThat(new AddFieldsOperation("scoresWithMappedField.student", Fields.field("scoresWithMappedField.homework"))
				.toPipelineStages(contextFor(ScoresWrapper.class)))
						.containsExactly(Document.parse(
								"{\"$addFields\" : {\"scoresWithMappedField.student_name\":\"$scoresWithMappedField.home_work\"}}"));
	}

	@Test // DATAMONGO-2363
	void appliesSpelExpressionCorrectly() {

		AddFieldsOperation operation = AddFieldsOperation.builder().addField("totalHomework")
				.withValueOfExpression("sum(homework) * [0]", 2) //
				.build();

		assertThat(operation.toPipelineStages(contextFor(ScoresWrapper.class))).contains(
				Document.parse("{\"$addFields\" : {\"totalHomework\": { $multiply : [{ \"$sum\" : [\"$homework\"] }, 2] }}}"));
	}

	@Test // DATAMONGO-2363
	void rendersTargetValueExpressionCorrectly() {

		assertThat(AddFieldsOperation.builder().addField("totalHomework")
				.withValueOf(ArithmeticOperators.valueOf("homework").sum()).build().toPipelineStages(contextFor(Scores.class)))
						.containsExactly(Document.parse("{\"$addFields\" : {\"totalHomework\": { \"$sum\" : \"$homework\" }}}"));
	}

	@Test // DATAMONGO-2363
	void exposesFieldsCorrectly() {

		ExposedFields fields = AddFieldsOperation.builder().addField("totalHomework").withValue("A+") //
				.addField("totalQuiz").withValue("B-") //
				.addField("computed").withValueOfExpression("totalHomework").build().getFields();

		assertThat(fields.getField("totalHomework")).isNotNull();
		assertThat(fields.getField("totalQuiz")).isNotNull();
		assertThat(fields.getField("computed")).isNotNull();
		assertThat(fields.getField("does-not-exist")).isNull();
	}

	private static AggregationOperationContext contextFor(@Nullable Class<?> type) {

		if (type == null) {
			return Aggregation.DEFAULT_CONTEXT;
		}

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE,
				new MongoMappingContext());
		mongoConverter.afterPropertiesSet();

		return new TypeBasedAggregationOperationContext(type, mongoConverter.getMappingContext(),
				new QueryMapper(mongoConverter)).continueOnMissingFieldReference();
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
