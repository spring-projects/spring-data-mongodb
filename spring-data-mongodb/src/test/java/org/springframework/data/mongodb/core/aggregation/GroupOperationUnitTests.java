/*
 * Copyright 2013-2023 the original author or authors.
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
import static org.springframework.data.mongodb.core.aggregation.Fields.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Percentile;
import org.springframework.data.mongodb.core.aggregation.SelectionOperators.Bottom;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Unit tests for {@link GroupOperation}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Gustavo de Geus
 * @author Julia Lee
 */
class GroupOperationUnitTests {

	@Test
	void rejectsNullFields() {
		assertThatIllegalArgumentException().isThrownBy(() -> new GroupOperation((Fields) null));
	}

	@Test // DATAMONGO-759
	void groupOperationWithNoGroupIdFieldsShouldGenerateNullAsGroupId() {

		GroupOperation operation = new GroupOperation(Fields.from());
		ExposedFields fields = operation.getFields();
		Document groupClause = extractDocumentFromGroupOperation(operation);

		assertThat(fields.exposesSingleFieldOnly()).isTrue();
		assertThat(fields.exposesNoFields()).isFalse();
		assertThat(groupClause.get(UNDERSCORE_ID)).isNull();
	}

	@Test // DATAMONGO-759
	void groupOperationWithNoGroupIdFieldsButAdditionalFieldsShouldGenerateNullAsGroupId() {

		GroupOperation operation = new GroupOperation(Fields.from()).count().as("cnt").last("foo").as("foo");
		ExposedFields fields = operation.getFields();
		Document groupClause = extractDocumentFromGroupOperation(operation);

		assertThat(fields.exposesSingleFieldOnly()).isFalse();
		assertThat(fields.exposesNoFields()).isFalse();
		assertThat(groupClause.get(UNDERSCORE_ID)).isNull();
		assertThat((Document) groupClause.get("cnt")).isEqualTo(new Document("$sum", 1));
		assertThat((Document) groupClause.get("foo")).isEqualTo(new Document("$last", "$foo"));
	}

	@Test
	void createsGroupOperationWithSingleField() {

		GroupOperation operation = new GroupOperation(fields("a"));

		Document groupClause = extractDocumentFromGroupOperation(operation);

		assertThat(groupClause).containsEntry(UNDERSCORE_ID, "$a");
	}

	@Test
	void createsGroupOperationWithMultipleFields() {

		GroupOperation operation = new GroupOperation(fields("a").and("b", "c"));

		Document groupClause = extractDocumentFromGroupOperation(operation);
		Document idClause = DocumentTestUtils.getAsDocument(groupClause, UNDERSCORE_ID);

		assertThat(idClause).containsEntry("a", "$a").containsEntry("b", "$c");
	}

	@Test
	void groupFactoryMethodWithMultipleFieldsAndSumOperation() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.sum("e").as("e");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document eOp = DocumentTestUtils.getAsDocument(groupClause, "e");
		assertThat(eOp).isEqualTo(new Document("$sum", "$e"));
	}

	@Test
	void groupFactoryMethodWithMultipleFieldsAndSumOperationWithAlias() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.sum("e").as("ee");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document eOp = DocumentTestUtils.getAsDocument(groupClause, "ee");
		assertThat(eOp).isEqualTo(new Document("$sum", "$e"));
	}

	@Test
	void groupFactoryMethodWithMultipleFieldsAndCountOperationWithout() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.count().as("count");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document eOp = DocumentTestUtils.getAsDocument(groupClause, "count");
		assertThat(eOp).isEqualTo(new Document("$sum", 1));
	}

	@Test
	void groupFactoryMethodWithMultipleFieldsAndMultipleAggregateOperationsWithAlias() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.sum("e").as("sum") //
				.min("e").as("min"); //

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document sum = DocumentTestUtils.getAsDocument(groupClause, "sum");
		assertThat(sum).isEqualTo(new Document("$sum", "$e"));

		Document min = DocumentTestUtils.getAsDocument(groupClause, "min");
		assertThat(min).isEqualTo(new Document("$min", "$e"));
	}

	@Test
	void groupOperationPushWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").push(1).as("x");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "x");

		assertThat(push).isEqualTo(new Document("$push", 1));
	}

	@Test
	void groupOperationPushWithReference() {

		GroupOperation groupOperation = Aggregation.group("a", "b").push("ref").as("x");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "x");

		assertThat(push).isEqualTo(new Document("$push", "$ref"));
	}

	@Test
	void groupOperationAddToSetWithReference() {

		GroupOperation groupOperation = Aggregation.group("a", "b").addToSet("ref").as("x");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "x");

		assertThat(push).isEqualTo(new Document("$addToSet", "$ref"));
	}

	@Test
	void groupOperationAddToSetWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").addToSet(42).as("x");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "x");

		assertThat(push).isEqualTo(new Document("$addToSet", 42));
	}

	@Test // DATAMONGO-979
	void shouldRenderSizeExpressionInGroup() {

		GroupOperation groupOperation = Aggregation //
				.group("username") //
				.first(ArrayOperators.arrayOf("tags").length()) //
				.as("tags_count");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document tagsCount = DocumentTestUtils.getAsDocument(groupClause, "tags_count");

		assertThat(tagsCount).containsEntry("$first", new Document("$size", "$tags"));
	}

	@Test // DATAMONGO-1327
	void groupOperationStdDevSampWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").stdDevSamp("field").as("fieldStdDevSamp");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "fieldStdDevSamp");

		assertThat(push).isEqualTo(new Document("$stdDevSamp", "$field"));
	}

	@Test // DATAMONGO-1327
	void groupOperationStdDevPopWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").stdDevPop("field").as("fieldStdDevPop");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "fieldStdDevPop");

		assertThat(push).isEqualTo(new Document("$stdDevPop", "$field"));
	}

	@Test // DATAMONGO-1784
	void shouldRenderSumWithExpressionInGroup() {

		GroupOperation groupOperation = Aggregation //
				.group("username") //
				.sum(ConditionalOperators //
						.when(Criteria.where("foo").is("bar")) //
						.then(1) //
						.otherwise(-1)) //
				.as("foobar");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document foobar = DocumentTestUtils.getAsDocument(groupClause, "foobar");

		assertThat(foobar).containsEntry("$sum", new Document("$cond",
				new Document("if", new Document("$eq", Arrays.asList("$foo", "bar"))).append("then", 1).append("else", -1)));
	}

	@Test // DATAMONGO-1784
	void sumWithNullExpressionShouldThrowException() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Aggregation.group("username").sum((AggregationExpression) null));
	}

	@Test // DATAMONGO-2651
	void accumulatorShouldBeAllowedOnGroupOperation() {

		GroupOperation groupOperation = Aggregation.group("id")
				.accumulate(
						ScriptOperators.accumulatorBuilder().init("inti").accumulate("acc").merge("merge").finalize("finalize"))
				.as("accumulated-value");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document accumulatedValue = DocumentTestUtils.getAsDocument(groupClause, "accumulated-value");

		assertThat(accumulatedValue).containsKey("$accumulator");
	}

	@Test // GH-4139
	void groupOperationAllowsToAddFieldsComputedViaExpression() {

		GroupOperation groupOperation = Aggregation.group("id").and("playerId",
				Bottom.bottom().output("playerId", "score").sortBy(Sort.by(Direction.DESC, "score")));
		Document groupClause = extractDocumentFromGroupOperation(groupOperation);

		assertThat(groupClause).containsEntry("playerId",
				Document.parse("{ $bottom : { output: [ \"$playerId\", \"$score\" ], sortBy: { \"score\": -1 }}}"));
	}

	@Test // GH-4473
	void groupOperationAllowsAddingFieldWithPercentileAggregationExpression() {

		GroupOperation groupOperation = Aggregation.group("id").and("scorePercentile",
			Percentile.percentileOf("score").percentages(0.2));

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);

		assertThat(groupClause).containsEntry("scorePercentile",
			Document.parse("{ $percentile : { input: \"$score\", method: \"approximate\", p: [0.2]}}"));
	}

	private Document extractDocumentFromGroupOperation(GroupOperation groupOperation) {
		Document document = groupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document groupClause = DocumentTestUtils.getAsDocument(document, "$group");
		return groupClause;
	}
}
