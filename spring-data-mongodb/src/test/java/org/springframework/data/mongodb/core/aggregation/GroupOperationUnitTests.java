/*
 * Copyright 2013-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.aggregation.AggregationFunctionExpressions.*;
import static org.springframework.data.mongodb.core.aggregation.Fields.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Unit tests for {@link GroupOperation}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Gustavo de Geus
 */
public class GroupOperationUnitTests {

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFields() {
		new GroupOperation((Fields) null);
	}

	@Test // DATAMONGO-759
	public void groupOperationWithNoGroupIdFieldsShouldGenerateNullAsGroupId() {

		GroupOperation operation = new GroupOperation(Fields.from());
		ExposedFields fields = operation.getFields();
		Document groupClause = extractDocumentFromGroupOperation(operation);

		assertThat(fields.exposesSingleFieldOnly(), is(true));
		assertThat(fields.exposesNoFields(), is(false));
		assertThat(groupClause.get(UNDERSCORE_ID), is(nullValue()));
	}

	@Test // DATAMONGO-759
	public void groupOperationWithNoGroupIdFieldsButAdditionalFieldsShouldGenerateNullAsGroupId() {

		GroupOperation operation = new GroupOperation(Fields.from()).count().as("cnt").last("foo").as("foo");
		ExposedFields fields = operation.getFields();
		Document groupClause = extractDocumentFromGroupOperation(operation);

		assertThat(fields.exposesSingleFieldOnly(), is(false));
		assertThat(fields.exposesNoFields(), is(false));
		assertThat(groupClause.get(UNDERSCORE_ID), is(nullValue()));
		assertThat((Document) groupClause.get("cnt"), is(new Document("$sum", 1)));
		assertThat((Document) groupClause.get("foo"), is(new Document("$last", "$foo")));
	}

	@Test
	public void createsGroupOperationWithSingleField() {

		GroupOperation operation = new GroupOperation(fields("a"));

		Document groupClause = extractDocumentFromGroupOperation(operation);

		assertThat(groupClause.get(UNDERSCORE_ID), is((Object) "$a"));
	}

	@Test
	public void createsGroupOperationWithMultipleFields() {

		GroupOperation operation = new GroupOperation(fields("a").and("b", "c"));

		Document groupClause = extractDocumentFromGroupOperation(operation);
		Document idClause = DocumentTestUtils.getAsDocument(groupClause, UNDERSCORE_ID);

		assertThat(idClause.get("a"), is((Object) "$a"));
		assertThat(idClause.get("b"), is((Object) "$c"));
	}

	@Test
	public void groupFactoryMethodWithMultipleFieldsAndSumOperation() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.sum("e").as("e");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document eOp = DocumentTestUtils.getAsDocument(groupClause, "e");
		assertThat(eOp, is((Document) new Document("$sum", "$e")));
	}

	@Test
	public void groupFactoryMethodWithMultipleFieldsAndSumOperationWithAlias() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.sum("e").as("ee");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document eOp = DocumentTestUtils.getAsDocument(groupClause, "ee");
		assertThat(eOp, is((Document) new Document("$sum", "$e")));
	}

	@Test
	public void groupFactoryMethodWithMultipleFieldsAndCountOperationWithout() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.count().as("count");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document eOp = DocumentTestUtils.getAsDocument(groupClause, "count");
		assertThat(eOp, is((Document) new Document("$sum", 1)));
	}

	@Test
	public void groupFactoryMethodWithMultipleFieldsAndMultipleAggregateOperationsWithAlias() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.sum("e").as("sum") //
				.min("e").as("min"); //

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document sum = DocumentTestUtils.getAsDocument(groupClause, "sum");
		assertThat(sum, is((Document) new Document("$sum", "$e")));

		Document min = DocumentTestUtils.getAsDocument(groupClause, "min");
		assertThat(min, is((Document) new Document("$min", "$e")));
	}

	@Test
	public void groupOperationPushWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").push(1).as("x");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "x");

		assertThat(push, is((Document) new Document("$push", 1)));
	}

	@Test
	public void groupOperationPushWithReference() {

		GroupOperation groupOperation = Aggregation.group("a", "b").push("ref").as("x");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "x");

		assertThat(push, is((Document) new Document("$push", "$ref")));
	}

	@Test
	public void groupOperationAddToSetWithReference() {

		GroupOperation groupOperation = Aggregation.group("a", "b").addToSet("ref").as("x");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "x");

		assertThat(push, is((Document) new Document("$addToSet", "$ref")));
	}

	@Test
	public void groupOperationAddToSetWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").addToSet(42).as("x");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "x");

		assertThat(push, is((Document) new Document("$addToSet", 42)));
	}

	@Test // DATAMONGO-979
	public void shouldRenderSizeExpressionInGroup() {

		GroupOperation groupOperation = Aggregation //
				.group("username") //
				.first(SIZE.of(field("tags"))) //
				.as("tags_count");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document tagsCount = DocumentTestUtils.getAsDocument(groupClause, "tags_count");

		assertThat(tagsCount.get("$first"), is((Object) new Document("$size", Arrays.asList("$tags"))));
	}

	@Test // DATAMONGO-1327
	public void groupOperationStdDevSampWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").stdDevSamp("field").as("fieldStdDevSamp");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "fieldStdDevSamp");

		assertThat(push, is(new Document("$stdDevSamp", "$field")));
	}

	@Test // DATAMONGO-1327
	public void groupOperationStdDevPopWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").stdDevPop("field").as("fieldStdDevPop");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "fieldStdDevPop");

		assertThat(push, is(new Document("$stdDevPop", "$field")));
	}

	@Test // DATAMONGO-1784
	public void shouldRenderSumWithExpressionInGroup() {

		GroupOperation groupOperation = Aggregation //
				.group("username") //
				.sum(ConditionalOperators //
						.when(Criteria.where("foo").is("bar")) //
						.then(1) //
						.otherwise(-1)) //
				.as("foobar");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document foobar = DocumentTestUtils.getAsDocument(groupClause, "foobar");

		assertThat(foobar.get("$sum"), is(new Document("$cond",
				new Document("if", new Document("$eq", Arrays.asList("$foo", "bar"))).append("then", 1).append("else", -1))));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1784
	public void sumWithNullExpressionShouldThrowException() {
		Aggregation.group("username").sum((AggregationExpression) null);
	}

	private Document extractDocumentFromGroupOperation(GroupOperation groupOperation) {
		Document document = groupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document groupClause = DocumentTestUtils.getAsDocument(document, "$group");
		return groupClause;
	}
}
