/*
 * Copyright 2013-2016 the original author or authors.
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

	/**
	 * @see DATAMONGO-759
	 */
	@Test
	public void groupOperationWithNoGroupIdFieldsShouldGenerateNullAsGroupId() {

		GroupOperation operation = new GroupOperation(Fields.from());
		ExposedFields fields = operation.getFields();
		Document groupClause = extractDocumentFromGroupOperation(operation);

		assertThat(fields.exposesSingleFieldOnly(), is(true));
		assertThat(fields.exposesNoFields(), is(false));
		assertThat(groupClause.get(UNDERSCORE_ID), is(nullValue()));
	}

	/**
	 * @see DATAMONGO-759
	 */
	@Test
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

	/**
	 * @see DATAMONGO-979
	 */
	@Test
	public void shouldRenderSizeExpressionInGroup() {

		GroupOperation groupOperation = Aggregation //
				.group("username") //
				.first(SIZE.of(field("tags"))) //
				.as("tags_count");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document tagsCount = DocumentTestUtils.getAsDocument(groupClause, "tags_count");

		assertThat(tagsCount.get("$first"), is((Object) new Document("$size", Arrays.asList("$tags"))));
	}

	/**
	 * @see DATAMONGO-1327
	 */
	@Test
	public void groupOperationStdDevSampWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").stdDevSamp("field").as("fieldStdDevSamp");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "fieldStdDevSamp");

		assertThat(push, is(new Document("$stdDevSamp", "$field")));
	}

	/**
	 * @see DATAMONGO-1327
	 */
	@Test
	public void groupOperationStdDevPopWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").stdDevPop("field").as("fieldStdDevPop");

		Document groupClause = extractDocumentFromGroupOperation(groupOperation);
		Document push = DocumentTestUtils.getAsDocument(groupClause, "fieldStdDevPop");

		assertThat(push, is(new Document("$stdDevPop", "$field")));
	}

	private Document extractDocumentFromGroupOperation(GroupOperation groupOperation) {
		Document document = groupOperation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document groupClause = DocumentTestUtils.getAsDocument(document, "$group");
		return groupClause;
	}
}
