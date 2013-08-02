/*
 * Copyright 2013 the original author or authors.
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
import static org.springframework.data.mongodb.core.aggregation.Fields.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link GroupOperation}.
 * 
 * @author Oliver Gierke
 */
public class GroupOperationUnitTests {

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFields() {
		new GroupOperation((Fields) null);
	}

	@Test
	public void createsGroupOperationWithSingleField() {

		GroupOperation operation = new GroupOperation(fields("a"));

		DBObject groupClause = extractDbObjectFromGroupOperation(operation);

		assertThat(groupClause.get(UNDERSCORE_ID), is((Object) "$a"));
	}

	@Test
	public void createsGroupOperationWithMultipleFields() {

		GroupOperation operation = new GroupOperation(fields("a").and("b", "c"));

		DBObject groupClause = extractDbObjectFromGroupOperation(operation);
		DBObject idClause = DBObjectUtils.getAsDBObject(groupClause, UNDERSCORE_ID);

		assertThat(idClause.get("a"), is((Object) "$a"));
		assertThat(idClause.get("b"), is((Object) "$c"));
	}

	@Test
	public void shouldCreateComplexIdForGroupOperationWithSingleComplexIdField() {

		// Fields fields = fields().and("a", 42);
		// GroupOperation groupOperation = new GroupOperation(fields());
		//
		// assertThat(groupOperation.toDBObject(Aggregation.DEFAULT_CONTEXT), is(notNullValue()));
		// assertThat(groupOperation.id, is(notNullValue()));
		// assertThat(groupOperation.id, is((Object) new BasicDBObject("a", 42)));
	}

	@Test
	public void groupFactoryMethodWithMultipleFieldsAndSumOperation() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.sum("e").as("e");

		DBObject groupClause = extractDbObjectFromGroupOperation(groupOperation);
		DBObject eOp = DBObjectUtils.getAsDBObject(groupClause, "e");
		assertThat(eOp, is((DBObject) new BasicDBObject("$sum", "$e")));
	}

	@Test
	public void groupFactoryMethodWithMultipleFieldsAndSumOperationWithAlias() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.sum("e").as("ee");

		DBObject groupClause = extractDbObjectFromGroupOperation(groupOperation);
		DBObject eOp = DBObjectUtils.getAsDBObject(groupClause, "ee");
		assertThat(eOp, is((DBObject) new BasicDBObject("$sum", "$e")));
	}

	@Test
	public void groupFactoryMethodWithMultipleFieldsAndCountOperationWithout() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.count().as("count");

		DBObject groupClause = extractDbObjectFromGroupOperation(groupOperation);
		DBObject eOp = DBObjectUtils.getAsDBObject(groupClause, "count");
		assertThat(eOp, is((DBObject) new BasicDBObject("$sum", 1)));
	}

	@Test
	public void groupFactoryMethodWithMultipleFieldsAndMultipleAggregateOperationsWithAlias() {

		GroupOperation groupOperation = Aggregation.group(fields("a", "b").and("c")) //
				.sum("e").as("sum") //
				.min("e").as("min"); //

		DBObject groupClause = extractDbObjectFromGroupOperation(groupOperation);
		DBObject sum = DBObjectUtils.getAsDBObject(groupClause, "sum");
		assertThat(sum, is((DBObject) new BasicDBObject("$sum", "$e")));

		DBObject min = DBObjectUtils.getAsDBObject(groupClause, "min");
		assertThat(min, is((DBObject) new BasicDBObject("$min", "$e")));
	}

	@Test
	public void groupOperationPushWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").push(1).as("x");

		DBObject groupClause = extractDbObjectFromGroupOperation(groupOperation);
		DBObject push = DBObjectUtils.getAsDBObject(groupClause, "x");

		assertThat(push, is((DBObject) new BasicDBObject("$push", 1)));
	}

	@Test
	public void groupOperationPushWithReference() {

		GroupOperation groupOperation = Aggregation.group("a", "b").push("ref").as("x");

		DBObject groupClause = extractDbObjectFromGroupOperation(groupOperation);
		DBObject push = DBObjectUtils.getAsDBObject(groupClause, "x");

		assertThat(push, is((DBObject) new BasicDBObject("$push", "$ref")));
	}

	@Test
	public void groupOperationAddToSetWithReference() {

		GroupOperation groupOperation = Aggregation.group("a", "b").addToSet("ref").as("x");

		DBObject groupClause = extractDbObjectFromGroupOperation(groupOperation);
		DBObject push = DBObjectUtils.getAsDBObject(groupClause, "x");

		assertThat(push, is((DBObject) new BasicDBObject("$addToSet", "$ref")));
	}

	@Test
	public void groupOperationAddToSetWithValue() {

		GroupOperation groupOperation = Aggregation.group("a", "b").addToSet(42).as("x");

		DBObject groupClause = extractDbObjectFromGroupOperation(groupOperation);
		DBObject push = DBObjectUtils.getAsDBObject(groupClause, "x");

		assertThat(push, is((DBObject) new BasicDBObject("$addToSet", 42)));
	}

	private DBObject extractDbObjectFromGroupOperation(GroupOperation groupOperation) {
		DBObject dbObject = groupOperation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject groupClause = DBObjectUtils.getAsDBObject(dbObject, "$group");
		return groupClause;
	}
}
