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

	@Test
	public void createsGroupOperationWithSingleField() {

		GroupOperation operation = new GroupOperation(fields("a"));

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject groupClause = DBObjectUtils.getAsDBObject(dbObject, "$group");

		assertThat(groupClause.get(UNDERSCORE_ID), is((Object) "$a"));
	}

	@Test
	public void createsGroupOperationWithMultipleFields() {

		GroupOperation operation = new GroupOperation(fields("a").and("b", "c"));

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject groupClause = DBObjectUtils.getAsDBObject(dbObject, "$group");
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

		Fields fields = fields("a", "b").and("c"); // .and("d", 42);
		GroupOperation groupOperation = new GroupOperation(fields).sum("e");

		DBObject dbObject = groupOperation.toDBObject(Aggregation.DEFAULT_CONTEXT);

		DBObject groupClause = DBObjectUtils.getAsDBObject(dbObject, "$group");
		DBObject eOp = DBObjectUtils.getAsDBObject(groupClause, "e");
		assertThat(eOp, is((DBObject) new BasicDBObject("$sum", "$e")));
	}
}
