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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectUtils;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation.ProjectionOperationBuilder;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link ProjectionOperation}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class ProjectionOperationUnitTests {

	static final String MOD = "$mod";
	static final String ADD = "$add";
	static final String SUBTRACT = "$subtract";
	static final String MULTIPLY = "$multiply";
	static final String DIVIDE = "$divide";
	static final String PROJECT = "$project";

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFields() {
		new ProjectionOperation(null);
	}

	@Test
	public void declaresBackReferenceCorrectly() {

		ProjectionOperation operation = new ProjectionOperation();
		operation = operation.and("prop").previousOperation();

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);
		assertThat(projectClause.get("prop"), is((Object) Fields.UNDERSCORE_ID_REF));
	}

	@Test
	public void alwaysUsesExplicitReference() {

		ProjectionOperation operation = new ProjectionOperation(Fields.fields("foo").and("bar", "foobar"));

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);

		assertThat(projectClause.get("foo"), is((Object) "$foo"));
		assertThat(projectClause.get("bar"), is((Object) "$foobar"));
	}

	@Test
	public void aliasesSimpleFieldProjection() {

		ProjectionOperation operation = new ProjectionOperation();

		DBObject dbObject = operation.and("foo").as("bar").toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);

		assertThat(projectClause.get("bar"), is((Object) "$foo"));
	}

	@Test
	public void aliasesArithmeticProjection() {

		ProjectionOperation operation = new ProjectionOperation();

		DBObject dbObject = operation.and("foo").plus(41).as("bar").toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);
		DBObject barClause = DBObjectUtils.getAsDBObject(projectClause, "bar");
		BasicDBList addClause = DBObjectUtils.getAsDBList(barClause, "$add");

		assertThat(addClause, hasSize(2));
		assertThat(addClause.get(0), is((Object) "$foo"));
		assertThat(addClause.get(1), is((Object) 41));
	}

	public void arithmenticProjectionOperationWithoutAlias() {

		String fieldName = "a";
		ProjectionOperationBuilder operation = new ProjectionOperation().and(fieldName).plus(1);
		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);
		DBObject oper = exctractOperation(fieldName, projectClause);

		assertThat(oper.containsField(ADD), is(true));
		assertThat(oper.get(ADD), is((Object) Arrays.<Object> asList("$a", 1)));
	}

	@Test
	public void arithmenticProjectionOperationPlus() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).plus(1).as(fieldAlias);
		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);

		DBObject oper = exctractOperation(fieldAlias, projectClause);
		assertThat(oper.containsField(ADD), is(true));
		assertThat(oper.get(ADD), is((Object) Arrays.<Object> asList("$a", 1)));
	}

	@Test
	public void arithmenticProjectionOperationMinus() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).minus(1).as(fieldAlias);
		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);
		DBObject oper = exctractOperation(fieldAlias, projectClause);

		assertThat(oper.containsField(SUBTRACT), is(true));
		assertThat(oper.get(SUBTRACT), is((Object) Arrays.<Object> asList("$a", 1)));
	}

	@Test
	public void arithmenticProjectionOperationMultiply() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).multiply(1).as(fieldAlias);
		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);
		DBObject oper = exctractOperation(fieldAlias, projectClause);

		assertThat(oper.containsField(MULTIPLY), is(true));
		assertThat(oper.get(MULTIPLY), is((Object) Arrays.<Object> asList("$a", 1)));
	}

	@Test
	public void arithmenticProjectionOperationDivide() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).divide(1).as(fieldAlias);
		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);
		DBObject oper = exctractOperation(fieldAlias, projectClause);

		assertThat(oper.containsField(DIVIDE), is(true));
		assertThat(oper.get(DIVIDE), is((Object) Arrays.<Object> asList("$a", 1)));
	}

	@Test(expected = IllegalArgumentException.class)
	public void arithmenticProjectionOperationDivideByZeroException() {

		new ProjectionOperation().and("a").divide(0);
	}

	@Test
	public void arithmenticProjectionOperationMod() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).mod(3).as(fieldAlias);
		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);
		DBObject oper = exctractOperation(fieldAlias, projectClause);

		assertThat(oper.containsField(MOD), is(true));
		assertThat(oper.get(MOD), is((Object) Arrays.<Object> asList("$a", 3)));
	}

	/**
	 * @see DATAMONGO-758
	 */
	@Test(expected = IllegalArgumentException.class)
	public void excludeShouldThrowExceptionForFieldsOtherThanUnderscoreId() {
		new ProjectionOperation().andExclude("foo");
	}

	/**
	 * @see DATAMONGO-758
	 */
	@Test
	public void andExcludeShouldSupportTheExclusionOfUnderscoreId() {

		ProjectionOperation projectionOp = new ProjectionOperation().andExclude(Fields.UNDERSCORE_ID);
		DBObject dbObject = projectionOp.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);

		assertThat((Integer) projectClause.get(Fields.UNDERSCORE_ID), is(0));
	}

	@Test(expected = IllegalArgumentException.class)
	public void arithmenticProjectionOperationModByZeroException() {

		new ProjectionOperation().and("a").mod(0);
	}

	private static DBObject exctractOperation(String field, DBObject fromProjectClause) {
		return (DBObject) fromProjectClause.get(field);
	}
}
