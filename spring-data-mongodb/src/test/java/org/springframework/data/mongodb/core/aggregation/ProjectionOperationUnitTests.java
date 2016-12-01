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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.aggregation.AggregationFunctionExpressions.*;
import static org.springframework.data.mongodb.core.aggregation.Fields.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;
import static org.springframework.data.mongodb.util.DBObjectUtils.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.ArithmeticOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.ArrayOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.DateOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.LiteralOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.SetOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.StringOperators;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation.ProjectionOperationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Unit tests for {@link ProjectionOperation}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
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
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);
		assertThat(projectClause.get("prop"), is((Object) Fields.UNDERSCORE_ID_REF));
	}

	@Test
	public void alwaysUsesExplicitReference() {

		ProjectionOperation operation = new ProjectionOperation(Fields.fields("foo").and("bar", "foobar"));

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);

		assertThat(projectClause.get("foo"), is((Object) 1));
		assertThat(projectClause.get("bar"), is((Object) "$foobar"));
	}

	@Test
	public void aliasesSimpleFieldProjection() {

		ProjectionOperation operation = new ProjectionOperation();

		DBObject dbObject = operation.and("foo").as("bar").toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);

		assertThat(projectClause.get("bar"), is((Object) "$foo"));
	}

	@Test
	public void aliasesArithmeticProjection() {

		ProjectionOperation operation = new ProjectionOperation();

		DBObject dbObject = operation.and("foo").plus(41).as("bar").toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);
		DBObject barClause = DBObjectTestUtils.getAsDBObject(projectClause, "bar");
		List<Object> addClause = (List<Object>) barClause.get("$add");

		assertThat(addClause, hasSize(2));
		assertThat(addClause.get(0), is((Object) "$foo"));
		assertThat(addClause.get(1), is((Object) 41));
	}

	public void arithmenticProjectionOperationWithoutAlias() {

		String fieldName = "a";
		ProjectionOperationBuilder operation = new ProjectionOperation().and(fieldName).plus(1);
		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);
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
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);

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
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);
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
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);
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
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);
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
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);
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
	public void excludeShouldAllowExclusionOfUnderscoreId() {

		ProjectionOperation projectionOp = new ProjectionOperation().andExclude(Fields.UNDERSCORE_ID);
		DBObject dbObject = projectionOp.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);
		assertThat((Integer) projectClause.get(Fields.UNDERSCORE_ID), is(0));
	}

	/**
	 * @see DATAMONGO-757
	 */
	@Test
	public void usesImplictAndExplicitFieldAliasAndIncludeExclude() {

		ProjectionOperation operation = Aggregation.project("foo").and("foobar").as("bar").andInclude("inc1", "inc2")
				.andExclude("_id");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);

		assertThat(projectClause.get("foo"), is((Object) 1)); // implicit
		assertThat(projectClause.get("bar"), is((Object) "$foobar")); // explicit
		assertThat(projectClause.get("inc1"), is((Object) 1)); // include shortcut
		assertThat(projectClause.get("inc2"), is((Object) 1));
		assertThat(projectClause.get("_id"), is((Object) 0));
	}

	@Test(expected = IllegalArgumentException.class)
	public void arithmenticProjectionOperationModByZeroException() {

		new ProjectionOperation().and("a").mod(0);
	}

	/**
	 * @see DATAMONGO-769
	 */
	@Test
	public void allowArithmeticOperationsWithFieldReferences() {

		ProjectionOperation operation = Aggregation.project() //
				.and("foo").plus("bar").as("fooPlusBar") //
				.and("foo").minus("bar").as("fooMinusBar") //
				.and("foo").multiply("bar").as("fooMultiplyBar") //
				.and("foo").divide("bar").as("fooDivideBar") //
				.and("foo").mod("bar").as("fooModBar");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);

		assertThat((BasicDBObject) projectClause.get("fooPlusBar"), //
				is(new BasicDBObject("$add", dbList("$foo", "$bar"))));
		assertThat((BasicDBObject) projectClause.get("fooMinusBar"), //
				is(new BasicDBObject("$subtract", dbList("$foo", "$bar"))));
		assertThat((BasicDBObject) projectClause.get("fooMultiplyBar"), //
				is(new BasicDBObject("$multiply", dbList("$foo", "$bar"))));
		assertThat((BasicDBObject) projectClause.get("fooDivideBar"), //
				is(new BasicDBObject("$divide", dbList("$foo", "$bar"))));
		assertThat((BasicDBObject) projectClause.get("fooModBar"), //
				is(new BasicDBObject("$mod", dbList("$foo", "$bar"))));
	}

	/**
	 * @see DATAMONGO-774
	 */
	@Test
	public void projectionExpressions() {

		ProjectionOperation operation = Aggregation.project() //
				.andExpression("(netPrice + surCharge) * taxrate * [0]", 2).as("grossSalesPrice") //
				.and("foo").as("bar"); //

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(dbObject.toString(), is(
				"{ \"$project\" : { \"grossSalesPrice\" : { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\" , 2]} , \"bar\" : \"$foo\"}}"));
	}

	/**
	 * @see DATAMONGO-975
	 */
	@Test
	public void shouldRenderDateTimeFragmentExtractionsForSimpleFieldProjectionsCorrectly() {

		ProjectionOperation operation = Aggregation.project() //
				.and("date").extractHour().as("hour") //
				.and("date").extractMinute().as("min") //
				.and("date").extractSecond().as("second") //
				.and("date").extractMillisecond().as("millis") //
				.and("date").extractYear().as("year") //
				.and("date").extractMonth().as("month") //
				.and("date").extractWeek().as("week") //
				.and("date").extractDayOfYear().as("dayOfYear") //
				.and("date").extractDayOfMonth().as("dayOfMonth") //
				.and("date").extractDayOfWeek().as("dayOfWeek") //
		;

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(dbObject, is(notNullValue()));

		DBObject projected = exctractOperation("$project", dbObject);

		assertThat(projected.get("hour"), is((Object) new BasicDBObject("$hour", Arrays.asList("$date"))));
		assertThat(projected.get("min"), is((Object) new BasicDBObject("$minute", Arrays.asList("$date"))));
		assertThat(projected.get("second"), is((Object) new BasicDBObject("$second", Arrays.asList("$date"))));
		assertThat(projected.get("millis"), is((Object) new BasicDBObject("$millisecond", Arrays.asList("$date"))));
		assertThat(projected.get("year"), is((Object) new BasicDBObject("$year", Arrays.asList("$date"))));
		assertThat(projected.get("month"), is((Object) new BasicDBObject("$month", Arrays.asList("$date"))));
		assertThat(projected.get("week"), is((Object) new BasicDBObject("$week", Arrays.asList("$date"))));
		assertThat(projected.get("dayOfYear"), is((Object) new BasicDBObject("$dayOfYear", Arrays.asList("$date"))));
		assertThat(projected.get("dayOfMonth"), is((Object) new BasicDBObject("$dayOfMonth", Arrays.asList("$date"))));
		assertThat(projected.get("dayOfWeek"), is((Object) new BasicDBObject("$dayOfWeek", Arrays.asList("$date"))));
	}

	/**
	 * @see DATAMONGO-975
	 */
	@Test
	public void shouldRenderDateTimeFragmentExtractionsForExpressionProjectionsCorrectly() throws Exception {

		ProjectionOperation operation = Aggregation.project() //
				.andExpression("date + 86400000") //
				.extractDayOfYear() //
				.as("dayOfYearPlus1Day") //
		;

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(dbObject, is(notNullValue()));

		DBObject projected = exctractOperation("$project", dbObject);
		assertThat(projected.get("dayOfYearPlus1Day"), is((Object) new BasicDBObject("$dayOfYear",
				Arrays.asList(new BasicDBObject("$add", Arrays.<Object> asList("$date", 86400000))))));
	}

	/**
	 * @see DATAMONGO-979
	 */
	@Test
	public void shouldRenderSizeExpressionInProjection() {

		ProjectionOperation operation = Aggregation //
				.project() //
				.and("tags") //
				.size()//
				.as("tags_count");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);

		DBObject projected = exctractOperation("$project", dbObject);
		assertThat(projected.get("tags_count"), is((Object) new BasicDBObject("$size", Arrays.asList("$tags"))));
	}

	/**
	 * @see DATAMONGO-979
	 */
	@Test
	public void shouldRenderGenericSizeExpressionInProjection() {

		ProjectionOperation operation = Aggregation //
				.project() //
				.and(SIZE.of(field("tags"))) //
				.as("tags_count");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);

		DBObject projected = exctractOperation("$project", dbObject);
		assertThat(projected.get("tags_count"), is((Object) new BasicDBObject("$size", Arrays.asList("$tags"))));
	}

	/**
	 * @see DATAMONGO-1457
	 */
	@Test
	public void shouldRenderSliceCorrectly() throws Exception {

		ProjectionOperation operation = Aggregation.project().and("field").slice(10).as("renamed");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projected = exctractOperation("$project", dbObject);

		assertThat(projected.get("renamed"),
				is((Object) new BasicDBObject("$slice", Arrays.<Object> asList("$field", 10))));
	}

	/**
	 * @see DATAMONGO-1457
	 */
	@Test
	public void shouldRenderSliceWithPositionCorrectly() throws Exception {

		ProjectionOperation operation = Aggregation.project().and("field").slice(10, 5).as("renamed");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projected = exctractOperation("$project", dbObject);

		assertThat(projected.get("renamed"),
				is((Object) new BasicDBObject("$slice", Arrays.<Object> asList("$field", 5, 10))));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderCmpCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").cmp(10).as("cmp10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.cmp10.$cmp.[0]", "$field").containing("$project.cmp10.$cmp.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderEqCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").eq(10).as("eq10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.eq10.$eq.[0]", "$field").containing("$project.eq10.$eq.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderGtCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").gt(10).as("gt10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.gt10.$gt.[0]", "$field").containing("$project.gt10.$gt.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderGteCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").gte(10).as("gte10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.gte10.$gte.[0]", "$field").containing("$project.gte10.$gte.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderLtCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").lt(10).as("lt10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.lt10.$lt.[0]", "$field").containing("$project.lt10.$lt.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderLteCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").lte(10).as("lte10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.lte10.$lte.[0]", "$field").containing("$project.lte10.$lte.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderNeCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").ne(10).as("ne10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.ne10.$ne.[0]", "$field").containing("$project.ne10.$ne.[1]", 10));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetEquals() {

		DBObject agg = project("A", "B").and("A").equalsArray("B").as("sameElements")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, sameElements: { $setEquals: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetEqualsAggregationExpresssion() {

		DBObject agg = project("A", "B").and(SetOperators.arrayAsSet("A").isEqualTo("B")).as("sameElements")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, sameElements: { $setEquals: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetIntersection() {

		DBObject agg = project("A", "B").and("A").intersectsArrays("B").as("commonToBoth")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { A: 1, B: 1, commonToBoth: { $setIntersection: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetIntersectionAggregationExpresssion() {

		DBObject agg = project("A", "B").and(SetOperators.arrayAsSet("A").intersects("B")).as("commonToBoth")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { A: 1, B: 1, commonToBoth: { $setIntersection: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetUnion() {

		DBObject agg = project("A", "B").and("A").unionArrays("B").as("allValues").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, allValues: { $setUnion: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetUnionAggregationExpresssion() {

		DBObject agg = project("A", "B").and(SetOperators.arrayAsSet("A").union("B")).as("allValues")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, allValues: { $setUnion: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetDifference() {

		DBObject agg = project("A", "B").and("B").differenceToArray("A").as("inBOnly")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, inBOnly: { $setDifference: [ \"$B\", \"$A\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetDifferenceAggregationExpresssion() {

		DBObject agg = project("A", "B").and(SetOperators.arrayAsSet("B").differenceTo("A")).as("inBOnly")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, inBOnly: { $setDifference: [ \"$B\", \"$A\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetIsSubset() {

		DBObject agg = project("A", "B").and("A").subsetOfArray("B").as("aIsSubsetOfB")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, aIsSubsetOfB: { $setIsSubset: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetIsSubsetAggregationExpresssion() {

		DBObject agg = project("A", "B").and(SetOperators.arrayAsSet("A").isSubsetOf("B")).as("aIsSubsetOfB")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, aIsSubsetOfB: { $setIsSubset: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAnyElementTrue() {

		DBObject agg = project("responses").and("responses").anyElementInArrayTrue().as("isAnyTrue")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { responses: 1, isAnyTrue: { $anyElementTrue: [ \"$responses\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAnyElementTrueAggregationExpresssion() {

		DBObject agg = project("responses").and(SetOperators.arrayAsSet("responses").anyElementTrue()).as("isAnyTrue")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { responses: 1, isAnyTrue: { $anyElementTrue: [ \"$responses\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAllElementsTrue() {

		DBObject agg = project("responses").and("responses").allElementsInArrayTrue().as("isAllTrue")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { responses: 1, isAllTrue: { $allElementsTrue: [ \"$responses\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAllElementsTrueAggregationExpresssion() {

		DBObject agg = project("responses").and(SetOperators.arrayAsSet("responses").allElementsTrue()).as("isAllTrue")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { responses: 1, isAllTrue: { $allElementsTrue: [ \"$responses\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAbs() {

		DBObject agg = project().and("anyNumber").absoluteValue().as("absoluteValue")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { absoluteValue : { $abs:  \"$anyNumber\" }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAbsAggregationExpresssion() {

		DBObject agg = project()
				.and(
						ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).abs())
				.as("delta").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { delta: { $abs: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAddAggregationExpresssion() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("price").add("fee")).as("total")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse(" { $project: { total: { $add: [ \"$price\", \"$fee\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderCeil() {

		DBObject agg = project().and("anyNumber").ceil().as("ceilValue").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { ceilValue : { $ceil:  \"$anyNumber\" }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderCeilAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).ceil())
				.as("delta").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { delta: { $ceil: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDivide() {

		DBObject agg = project().and("value")
				.divide(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).as("result")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { result: { $divide: [ \"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDivideAggregationExpresssion() {

		DBObject agg = project()
				.and(ArithmeticOperators.valueOf("anyNumber")
						.divideBy(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON
				.parse("{ $project: { result: { $divide: [ \"$anyNumber\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderExp() {

		DBObject agg = project().and("value").exp().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $exp: \"$value\" } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderExpAggregationExpresssion() {

		DBObject agg = project()
				.and(
						ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).exp())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $exp: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderFloor() {

		DBObject agg = project().and("value").floor().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $floor: \"$value\" } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderFloorAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).floor())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $floor: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLn() {

		DBObject agg = project().and("value").ln().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $ln: \"$value\"} }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLnAggregationExpresssion() {

		DBObject agg = project()
				.and(ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).ln())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $ln: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLog() {

		DBObject agg = project().and("value").log(2).as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $log: [ \"$value\", 2] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLogAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).log(2))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $log: [ { $subtract: [ \"$start\", \"$end\" ] }, 2] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLog10() {

		DBObject agg = project().and("value").log10().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $log10: \"$value\" } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLog10AggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).log10())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $log10: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMod() {

		DBObject agg = project().and("value").mod(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end")))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { result: { $mod: [\"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderModAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).mod(2))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $mod: [{ $subtract: [ \"$start\", \"$end\" ] }, 2] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMultiply() {

		DBObject agg = project().and("value")
				.multiply(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).as("result")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(
				JSON.parse("{ $project: { result: { $multiply: [\"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMultiplyAggregationExpresssion() {

		DBObject agg = project()
				.and(ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end")))
						.multiplyBy(2).multiplyBy("refToAnotherNumber"))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse(
				"{ $project: { result: { $multiply: [{ $subtract: [ \"$start\", \"$end\" ] }, 2, \"$refToAnotherNumber\"] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderPow() {

		DBObject agg = project().and("value").pow(2).as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $pow: [\"$value\", 2] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderPowAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).pow(2))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $pow: [{ $subtract: [ \"$start\", \"$end\" ] }, 2] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSqrt() {

		DBObject agg = project().and("value").sqrt().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $sqrt: \"$value\" } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSqrtAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).sqrt())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $sqrt: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSubtract() {

		DBObject agg = project().and("numericField").minus(AggregationFunctionExpressions.SIZE.of(field("someArray")))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { result: { $subtract: [ \"$numericField\", { $size : [\"$someArray\"]}] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSubtractAggregationExpresssion() {

		DBObject agg = project()
				.and(ArithmeticOperators.valueOf("numericField")
						.subtract(AggregationFunctionExpressions.SIZE.of(field("someArray"))))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { result: { $subtract: [ \"$numericField\", { $size : [\"$someArray\"]}] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderTrunc() {

		DBObject agg = project().and("value").trunc().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result : { $trunc: \"$value\" }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderTruncAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).trunc())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $trunc: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderConcat() {

		DBObject agg = project().and("item").concat(" - ", field("description")).as("itemDescription")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { itemDescription: { $concat: [ \"$item\", \" - \", \"$description\" ] } } }")));

	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderConcatAggregationExpression() {

		DBObject agg = project().and(StringOperators.valueOf("item").concat(" - ").concatValueOf("description"))
				.as("itemDescription").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { itemDescription: { $concat: [ \"$item\", \" - \", \"$description\" ] } } }")));

	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSubstr() {

		DBObject agg = project().and("quarter").substring(0, 2).as("yearSubstring").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { yearSubstring: { $substr: [ \"$quarter\", 0, 2 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSubstrAggregationExpression() {

		DBObject agg = project().and(StringOperators.valueOf("quarter").substring(0, 2)).as("yearSubstring")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { yearSubstring: { $substr: [ \"$quarter\", 0, 2 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderToLower() {

		DBObject agg = project().and("item").toLower().as("item").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { item: { $toLower: \"$item\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderToLowerAggregationExpression() {

		DBObject agg = project().and(StringOperators.valueOf("item").toLower()).as("item")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { item: { $toLower: \"$item\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderToUpper() {

		DBObject agg = project().and("item").toUpper().as("item").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { item: { $toUpper: \"$item\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderToUpperAggregationExpression() {

		DBObject agg = project().and(StringOperators.valueOf("item").toUpper()).as("item")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { item: { $toUpper: \"$item\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderStrCaseCmp() {

		DBObject agg = project().and("quarter").strCaseCmp("13q4").as("comparisonResult")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { comparisonResult: { $strcasecmp: [ \"$quarter\", \"13q4\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderStrCaseCmpAggregationExpression() {

		DBObject agg = project().and(StringOperators.valueOf("quarter").strCaseCmp("13q4")).as("comparisonResult")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { comparisonResult: { $strcasecmp: [ \"$quarter\", \"13q4\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderArrayElementAt() {

		DBObject agg = project().and("favorites").arrayElementAt(0).as("first").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { first: { $arrayElemAt: [ \"$favorites\", 0 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderArrayElementAtAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("favorites").elementAt(0)).as("first")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { first: { $arrayElemAt: [ \"$favorites\", 0 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderConcatArrays() {

		DBObject agg = project().and("instock").concatArrays("ordered").as("items").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { items: { $concatArrays: [ \"$instock\", \"$ordered\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderConcatArraysAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("instock").concat("ordered")).as("items")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { items: { $concatArrays: [ \"$instock\", \"$ordered\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderIsArray() {

		DBObject agg = project().and("instock").isArray().as("isAnArray").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { isAnArray: { $isArray: \"$instock\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderIsArrayAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("instock").isArray()).as("isAnArray")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { isAnArray: { $isArray: \"$instock\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSizeAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("instock").length()).as("arraySize")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { arraySize: { $size: \"$instock\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSliceAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("favorites").slice().itemCount(3)).as("threeFavorites")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { threeFavorites: { $slice: [ \"$favorites\", 3 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSliceWithPositionAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("favorites").slice().offset(2).itemCount(3))
				.as("threeFavorites").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { threeFavorites: { $slice: [ \"$favorites\", 2, 3 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLiteral() {

		DBObject agg = project().and("$1").asLiteral().as("literalOnly").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { literalOnly: { $literal:  \"$1\"} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLiteralAggregationExpression() {

		DBObject agg = project().and(LiteralOperators.valueOf("$1").asLiteral()).as("literalOnly")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { literalOnly: { $literal:  \"$1\"} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDayOfYearAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").dayOfYear()).as("dayOfYear")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { dayOfYear: { $dayOfYear: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDayOfMonthAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").dayOfMonth()).as("day")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { day: { $dayOfMonth: \"$date\" }} }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDayOfWeekAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").dayOfWeek()).as("dayOfWeek")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { dayOfWeek: { $dayOfWeek: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderYearAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").year()).as("year")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { year: { $year: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMonthAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").month()).as("month")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { month: { $month: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderWeekAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").week()).as("week")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { week: { $week: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderHourAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").hour()).as("hour")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { hour: { $hour: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMinuteAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").minute()).as("minute")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { minute: { $minute: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSecondAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").second()).as("second")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { second: { $second: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMillisecondAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").millisecond()).as("msec")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { msec: { $millisecond: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDateToString() {

		DBObject agg = project().and("date").dateAsFormattedString("%H:%M:%S:%L").as("time")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { time: { $dateToString: { format: \"%H:%M:%S:%L\", date: \"$date\" } } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDateToStringAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").toString("%H:%M:%S:%L")).as("time")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { time: { $dateToString: { format: \"%H:%M:%S:%L\", date: \"$date\" } } } }")));
	}

	private static DBObject exctractOperation(String field, DBObject fromProjectClause) {
		return (DBObject) fromProjectClause.get(field);
	}
}
