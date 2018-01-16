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

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.aggregation.AggregationFunctionExpressions.*;
import static org.springframework.data.mongodb.core.aggregation.Fields.*;
import static org.springframework.data.mongodb.core.aggregation.VariableOperators.Let.ExpressionVariable.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;
import static org.springframework.data.mongodb.util.DBObjectUtils.*;

import java.util.Arrays;
import java.util.List;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Reduce;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Reduce.PropertyExpression;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Reduce.Variable;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Slice;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Switch.CaseOperator;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation.ProjectionOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.StringOperators.Concat;
import org.springframework.data.mongodb.core.aggregation.VariableOperators.Let.ExpressionVariable;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Unit tests for {@link ProjectionOperation}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ProjectionOperationUnitTests {

	static final String MOD = "$mod";
	static final String ADD = "$add";
	static final String SUBTRACT = "$subtract";
	static final String MULTIPLY = "$multiply";
	static final String DIVIDE = "$divide";
	static final String PROJECT = "$project";

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-586
	public void rejectsNullFields() {
		new ProjectionOperation(null);
	}

	@Test // DATAMONGO-586
	public void declaresBackReferenceCorrectly() {

		ProjectionOperation operation = new ProjectionOperation();
		operation = operation.and("prop").previousOperation();

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);
		assertThat(projectClause.get("prop"), is((Object) Fields.UNDERSCORE_ID_REF));
	}

	@Test // DATAMONGO-586
	public void alwaysUsesExplicitReference() {

		ProjectionOperation operation = new ProjectionOperation(Fields.fields("foo").and("bar", "foobar"));

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);

		assertThat(projectClause.get("foo"), is((Object) 1));
		assertThat(projectClause.get("bar"), is((Object) "$foobar"));
	}

	@Test // DATAMONGO-586
	public void aliasesSimpleFieldProjection() {

		ProjectionOperation operation = new ProjectionOperation();

		DBObject dbObject = operation.and("foo").as("bar").toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);

		assertThat(projectClause.get("bar"), is((Object) "$foo"));
	}

	@Test // DATAMONGO-586
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

	@Test // DATAMONGO-586
	public void arithmenticProjectionOperationWithoutAlias() {

		String fieldName = "a";
		ProjectionOperationBuilder operation = new ProjectionOperation().and(fieldName).plus(1);
		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);
		DBObject oper = exctractOperation(fieldName, projectClause);

		assertThat(oper.containsField(ADD), is(true));
		assertThat(oper.get(ADD), is((Object) Arrays.<Object> asList("$a", 1)));
	}

	@Test // DATAMONGO-586
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

	@Test // DATAMONGO-586
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

	@Test // DATAMONGO-586
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

	@Test // DATAMONGO-586
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

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-586
	public void arithmenticProjectionOperationDivideByZeroException() {

		new ProjectionOperation().and("a").divide(0);
	}

	@Test // DATAMONGO-586
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

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-758
	public void excludeShouldThrowExceptionForFieldsOtherThanUnderscoreId() {

		new ProjectionOperation().andExclude("foo");
	}

	@Test // DATAMONGO-758
	public void excludeShouldAllowExclusionOfUnderscoreId() {

		ProjectionOperation projectionOp = new ProjectionOperation().andExclude(Fields.UNDERSCORE_ID);
		DBObject dbObject = projectionOp.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectTestUtils.getAsDBObject(dbObject, PROJECT);
		assertThat((Integer) projectClause.get(Fields.UNDERSCORE_ID), is(0));
	}

	@Test // DATAMONGO-757
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

	@Test // DATAMONGO-769
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

	@Test // DATAMONGO-774
	public void projectionExpressions() {

		ProjectionOperation operation = Aggregation.project() //
				.andExpression("(netPrice + surCharge) * taxrate * [0]", 2).as("grossSalesPrice") //
				.and("foo").as("bar"); //

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(dbObject.toString(), is(
				"{ \"$project\" : { \"grossSalesPrice\" : { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\" , 2]} , \"bar\" : \"$foo\"}}"));
	}

	@Test // DATAMONGO-975
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

	@Test // DATAMONGO-975
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

	@Test // DATAMONGO-979
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

	@Test // DATAMONGO-979
	public void shouldRenderGenericSizeExpressionInProjection() {

		ProjectionOperation operation = Aggregation //
				.project() //
				.and(SIZE.of(field("tags"))) //
				.as("tags_count");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);

		DBObject projected = exctractOperation("$project", dbObject);
		assertThat(projected.get("tags_count"), is((Object) new BasicDBObject("$size", Arrays.asList("$tags"))));
	}

	@Test // DATAMONGO-1457
	public void shouldRenderSliceCorrectly() throws Exception {

		ProjectionOperation operation = Aggregation.project().and("field").slice(10).as("renamed");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projected = exctractOperation("$project", dbObject);

		assertThat(projected.get("renamed"),
				is((Object) new BasicDBObject("$slice", Arrays.<Object> asList("$field", 10))));
	}

	@Test // DATAMONGO-1457
	public void shouldRenderSliceWithPositionCorrectly() throws Exception {

		ProjectionOperation operation = Aggregation.project().and("field").slice(10, 5).as("renamed");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projected = exctractOperation("$project", dbObject);

		assertThat(projected.get("renamed"),
				is((Object) new BasicDBObject("$slice", Arrays.<Object> asList("$field", 5, 10))));
	}

	@Test // DATAMONGO-784
	public void shouldRenderCmpCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").cmp(10).as("cmp10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.cmp10.$cmp.[0]", "$field").containing("$project.cmp10.$cmp.[1]", 10));
	}

	@Test // DATAMONGO-784
	public void shouldRenderEqCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").eq(10).as("eq10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.eq10.$eq.[0]", "$field").containing("$project.eq10.$eq.[1]", 10));
	}

	@Test // DATAMONGO-784
	public void shouldRenderGtCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").gt(10).as("gt10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.gt10.$gt.[0]", "$field").containing("$project.gt10.$gt.[1]", 10));
	}

	@Test // DATAMONGO-784
	public void shouldRenderGteCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").gte(10).as("gte10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.gte10.$gte.[0]", "$field").containing("$project.gte10.$gte.[1]", 10));
	}

	@Test // DATAMONGO-784
	public void shouldRenderLtCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").lt(10).as("lt10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.lt10.$lt.[0]", "$field").containing("$project.lt10.$lt.[1]", 10));
	}

	@Test // DATAMONGO-784
	public void shouldRenderLteCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").lte(10).as("lte10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.lte10.$lte.[0]", "$field").containing("$project.lte10.$lte.[1]", 10));
	}

	@Test // DATAMONGO-784
	public void shouldRenderNeCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").ne(10).as("ne10");

		assertThat(operation.toDBObject(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.ne10.$ne.[0]", "$field").containing("$project.ne10.$ne.[1]", 10));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSetEquals() {

		DBObject agg = project("A", "B").and("A").equalsArrays("B").as("sameElements")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, sameElements: { $setEquals: [ \"$A\", \"$B\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSetEqualsAggregationExpresssion() {

		DBObject agg = project("A", "B").and(SetOperators.arrayAsSet("A").isEqualTo("B")).as("sameElements")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, sameElements: { $setEquals: [ \"$A\", \"$B\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSetIntersection() {

		DBObject agg = project("A", "B").and("A").intersectsArrays("B").as("commonToBoth")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { A: 1, B: 1, commonToBoth: { $setIntersection: [ \"$A\", \"$B\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSetIntersectionAggregationExpresssion() {

		DBObject agg = project("A", "B").and(SetOperators.arrayAsSet("A").intersects("B")).as("commonToBoth")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { A: 1, B: 1, commonToBoth: { $setIntersection: [ \"$A\", \"$B\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSetUnion() {

		DBObject agg = project("A", "B").and("A").unionArrays("B").as("allValues").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, allValues: { $setUnion: [ \"$A\", \"$B\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSetUnionAggregationExpresssion() {

		DBObject agg = project("A", "B").and(SetOperators.arrayAsSet("A").union("B")).as("allValues")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, allValues: { $setUnion: [ \"$A\", \"$B\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSetDifference() {

		DBObject agg = project("A", "B").and("B").differenceToArray("A").as("inBOnly")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, inBOnly: { $setDifference: [ \"$B\", \"$A\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSetDifferenceAggregationExpresssion() {

		DBObject agg = project("A", "B").and(SetOperators.arrayAsSet("B").differenceTo("A")).as("inBOnly")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, inBOnly: { $setDifference: [ \"$B\", \"$A\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSetIsSubset() {

		DBObject agg = project("A", "B").and("A").subsetOfArray("B").as("aIsSubsetOfB")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, aIsSubsetOfB: { $setIsSubset: [ \"$A\", \"$B\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSetIsSubsetAggregationExpresssion() {

		DBObject agg = project("A", "B").and(SetOperators.arrayAsSet("A").isSubsetOf("B")).as("aIsSubsetOfB")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { A: 1, B: 1, aIsSubsetOfB: { $setIsSubset: [ \"$A\", \"$B\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderAnyElementTrue() {

		DBObject agg = project("responses").and("responses").anyElementInArrayTrue().as("isAnyTrue")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { responses: 1, isAnyTrue: { $anyElementTrue: [ \"$responses\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderAnyElementTrueAggregationExpresssion() {

		DBObject agg = project("responses").and(SetOperators.arrayAsSet("responses").anyElementTrue()).as("isAnyTrue")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { responses: 1, isAnyTrue: { $anyElementTrue: [ \"$responses\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderAllElementsTrue() {

		DBObject agg = project("responses").and("responses").allElementsInArrayTrue().as("isAllTrue")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { responses: 1, isAllTrue: { $allElementsTrue: [ \"$responses\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderAllElementsTrueAggregationExpresssion() {

		DBObject agg = project("responses").and(SetOperators.arrayAsSet("responses").allElementsTrue()).as("isAllTrue")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { responses: 1, isAllTrue: { $allElementsTrue: [ \"$responses\" ] }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderAbs() {

		DBObject agg = project().and("anyNumber").absoluteValue().as("absoluteValue")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { absoluteValue : { $abs:  \"$anyNumber\" }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderAbsAggregationExpresssion() {

		DBObject agg = project()
				.and(
						ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).abs())
				.as("delta").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { delta: { $abs: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderAddAggregationExpresssion() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("price").add("fee")).as("total")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse(" { $project: { total: { $add: [ \"$price\", \"$fee\" ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderCeil() {

		DBObject agg = project().and("anyNumber").ceil().as("ceilValue").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { ceilValue : { $ceil:  \"$anyNumber\" }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderCeilAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).ceil())
				.as("delta").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { delta: { $ceil: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderDivide() {

		DBObject agg = project().and("value")
				.divide(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).as("result")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { result: { $divide: [ \"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderDivideAggregationExpresssion() {

		DBObject agg = project()
				.and(ArithmeticOperators.valueOf("anyNumber")
						.divideBy(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON
				.parse("{ $project: { result: { $divide: [ \"$anyNumber\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderExp() {

		DBObject agg = project().and("value").exp().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $exp: \"$value\" } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderExpAggregationExpresssion() {

		DBObject agg = project()
				.and(
						ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).exp())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $exp: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderFloor() {

		DBObject agg = project().and("value").floor().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $floor: \"$value\" } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderFloorAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).floor())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $floor: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLn() {

		DBObject agg = project().and("value").ln().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $ln: \"$value\"} }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLnAggregationExpresssion() {

		DBObject agg = project()
				.and(ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).ln())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $ln: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLog() {

		DBObject agg = project().and("value").log(2).as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $log: [ \"$value\", 2] } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLogAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).log(2))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $log: [ { $subtract: [ \"$start\", \"$end\" ] }, 2] } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLog10() {

		DBObject agg = project().and("value").log10().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $log10: \"$value\" } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLog10AggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).log10())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $log10: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderMod() {

		DBObject agg = project().and("value").mod(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end")))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { result: { $mod: [\"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderModAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).mod(2))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $mod: [{ $subtract: [ \"$start\", \"$end\" ] }, 2] } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderMultiply() {

		DBObject agg = project().and("value")
				.multiply(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).as("result")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(
				JSON.parse("{ $project: { result: { $multiply: [\"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderMultiplyAggregationExpresssion() {

		DBObject agg = project()
				.and(ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end")))
						.multiplyBy(2).multiplyBy("refToAnotherNumber"))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse(
				"{ $project: { result: { $multiply: [{ $subtract: [ \"$start\", \"$end\" ] }, 2, \"$refToAnotherNumber\"] } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderPow() {

		DBObject agg = project().and("value").pow(2).as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $pow: [\"$value\", 2] } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderPowAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).pow(2))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $pow: [{ $subtract: [ \"$start\", \"$end\" ] }, 2] } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSqrt() {

		DBObject agg = project().and("value").sqrt().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $sqrt: \"$value\" } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSqrtAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).sqrt())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $sqrt: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSubtract() {

		DBObject agg = project().and("numericField").minus(AggregationFunctionExpressions.SIZE.of(field("someArray")))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { result: { $subtract: [ \"$numericField\", { $size : [\"$someArray\"]}] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSubtractAggregationExpresssion() {

		DBObject agg = project()
				.and(ArithmeticOperators.valueOf("numericField")
						.subtract(AggregationFunctionExpressions.SIZE.of(field("someArray"))))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { result: { $subtract: [ \"$numericField\", { $size : [\"$someArray\"]}] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderTrunc() {

		DBObject agg = project().and("value").trunc().as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result : { $trunc: \"$value\" }}}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderTruncAggregationExpresssion() {

		DBObject agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).trunc())
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $trunc: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderConcat() {

		DBObject agg = project().and("item").concat(" - ", field("description")).as("itemDescription")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { itemDescription: { $concat: [ \"$item\", \" - \", \"$description\" ] } } }")));

	}

	@Test // DATAMONGO-1536
	public void shouldRenderConcatAggregationExpression() {

		DBObject agg = project().and(StringOperators.valueOf("item").concat(" - ").concatValueOf("description"))
				.as("itemDescription").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { itemDescription: { $concat: [ \"$item\", \" - \", \"$description\" ] } } }")));

	}

	@Test // DATAMONGO-1536
	public void shouldRenderSubstr() {

		DBObject agg = project().and("quarter").substring(0, 2).as("yearSubstring").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { yearSubstring: { $substr: [ \"$quarter\", 0, 2 ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSubstrAggregationExpression() {

		DBObject agg = project().and(StringOperators.valueOf("quarter").substring(0, 2)).as("yearSubstring")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { yearSubstring: { $substr: [ \"$quarter\", 0, 2 ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderToLower() {

		DBObject agg = project().and("item").toLower().as("item").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { item: { $toLower: \"$item\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderToLowerAggregationExpression() {

		DBObject agg = project().and(StringOperators.valueOf("item").toLower()).as("item")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { item: { $toLower: \"$item\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderToUpper() {

		DBObject agg = project().and("item").toUpper().as("item").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { item: { $toUpper: \"$item\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderToUpperAggregationExpression() {

		DBObject agg = project().and(StringOperators.valueOf("item").toUpper()).as("item")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { item: { $toUpper: \"$item\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderStrCaseCmp() {

		DBObject agg = project().and("quarter").strCaseCmp("13q4").as("comparisonResult")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { comparisonResult: { $strcasecmp: [ \"$quarter\", \"13q4\" ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderStrCaseCmpAggregationExpression() {

		DBObject agg = project().and(StringOperators.valueOf("quarter").strCaseCmp("13q4")).as("comparisonResult")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { comparisonResult: { $strcasecmp: [ \"$quarter\", \"13q4\" ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderArrayElementAt() {

		DBObject agg = project().and("favorites").arrayElementAt(0).as("first").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { first: { $arrayElemAt: [ \"$favorites\", 0 ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderArrayElementAtAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("favorites").elementAt(0)).as("first")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { first: { $arrayElemAt: [ \"$favorites\", 0 ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderConcatArrays() {

		DBObject agg = project().and("instock").concatArrays("ordered").as("items").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { items: { $concatArrays: [ \"$instock\", \"$ordered\" ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderConcatArraysAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("instock").concat("ordered")).as("items")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { items: { $concatArrays: [ \"$instock\", \"$ordered\" ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderIsArray() {

		DBObject agg = project().and("instock").isArray().as("isAnArray").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { isAnArray: { $isArray: \"$instock\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderIsArrayAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("instock").isArray()).as("isAnArray")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { isAnArray: { $isArray: \"$instock\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSizeAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("instock").length()).as("arraySize")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { arraySize: { $size: \"$instock\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSliceAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("favorites").slice().itemCount(3)).as("threeFavorites")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { threeFavorites: { $slice: [ \"$favorites\", 3 ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSliceWithPositionAggregationExpression() {

		DBObject agg = project().and(ArrayOperators.arrayOf("favorites").slice().offset(2).itemCount(3))
				.as("threeFavorites").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { threeFavorites: { $slice: [ \"$favorites\", 2, 3 ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLiteral() {

		DBObject agg = project().and("$1").asLiteral().as("literalOnly").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { literalOnly: { $literal:  \"$1\"} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLiteralAggregationExpression() {

		DBObject agg = project().and(LiteralOperators.valueOf("$1").asLiteral()).as("literalOnly")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { literalOnly: { $literal:  \"$1\"} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderDayOfYearAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").dayOfYear()).as("dayOfYear")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { dayOfYear: { $dayOfYear: \"$date\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderDayOfMonthAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").dayOfMonth()).as("day")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { day: { $dayOfMonth: \"$date\" }} }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderDayOfWeekAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").dayOfWeek()).as("dayOfWeek")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { dayOfWeek: { $dayOfWeek: \"$date\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderYearAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").year()).as("year")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { year: { $year: \"$date\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderMonthAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").month()).as("month")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { month: { $month: \"$date\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderWeekAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").week()).as("week")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { week: { $week: \"$date\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderHourAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").hour()).as("hour")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { hour: { $hour: \"$date\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderMinuteAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").minute()).as("minute")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { minute: { $minute: \"$date\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSecondAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").second()).as("second")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { second: { $second: \"$date\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderMillisecondAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").millisecond()).as("msec")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { msec: { $millisecond: \"$date\" } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderDateToString() {

		DBObject agg = project().and("date").dateAsFormattedString("%H:%M:%S:%L").as("time")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { time: { $dateToString: { format: \"%H:%M:%S:%L\", date: \"$date\" } } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderDateToStringAggregationExpression() {

		DBObject agg = project().and(DateOperators.dateOf("date").toString("%H:%M:%S:%L")).as("time")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { time: { $dateToString: { format: \"%H:%M:%S:%L\", date: \"$date\" } } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSumAggregationExpression() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("quizzes").sum()).as("quizTotal")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { quizTotal: { $sum: \"$quizzes\"} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderSumWithMultipleArgsAggregationExpression() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("final").sum().and("midterm")).as("examTotal")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: {  examTotal: { $sum: [ \"$final\", \"$midterm\" ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderAvgAggregationExpression() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("quizzes").avg()).as("quizAvg")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { quizAvg: { $avg: \"$quizzes\"} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderAvgWithMultipleArgsAggregationExpression() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("final").avg().and("midterm")).as("examAvg")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: {  examAvg: { $avg: [ \"$final\", \"$midterm\" ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderMaxAggregationExpression() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("quizzes").max()).as("quizMax")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { quizMax: { $max: \"$quizzes\"} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderMaxWithMultipleArgsAggregationExpression() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("final").max().and("midterm")).as("examMax")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: {  examMax: { $max: [ \"$final\", \"$midterm\" ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderMinAggregationExpression() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("quizzes").min()).as("quizMin")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { quizMin: { $min: \"$quizzes\"} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderMinWithMultipleArgsAggregationExpression() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("final").min().and("midterm")).as("examMin")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: {  examMin: { $min: [ \"$final\", \"$midterm\" ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderStdDevPopAggregationExpression() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("scores").stdDevPop()).as("stdDev")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { stdDev: { $stdDevPop: \"$scores\"} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderStdDevSampAggregationExpression() {

		DBObject agg = project().and(ArithmeticOperators.valueOf("scores").stdDevSamp()).as("stdDev")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { stdDev: { $stdDevSamp: \"$scores\"} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderCmpAggregationExpression() {

		DBObject agg = project().and(ComparisonOperators.valueOf("qty").compareToValue(250)).as("cmp250")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { cmp250: { $cmp: [\"$qty\", 250]} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderEqAggregationExpression() {

		DBObject agg = project().and(ComparisonOperators.valueOf("qty").equalToValue(250)).as("eq250")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { eq250: { $eq: [\"$qty\", 250]} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderGtAggregationExpression() {

		DBObject agg = project().and(ComparisonOperators.valueOf("qty").greaterThanValue(250)).as("gt250")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { gt250: { $gt: [\"$qty\", 250]} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderGteAggregationExpression() {

		DBObject agg = project().and(ComparisonOperators.valueOf("qty").greaterThanEqualToValue(250)).as("gte250")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { gte250: { $gte: [\"$qty\", 250]} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLtAggregationExpression() {

		DBObject agg = project().and(ComparisonOperators.valueOf("qty").lessThanValue(250)).as("lt250")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { lt250: { $lt: [\"$qty\", 250]} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLteAggregationExpression() {

		DBObject agg = project().and(ComparisonOperators.valueOf("qty").lessThanEqualToValue(250)).as("lte250")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { lte250: { $lte: [\"$qty\", 250]} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderNeAggregationExpression() {

		DBObject agg = project().and(ComparisonOperators.valueOf("qty").notEqualToValue(250)).as("ne250")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { ne250: { $ne: [\"$qty\", 250]} } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLogicAndAggregationExpression() {

		DBObject agg = project()
				.and(BooleanOperators.valueOf(ComparisonOperators.valueOf("qty").greaterThanValue(100))
						.and(ComparisonOperators.valueOf("qty").lessThanValue(250)))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(
				JSON.parse("{ $project: { result: { $and: [ { $gt: [ \"$qty\", 100 ] }, { $lt: [ \"$qty\", 250 ] } ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderLogicOrAggregationExpression() {

		DBObject agg = project()
				.and(BooleanOperators.valueOf(ComparisonOperators.valueOf("qty").greaterThanValue(250))
						.or(ComparisonOperators.valueOf("qty").lessThanValue(200)))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(
				JSON.parse("{ $project: { result: { $or: [ { $gt: [ \"$qty\", 250 ] }, { $lt: [ \"$qty\", 200 ] } ] } } }")));
	}

	@Test // DATAMONGO-1536
	public void shouldRenderNotAggregationExpression() {

		DBObject agg = project().and(BooleanOperators.not(ComparisonOperators.valueOf("qty").greaterThanValue(250)))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $not: [ { $gt: [ \"$qty\", 250 ] } ] } } }")));
	}

	@Test // DATAMONGO-1540
	public void shouldRenderMapAggregationExpression() {

		DBObject agg = Aggregation.project()
				.and(VariableOperators.mapItemsOf("quizzes").as("grade")
						.andApply(AggregationFunctionExpressions.ADD.of(field("grade"), 2)))
				.as("adjustedGrades").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse(
				"{ $project:{ adjustedGrades:{ $map: { input: \"$quizzes\", as: \"grade\",in: { $add: [ \"$$grade\", 2 ] }}}}}")));
	}

	@Test // DATAMONGO-1540
	public void shouldRenderMapAggregationExpressionOnExpression() {

		DBObject agg = Aggregation.project()
				.and(VariableOperators.mapItemsOf(AggregationFunctionExpressions.SIZE.of("foo")).as("grade")
						.andApply(AggregationFunctionExpressions.ADD.of(field("grade"), 2)))
				.as("adjustedGrades").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse(
				"{ $project:{ adjustedGrades:{ $map: { input: { $size : [\"foo\"]}, as: \"grade\",in: { $add: [ \"$$grade\", 2 ] }}}}}")));
	}

	@Test // DATAMONGO-861, DATAMONGO-1542
	public void shouldRenderIfNullConditionAggregationExpression() {

		DBObject agg = project().and(
				ConditionalOperators.ifNull(ArrayOperators.arrayOf("array").elementAt(1)).then("a more sophisticated value"))
				.as("result").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse(
				"{ $project: { result: { $ifNull: [ { $arrayElemAt: [\"$array\", 1] }, \"a more sophisticated value\" ] } } }")));
	}

	@Test // DATAMONGO-1542
	public void shouldRenderIfNullValueAggregationExpression() {

		DBObject agg = project()
				.and(ConditionalOperators.ifNull("field").then(ArrayOperators.arrayOf("array").elementAt(1))).as("result")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project: { result: { $ifNull: [ \"$field\", { $arrayElemAt: [\"$array\", 1] } ] } } }")));
	}

	@Test // DATAMONGO-861, DATAMONGO-1542
	public void fieldReplacementIfNullShouldRenderCorrectly() {

		DBObject agg = project().and(ConditionalOperators.ifNull("optional").thenValueOf("$never-null")).as("result")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $project: { result: { $ifNull: [ \"$optional\", \"$never-null\" ] } } }")));
	}

	@Test // DATAMONGO-1538
	public void shouldRenderLetExpressionCorrectly() {

		DBObject agg = Aggregation.project()
				.and(VariableOperators
						.define(
								newVariable("total")
										.forExpression(AggregationFunctionExpressions.ADD.of(Fields.field("price"), Fields.field("tax"))),
								newVariable("discounted")
										.forExpression(ConditionalOperators.Cond.when("applyDiscount").then(0.9D).otherwise(1.0D)))
						.andApply(AggregationFunctionExpressions.MULTIPLY.of(Fields.field("total"), Fields.field("discounted")))) //
				.as("finalTotal").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project:{  \"finalTotal\" : { \"$let\": {" + //
						"\"vars\": {" + //
						"\"total\": { \"$add\": [ \"$price\", \"$tax\" ] }," + //
						"\"discounted\": { \"$cond\": { \"if\": \"$applyDiscount\", \"then\": 0.9, \"else\": 1.0 } }" + //
						"}," + //
						"\"in\": { \"$multiply\": [ \"$$total\", \"$$discounted\" ] }" + //
						"}}}}")));
	}

	@Test // DATAMONGO-1538
	public void shouldRenderLetExpressionCorrectlyWhenUsingLetOnProjectionBuilder() {

		ExpressionVariable var1 = newVariable("total")
				.forExpression(AggregationFunctionExpressions.ADD.of(Fields.field("price"), Fields.field("tax")));

		ExpressionVariable var2 = newVariable("discounted")
				.forExpression(ConditionalOperators.Cond.when("applyDiscount").then(0.9D).otherwise(1.0D));

		DBObject agg = Aggregation.project().and("foo")
				.let(Arrays.asList(var1, var2),
						AggregationFunctionExpressions.MULTIPLY.of(Fields.field("total"), Fields.field("discounted")))
				.as("finalTotal").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $project:{ \"finalTotal\" : { \"$let\": {" + //
						"\"vars\": {" + //
						"\"total\": { \"$add\": [ \"$price\", \"$tax\" ] }," + //
						"\"discounted\": { \"$cond\": { \"if\": \"$applyDiscount\", \"then\": 0.9, \"else\": 1.0 } }" + //
						"}," + //
						"\"in\": { \"$multiply\": [ \"$$total\", \"$$discounted\" ] }" + //
						"}}}}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderIndexOfBytesCorrectly() {

		DBObject agg = project().and(StringOperators.valueOf("item").indexOf("foo")).as("byteLocation")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				Matchers.is(JSON.parse("{ $project: { byteLocation: { $indexOfBytes: [ \"$item\", \"foo\" ] } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderIndexOfBytesWithRangeCorrectly() {

		DBObject agg = project().and(StringOperators.valueOf("item").indexOf("foo").within(new Range<Long>(5L, 9L)))
				.as("byteLocation").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, isBsonObject().containing("$project.byteLocation.$indexOfBytes.[2]", 5L)
				.containing("$project.byteLocation.$indexOfBytes.[3]", 9L));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderIndexOfCPCorrectly() {

		DBObject agg = project().and(StringOperators.valueOf("item").indexOfCP("foo")).as("cpLocation")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project: { cpLocation: { $indexOfCP: [ \"$item\", \"foo\" ] } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderIndexOfCPWithRangeCorrectly() {

		DBObject agg = project().and(StringOperators.valueOf("item").indexOfCP("foo").within(new Range<Long>(5L, 9L)))
				.as("cpLocation").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, isBsonObject().containing("$project.cpLocation.$indexOfCP.[2]", 5L)
				.containing("$project.cpLocation.$indexOfCP.[3]", 9L));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderSplitCorrectly() {

		DBObject agg = project().and(StringOperators.valueOf("city").split(", ")).as("city_state")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project : { city_state : { $split: [\"$city\", \", \"] }} }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderStrLenBytesCorrectly() {

		DBObject agg = project().and(StringOperators.valueOf("name").length()).as("length")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project : { \"length\": { $strLenBytes: \"$name\" } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderStrLenCPCorrectly() {

		DBObject agg = project().and(StringOperators.valueOf("name").lengthCP()).as("length")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project : { \"length\": { $strLenCP: \"$name\" } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderSubstrCPCorrectly() {

		DBObject agg = project().and(StringOperators.valueOf("quarter").substringCP(0, 2)).as("yearSubstring")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project : { yearSubstring: { $substrCP: [ \"$quarter\", 0, 2 ] } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderIndexOfArrayCorrectly() {

		DBObject agg = project().and(ArrayOperators.arrayOf("items").indexOf(2)).as("index")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project : { index: { $indexOfArray: [ \"$items\", 2 ] } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderRangeCorrectly() {

		DBObject agg = project().and(ArrayOperators.RangeOperator.rangeStartingAt(0L).to("distance").withStepSize(25L))
				.as("rest_stops").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, isBsonObject().containing("$project.rest_stops.$range.[0]", 0L)
				.containing("$project.rest_stops.$range.[1]", "$distance").containing("$project.rest_stops.$range.[2]", 25L));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderReverseArrayCorrectly() {

		DBObject agg = project().and(ArrayOperators.arrayOf("favorites").reverse()).as("reverseFavorites")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project : { reverseFavorites: { $reverseArray: \"$favorites\" } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderReduceWithSimpleObjectCorrectly() {

		DBObject agg = project()
				.and(ArrayOperators.arrayOf("probabilityArr")
						.reduce(ArithmeticOperators.valueOf("$$value").multiplyBy("$$this")).startingWith(1))
				.as("results").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse(
				"{ $project : { \"results\": { $reduce: { input: \"$probabilityArr\", initialValue: 1, in: { $multiply: [ \"$$value\", \"$$this\" ] } } } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderReduceWithComplexObjectCorrectly() {

		PropertyExpression sum = PropertyExpression.property("sum").definedAs(
				ArithmeticOperators.valueOf(Variable.VALUE.referringTo("sum").getName()).add(Variable.THIS.getName()));
		PropertyExpression product = PropertyExpression.property("product").definedAs(ArithmeticOperators
				.valueOf(Variable.VALUE.referringTo("product").getName()).multiplyBy(Variable.THIS.getName()));

		DBObject agg = project()
				.and(ArrayOperators.arrayOf("probabilityArr").reduce(sum, product)
						.startingWith(new BasicDBObjectBuilder().add("sum", 5).add("product", 2).get()))
				.as("results").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse(
				"{ $project : { \"results\": { $reduce: { input: \"$probabilityArr\", initialValue:  { \"sum\" : 5 , \"product\" : 2} , in: { \"sum\": { $add : [\"$$value.sum\", \"$$this\"] }, \"product\": { $multiply: [ \"$$value.product\", \"$$this\" ] } } } } } }")));
	}

	@Test // DATAMONGO-1843
	public void shouldRenderReduceWithInputAndInExpressionsCorrectly() {

		DBObject exprected = (DBObject) JSON.parse(
				"{ \"$project\" : { \"results\" : { \"$reduce\" : { \"input\" : { \"$slice\" : [\"$array\", 5] }, \"initialValue\" : \"\", \"in\" : { \"$concat\" : [\"$$value\", \"/\", \"$$this\"] } } } } }");

		Reduce reduceEntryPoint = Reduce.arrayOf(Slice.sliceArrayOf("array").itemCount(5)) //
				.withInitialValue("") //
				.reduce(Concat.valueOf("$$value").concat("/").concatValueOf("$$this"));

		Reduce arrayEntryPoint = ArrayOperators.arrayOf(Slice.sliceArrayOf("array").itemCount(5)) //
				.reduce(Concat.valueOf("$$value").concat("/").concatValueOf("$$this")) //
				.startingWith("");

		assertThat(project().and(reduceEntryPoint).as("results").toDBObject(Aggregation.DEFAULT_CONTEXT),
				Matchers.is(exprected));

		assertThat(project().and(arrayEntryPoint).as("results").toDBObject(Aggregation.DEFAULT_CONTEXT),
				Matchers.is(exprected));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderZipCorrectly() {

		AggregationExpression elemAt0 = ArrayOperators.arrayOf("matrix").elementAt(0);
		AggregationExpression elemAt1 = ArrayOperators.arrayOf("matrix").elementAt(1);
		AggregationExpression elemAt2 = ArrayOperators.arrayOf("matrix").elementAt(2);

		DBObject agg = project().and(
				ArrayOperators.arrayOf(elemAt0).zipWith(elemAt1, elemAt2).useLongestLength().defaultTo(new Object[] { 1, 2 }))
				.as("transposed").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse(
				"{ $project : {  transposed: { $zip: { inputs: [ { $arrayElemAt: [ \"$matrix\", 0 ] }, { $arrayElemAt: [ \"$matrix\", 1 ] }, { $arrayElemAt: [ \"$matrix\", 2 ] } ], useLongestLength : true, defaults: [1,2] } } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderInCorrectly() {

		DBObject agg = project().and(ArrayOperators.arrayOf("in_stock").containsValue("bananas")).as("has_bananas")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				Matchers.is(JSON.parse("{ $project : { has_bananas : { $in : [\"bananas\", \"$in_stock\" ] } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderIsoDayOfWeekCorrectly() {

		DBObject agg = project().and(DateOperators.dateOf("birthday").isoDayOfWeek()).as("dayOfWeek")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project : { dayOfWeek: { $isoDayOfWeek: \"$birthday\" } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderIsoWeekCorrectly() {

		DBObject agg = project().and(DateOperators.dateOf("date").isoWeek()).as("weekNumber")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project : { weekNumber: { $isoWeek: \"$date\" } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderIsoWeekYearCorrectly() {

		DBObject agg = project().and(DateOperators.dateOf("date").isoWeekYear()).as("yearNumber")
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project : { yearNumber: { $isoWeekYear: \"$date\" } } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderSwitchCorrectly() {

		String expected = "$switch:\n" + //
				"{\n" + //
				"     branches: [\n" + //
				"       {\n" + //
				"         case: { $gte : [ { $avg : \"$scores\" }, 90 ] },\n" + //
				"         then: \"Doing great!\"\n" + //
				"       },\n" + //
				"       {\n" + //
				"         case: { $and : [ { $gte : [ { $avg : \"$scores\" }, 80 ] },\n" + //
				"                          { $lt : [ { $avg : \"$scores\" }, 90 ] } ] },\n" + //
				"         then: \"Doing pretty well.\"\n" + //
				"       },\n" + //
				"       {\n" + //
				"         case: { $lt : [ { $avg : \"$scores\" }, 80 ] },\n" + //
				"         then: \"Needs improvement.\"\n" + //
				"       }\n" + //
				"     ],\n" + //
				"     default: \"No scores found.\"\n" + //
				"   }\n" + //
				"}";

		CaseOperator cond1 = CaseOperator
				.when(ComparisonOperators.Gte.valueOf(AccumulatorOperators.Avg.avgOf("scores")).greaterThanEqualToValue(90))
				.then("Doing great!");
		CaseOperator cond2 = CaseOperator
				.when(BooleanOperators.And.and(
						ComparisonOperators.Gte.valueOf(AccumulatorOperators.Avg.avgOf("scores")).greaterThanEqualToValue(80),
						ComparisonOperators.Lt.valueOf(AccumulatorOperators.Avg.avgOf("scores")).lessThanValue(90)))
				.then("Doing pretty well.");
		CaseOperator cond3 = CaseOperator
				.when(ComparisonOperators.Lt.valueOf(AccumulatorOperators.Avg.avgOf("scores")).lessThanValue(80))
				.then("Needs improvement.");

		DBObject agg = project().and(ConditionalOperators.switchCases(cond1, cond2, cond3).defaultTo("No scores found."))
				.as("summary").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project : { summary: {" + expected + "} } }")));
	}

	@Test // DATAMONGO-1548
	public void shouldTypeCorrectly() {

		DBObject agg = project().and(DataTypeOperators.Type.typeOf("a")).as("a").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(JSON.parse("{ $project : { a: { $type: \"$a\" } } }")));
	}

	private static DBObject exctractOperation(String field, DBObject fromProjectClause) {
		return (DBObject) fromProjectClause.get(field);
	}

}
