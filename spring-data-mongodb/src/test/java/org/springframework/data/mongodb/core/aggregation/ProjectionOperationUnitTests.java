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
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Let.ExpressionVariable.*;
import static org.springframework.data.mongodb.core.aggregation.AggregationFunctionExpressions.*;
import static org.springframework.data.mongodb.core.aggregation.Fields.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.And;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.ArithmeticOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.ArrayOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Avg;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.BooleanOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.ComparisonOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.ConditionalOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.DateOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Gte;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Let.ExpressionVariable;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.LiteralOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Lt;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.RangeOperator;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Reduce.PropertyExpression;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Reduce.Variable;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.SetOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.StringOperators;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Switch.CaseOperator;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Type;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.VariableOperators;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation.ProjectionOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.*;

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

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFields() {
		new ProjectionOperation(null);
	}

	@Test
	public void declaresBackReferenceCorrectly() {

		ProjectionOperation operation = new ProjectionOperation();
		operation = operation.and("prop").previousOperation();

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		assertThat(projectClause.get("prop"), is((Object) Fields.UNDERSCORE_ID_REF));
	}

	@Test
	public void alwaysUsesExplicitReference() {

		ProjectionOperation operation = new ProjectionOperation(Fields.fields("foo").and("bar", "foobar"));

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat(projectClause.get("foo"), is((Object) 1));
		assertThat(projectClause.get("bar"), is((Object) "$foobar"));
	}

	@Test
	public void aliasesSimpleFieldProjection() {

		ProjectionOperation operation = new ProjectionOperation();

		Document document = operation.and("foo").as("bar").toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat(projectClause.get("bar"), is((Object) "$foo"));
	}

	@Test
	public void aliasesArithmeticProjection() {

		ProjectionOperation operation = new ProjectionOperation();

		Document document = operation.and("foo").plus(41).as("bar").toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document barClause = DocumentTestUtils.getAsDocument(projectClause, "bar");
		List<Object> addClause = (List<Object>) barClause.get("$add");

		assertThat(addClause, hasSize(2));
		assertThat(addClause.get(0), is((Object) "$foo"));
		assertThat(addClause.get(1), is((Object) 41));
	}

	public void arithmenticProjectionOperationWithoutAlias() {

		String fieldName = "a";
		ProjectionOperationBuilder operation = new ProjectionOperation().and(fieldName).plus(1);
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document oper = exctractOperation(fieldName, projectClause);

		assertThat(oper.containsKey(ADD), is(true));
		assertThat(oper.get(ADD), is((Object) Arrays.<Object> asList("$a", 1)));
	}

	@Test
	public void arithmenticProjectionOperationPlus() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).plus(1).as(fieldAlias);
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		Document oper = exctractOperation(fieldAlias, projectClause);
		assertThat(oper.containsKey(ADD), is(true));
		assertThat(oper.get(ADD), is((Object) Arrays.<Object> asList("$a", 1)));
	}

	@Test
	public void arithmenticProjectionOperationMinus() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).minus(1).as(fieldAlias);
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document oper = exctractOperation(fieldAlias, projectClause);

		assertThat(oper.containsKey(SUBTRACT), is(true));
		assertThat(oper.get(SUBTRACT), is((Object) Arrays.<Object> asList("$a", 1)));
	}

	@Test
	public void arithmenticProjectionOperationMultiply() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).multiply(1).as(fieldAlias);
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document oper = exctractOperation(fieldAlias, projectClause);

		assertThat(oper.containsKey(MULTIPLY), is(true));
		assertThat(oper.get(MULTIPLY), is((Object) Arrays.<Object> asList("$a", 1)));
	}

	@Test
	public void arithmenticProjectionOperationDivide() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).divide(1).as(fieldAlias);
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document oper = exctractOperation(fieldAlias, projectClause);

		assertThat(oper.containsKey(DIVIDE), is(true));
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
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document oper = exctractOperation(fieldAlias, projectClause);

		assertThat(oper.containsKey(MOD), is(true));
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
		Document document = projectionOp.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		assertThat((Integer) projectClause.get(Fields.UNDERSCORE_ID), is(0));
	}

	/**
	 * @see DATAMONGO-757
	 */
	@Test
	public void usesImplictAndExplicitFieldAliasAndIncludeExclude() {

		ProjectionOperation operation = Aggregation.project("foo").and("foobar").as("bar").andInclude("inc1", "inc2")
				.andExclude("_id");

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

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

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat((Document) projectClause.get("fooPlusBar"), //
				is(new Document("$add", Arrays.asList("$foo", "$bar"))));
		assertThat((Document) projectClause.get("fooMinusBar"), //
				is(new Document("$subtract", Arrays.asList("$foo", "$bar"))));
		assertThat((Document) projectClause.get("fooMultiplyBar"), //
				is(new Document("$multiply", Arrays.asList("$foo", "$bar"))));
		assertThat((Document) projectClause.get("fooDivideBar"), //
				is(new Document("$divide", Arrays.asList("$foo", "$bar"))));
		assertThat((Document) projectClause.get("fooModBar"), //
				is(new Document("$mod", Arrays.asList("$foo", "$bar"))));
	}

	/**
	 * @see DATAMONGO-774
	 */
	@Test
	public void projectionExpressions() {

		ProjectionOperation operation = Aggregation.project() //
				.andExpression("(netPrice + surCharge) * taxrate * [0]", 2).as("grossSalesPrice") //
				.and("foo").as("bar"); //

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document, is(Document.parse(
				"{ \"$project\" : { \"grossSalesPrice\" : { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\" , 2]} , \"bar\" : \"$foo\"}}")));
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

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document, is(notNullValue()));

		Document projected = exctractOperation("$project", document);

		assertThat(projected.get("hour"), is((Object) new Document("$hour", Arrays.asList("$date"))));
		assertThat(projected.get("min"), is((Object) new Document("$minute", Arrays.asList("$date"))));
		assertThat(projected.get("second"), is((Object) new Document("$second", Arrays.asList("$date"))));
		assertThat(projected.get("millis"), is((Object) new Document("$millisecond", Arrays.asList("$date"))));
		assertThat(projected.get("year"), is((Object) new Document("$year", Arrays.asList("$date"))));
		assertThat(projected.get("month"), is((Object) new Document("$month", Arrays.asList("$date"))));
		assertThat(projected.get("week"), is((Object) new Document("$week", Arrays.asList("$date"))));
		assertThat(projected.get("dayOfYear"), is((Object) new Document("$dayOfYear", Arrays.asList("$date"))));
		assertThat(projected.get("dayOfMonth"), is((Object) new Document("$dayOfMonth", Arrays.asList("$date"))));
		assertThat(projected.get("dayOfWeek"), is((Object) new Document("$dayOfWeek", Arrays.asList("$date"))));
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

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document, is(notNullValue()));

		Document projected = exctractOperation("$project", document);
		assertThat(projected.get("dayOfYearPlus1Day"), is((Object) new Document("$dayOfYear",
				Arrays.asList(new Document("$add", Arrays.<Object> asList("$date", 86400000))))));
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

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		Document projected = exctractOperation("$project", document);
		assertThat(projected.get("tags_count"), is((Object) new Document("$size", Arrays.asList("$tags"))));
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

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		Document projected = exctractOperation("$project", document);
		assertThat(projected.get("tags_count"), is((Object) new Document("$size", Arrays.asList("$tags"))));
	}

	/**
	 * @see DATAMONGO-1457
	 */
	@Test
	public void shouldRenderSliceCorrectly() throws Exception {

		ProjectionOperation operation = Aggregation.project().and("field").slice(10).as("renamed");

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projected = exctractOperation("$project", document);

		assertThat(projected.get("renamed"), is((Object) new Document("$slice", Arrays.<Object> asList("$field", 10))));
	}

	/**
	 * @see DATAMONGO-1457
	 */
	@Test
	public void shouldRenderSliceWithPositionCorrectly() throws Exception {

		ProjectionOperation operation = Aggregation.project().and("field").slice(10, 5).as("renamed");

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projected = exctractOperation("$project", document);

		assertThat(projected.get("renamed"), is((Object) new Document("$slice", Arrays.<Object> asList("$field", 5, 10))));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderCmpCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").cmp(10).as("cmp10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.cmp10.$cmp.[0]", "$field").containing("$project.cmp10.$cmp.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderEqCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").eq(10).as("eq10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.eq10.$eq.[0]", "$field").containing("$project.eq10.$eq.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderGtCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").gt(10).as("gt10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.gt10.$gt.[0]", "$field").containing("$project.gt10.$gt.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderGteCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").gte(10).as("gte10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.gte10.$gte.[0]", "$field").containing("$project.gte10.$gte.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderLtCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").lt(10).as("lt10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.lt10.$lt.[0]", "$field").containing("$project.lt10.$lt.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderLteCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").lte(10).as("lte10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.lte10.$lte.[0]", "$field").containing("$project.lte10.$lte.[1]", 10));
	}

	/**
	 * @see DATAMONGO-784
	 */
	@Test
	public void shouldRenderNeCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").ne(10).as("ne10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT),
				isBsonObject().containing("$project.ne10.$ne.[0]", "$field").containing("$project.ne10.$ne.[1]", 10));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetEquals() {

		Document agg = project("A", "B").and("A").equalsArrays("B").as("sameElements")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { A: 1, B: 1, sameElements: { $setEquals: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetEqualsAggregationExpresssion() {

		Document agg = project("A", "B").and(SetOperators.arrayAsSet("A").isEqualTo("B")).as("sameElements")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { A: 1, B: 1, sameElements: { $setEquals: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetIntersection() {

		Document agg = project("A", "B").and("A").intersectsArrays("B").as("commonToBoth")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { A: 1, B: 1, commonToBoth: { $setIntersection: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetIntersectionAggregationExpresssion() {

		Document agg = project("A", "B").and(SetOperators.arrayAsSet("A").intersects("B")).as("commonToBoth")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { A: 1, B: 1, commonToBoth: { $setIntersection: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetUnion() {

		Document agg = project("A", "B").and("A").unionArrays("B").as("allValues").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { A: 1, B: 1, allValues: { $setUnion: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetUnionAggregationExpresssion() {

		Document agg = project("A", "B").and(SetOperators.arrayAsSet("A").union("B")).as("allValues")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { A: 1, B: 1, allValues: { $setUnion: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetDifference() {

		Document agg = project("A", "B").and("B").differenceToArray("A").as("inBOnly")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { A: 1, B: 1, inBOnly: { $setDifference: [ \"$B\", \"$A\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetDifferenceAggregationExpresssion() {

		Document agg = project("A", "B").and(SetOperators.arrayAsSet("B").differenceTo("A")).as("inBOnly")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { A: 1, B: 1, inBOnly: { $setDifference: [ \"$B\", \"$A\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetIsSubset() {

		Document agg = project("A", "B").and("A").subsetOfArray("B").as("aIsSubsetOfB")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { A: 1, B: 1, aIsSubsetOfB: { $setIsSubset: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSetIsSubsetAggregationExpresssion() {

		Document agg = project("A", "B").and(SetOperators.arrayAsSet("A").isSubsetOf("B")).as("aIsSubsetOfB")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { A: 1, B: 1, aIsSubsetOfB: { $setIsSubset: [ \"$A\", \"$B\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAnyElementTrue() {

		Document agg = project("responses").and("responses").anyElementInArrayTrue().as("isAnyTrue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { responses: 1, isAnyTrue: { $anyElementTrue: [ \"$responses\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAnyElementTrueAggregationExpresssion() {

		Document agg = project("responses").and(SetOperators.arrayAsSet("responses").anyElementTrue()).as("isAnyTrue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { responses: 1, isAnyTrue: { $anyElementTrue: [ \"$responses\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAllElementsTrue() {

		Document agg = project("responses").and("responses").allElementsInArrayTrue().as("isAllTrue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { responses: 1, isAllTrue: { $allElementsTrue: [ \"$responses\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAllElementsTrueAggregationExpresssion() {

		Document agg = project("responses").and(SetOperators.arrayAsSet("responses").allElementsTrue()).as("isAllTrue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { responses: 1, isAllTrue: { $allElementsTrue: [ \"$responses\" ] }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAbs() {

		Document agg = project().and("anyNumber").absoluteValue().as("absoluteValue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { absoluteValue : { $abs:  \"$anyNumber\" }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAbsAggregationExpresssion() {

		Document agg = project()
				.and(
						ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).abs())
				.as("delta").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { delta: { $abs: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAddAggregationExpresssion() {

		Document agg = project().and(ArithmeticOperators.valueOf("price").add("fee")).as("total")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse(" { $project: { total: { $add: [ \"$price\", \"$fee\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderCeil() {

		Document agg = project().and("anyNumber").ceil().as("ceilValue").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { ceilValue : { $ceil:  \"$anyNumber\" }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderCeilAggregationExpresssion() {

		Document agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).ceil())
				.as("delta").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { delta: { $ceil: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDivide() {

		Document agg = project().and("value")
				.divide(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document
				.parse("{ $project: { result: { $divide: [ \"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDivideAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf("anyNumber")
						.divideBy(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document
				.parse("{ $project: { result: { $divide: [ \"$anyNumber\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderExp() {

		Document agg = project().and("value").exp().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $exp: \"$value\" } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderExpAggregationExpresssion() {

		Document agg = project()
				.and(
						ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).exp())
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $exp: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderFloor() {

		Document agg = project().and("value").floor().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $floor: \"$value\" } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderFloorAggregationExpresssion() {

		Document agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).floor())
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $floor: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLn() {

		Document agg = project().and("value").ln().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $ln: \"$value\"} }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLnAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).ln())
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $ln: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLog() {

		Document agg = project().and("value").log(2).as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $log: [ \"$value\", 2] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLogAggregationExpresssion() {

		Document agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).log(2))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { result: { $log: [ { $subtract: [ \"$start\", \"$end\" ] }, 2] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLog10() {

		Document agg = project().and("value").log10().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $log10: \"$value\" } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLog10AggregationExpresssion() {

		Document agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).log10())
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $log10: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMod() {

		Document agg = project().and("value").mod(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end")))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { result: { $mod: [\"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderModAggregationExpresssion() {

		Document agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).mod(2))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { result: { $mod: [{ $subtract: [ \"$start\", \"$end\" ] }, 2] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMultiply() {

		Document agg = project().and("value")
				.multiply(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document
				.parse("{ $project: { result: { $multiply: [\"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMultiplyAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end")))
						.multiplyBy(2).multiplyBy("refToAnotherNumber"))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse(
				"{ $project: { result: { $multiply: [{ $subtract: [ \"$start\", \"$end\" ] }, 2, \"$refToAnotherNumber\"] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderPow() {

		Document agg = project().and("value").pow(2).as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $pow: [\"$value\", 2] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderPowAggregationExpresssion() {

		Document agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).pow(2))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { result: { $pow: [{ $subtract: [ \"$start\", \"$end\" ] }, 2] } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSqrt() {

		Document agg = project().and("value").sqrt().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $sqrt: \"$value\" } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSqrtAggregationExpresssion() {

		Document agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).sqrt())
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $sqrt: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSubtract() {

		Document agg = project().and("numericField").minus(AggregationFunctionExpressions.SIZE.of(field("someArray")))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(
				Document.parse("{ $project: { result: { $subtract: [ \"$numericField\", { $size : [\"$someArray\"]}] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSubtractAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf("numericField")
						.subtract(AggregationFunctionExpressions.SIZE.of(field("someArray"))))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(
				Document.parse("{ $project: { result: { $subtract: [ \"$numericField\", { $size : [\"$someArray\"]}] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderTrunc() {

		Document agg = project().and("value").trunc().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result : { $trunc: \"$value\" }}}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderTruncAggregationExpresssion() {

		Document agg = project().and(
				ArithmeticOperators.valueOf(AggregationFunctionExpressions.SUBTRACT.of(field("start"), field("end"))).trunc())
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $trunc: { $subtract: [ \"$start\", \"$end\" ] } } }}")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderConcat() {

		Document agg = project().and("item").concat(" - ", field("description")).as("itemDescription")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { itemDescription: { $concat: [ \"$item\", \" - \", \"$description\" ] } } }")));

	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderConcatAggregationExpression() {

		Document agg = project().and(StringOperators.valueOf("item").concat(" - ").concatValueOf("description"))
				.as("itemDescription").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { itemDescription: { $concat: [ \"$item\", \" - \", \"$description\" ] } } }")));

	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSubstr() {

		Document agg = project().and("quarter").substring(0, 2).as("yearSubstring").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { yearSubstring: { $substr: [ \"$quarter\", 0, 2 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSubstrAggregationExpression() {

		Document agg = project().and(StringOperators.valueOf("quarter").substring(0, 2)).as("yearSubstring")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { yearSubstring: { $substr: [ \"$quarter\", 0, 2 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderToLower() {

		Document agg = project().and("item").toLower().as("item").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { item: { $toLower: \"$item\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderToLowerAggregationExpression() {

		Document agg = project().and(StringOperators.valueOf("item").toLower()).as("item")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { item: { $toLower: \"$item\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderToUpper() {

		Document agg = project().and("item").toUpper().as("item").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { item: { $toUpper: \"$item\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderToUpperAggregationExpression() {

		Document agg = project().and(StringOperators.valueOf("item").toUpper()).as("item")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { item: { $toUpper: \"$item\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderStrCaseCmp() {

		Document agg = project().and("quarter").strCaseCmp("13q4").as("comparisonResult")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { comparisonResult: { $strcasecmp: [ \"$quarter\", \"13q4\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderStrCaseCmpAggregationExpression() {

		Document agg = project().and(StringOperators.valueOf("quarter").strCaseCmp("13q4")).as("comparisonResult")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { comparisonResult: { $strcasecmp: [ \"$quarter\", \"13q4\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderArrayElementAt() {

		Document agg = project().and("favorites").arrayElementAt(0).as("first").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { first: { $arrayElemAt: [ \"$favorites\", 0 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderArrayElementAtAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("favorites").elementAt(0)).as("first")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { first: { $arrayElemAt: [ \"$favorites\", 0 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderConcatArrays() {

		Document agg = project().and("instock").concatArrays("ordered").as("items").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { items: { $concatArrays: [ \"$instock\", \"$ordered\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderConcatArraysAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("instock").concat("ordered")).as("items")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { items: { $concatArrays: [ \"$instock\", \"$ordered\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderIsArray() {

		Document agg = project().and("instock").isArray().as("isAnArray").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { isAnArray: { $isArray: \"$instock\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderIsArrayAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("instock").isArray()).as("isAnArray")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { isAnArray: { $isArray: \"$instock\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSizeAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("instock").length()).as("arraySize")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { arraySize: { $size: \"$instock\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSliceAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("favorites").slice().itemCount(3)).as("threeFavorites")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { threeFavorites: { $slice: [ \"$favorites\", 3 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSliceWithPositionAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("favorites").slice().offset(2).itemCount(3))
				.as("threeFavorites").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { threeFavorites: { $slice: [ \"$favorites\", 2, 3 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLiteral() {

		Document agg = project().and("$1").asLiteral().as("literalOnly").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { literalOnly: { $literal:  \"$1\"} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLiteralAggregationExpression() {

		Document agg = project().and(LiteralOperators.valueOf("$1").asLiteral()).as("literalOnly")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { literalOnly: { $literal:  \"$1\"} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDayOfYearAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").dayOfYear()).as("dayOfYear")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { dayOfYear: { $dayOfYear: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDayOfMonthAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").dayOfMonth()).as("day")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { day: { $dayOfMonth: \"$date\" }} }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDayOfWeekAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").dayOfWeek()).as("dayOfWeek")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { dayOfWeek: { $dayOfWeek: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderYearAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").year()).as("year")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { year: { $year: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMonthAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").month()).as("month")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { month: { $month: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderWeekAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").week()).as("week")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { week: { $week: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderHourAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").hour()).as("hour")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { hour: { $hour: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMinuteAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").minute()).as("minute")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { minute: { $minute: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSecondAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").second()).as("second")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { second: { $second: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMillisecondAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").millisecond()).as("msec")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { msec: { $millisecond: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDateToString() {

		Document agg = project().and("date").dateAsFormattedString("%H:%M:%S:%L").as("time")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { time: { $dateToString: { format: \"%H:%M:%S:%L\", date: \"$date\" } } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderDateToStringAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").toString("%H:%M:%S:%L")).as("time")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { time: { $dateToString: { format: \"%H:%M:%S:%L\", date: \"$date\" } } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSumAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("quizzes").sum()).as("quizTotal")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { quizTotal: { $sum: \"$quizzes\"} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderSumWithMultipleArgsAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("final").sum().and("midterm")).as("examTotal")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: {  examTotal: { $sum: [ \"$final\", \"$midterm\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAvgAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("quizzes").avg()).as("quizAvg")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { quizAvg: { $avg: \"$quizzes\"} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderAvgWithMultipleArgsAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("final").avg().and("midterm")).as("examAvg")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: {  examAvg: { $avg: [ \"$final\", \"$midterm\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMaxAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("quizzes").max()).as("quizMax")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { quizMax: { $max: \"$quizzes\"} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMaxWithMultipleArgsAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("final").max().and("midterm")).as("examMax")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: {  examMax: { $max: [ \"$final\", \"$midterm\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMinAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("quizzes").min()).as("quizMin")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { quizMin: { $min: \"$quizzes\"} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderMinWithMultipleArgsAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("final").min().and("midterm")).as("examMin")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: {  examMin: { $min: [ \"$final\", \"$midterm\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderStdDevPopAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("scores").stdDevPop()).as("stdDev")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { stdDev: { $stdDevPop: \"$scores\"} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderStdDevSampAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("scores").stdDevSamp()).as("stdDev")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { stdDev: { $stdDevSamp: \"$scores\"} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderCmpAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").compareToValue(250)).as("cmp250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { cmp250: { $cmp: [\"$qty\", 250]} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderEqAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").equalToValue(250)).as("eq250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { eq250: { $eq: [\"$qty\", 250]} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderGtAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").greaterThanValue(250)).as("gt250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { gt250: { $gt: [\"$qty\", 250]} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderGteAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").greaterThanEqualToValue(250)).as("gte250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { gte250: { $gte: [\"$qty\", 250]} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLtAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").lessThanValue(250)).as("lt250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { lt250: { $lt: [\"$qty\", 250]} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLteAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").lessThanEqualToValue(250)).as("lte250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { lte250: { $lte: [\"$qty\", 250]} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderNeAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").notEqualToValue(250)).as("ne250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { ne250: { $ne: [\"$qty\", 250]} } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLogicAndAggregationExpression() {

		Document agg = project()
				.and(BooleanOperators.valueOf(ComparisonOperators.valueOf("qty").greaterThanValue(100))
						.and(ComparisonOperators.valueOf("qty").lessThanValue(250)))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document
				.parse("{ $project: { result: { $and: [ { $gt: [ \"$qty\", 100 ] }, { $lt: [ \"$qty\", 250 ] } ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderLogicOrAggregationExpression() {

		Document agg = project()
				.and(BooleanOperators.valueOf(ComparisonOperators.valueOf("qty").greaterThanValue(250))
						.or(ComparisonOperators.valueOf("qty").lessThanValue(200)))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document
				.parse("{ $project: { result: { $or: [ { $gt: [ \"$qty\", 250 ] }, { $lt: [ \"$qty\", 200 ] } ] } } }")));
	}

	/**
	 * @see DATAMONGO-1536
	 */
	@Test
	public void shouldRenderNotAggregationExpression() {

		Document agg = project().and(BooleanOperators.not(ComparisonOperators.valueOf("qty").greaterThanValue(250)))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $not: [ { $gt: [ \"$qty\", 250 ] } ] } } }")));
	}

	/**
	 * @see DATAMONGO-1540
	 */
	@Test
	public void shouldRenderMapAggregationExpression() {

		Document agg = Aggregation.project()
				.and(VariableOperators.mapItemsOf("quizzes").as("grade")
						.andApply(AggregationFunctionExpressions.ADD.of(field("grade"), 2)))
				.as("adjustedGrades").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse(
				"{ $project:{ adjustedGrades:{ $map: { input: \"$quizzes\", as: \"grade\",in: { $add: [ \"$$grade\", 2 ] }}}}}")));
	}

	/**
	 * @see DATAMONGO-1540
	 */
	@Test
	public void shouldRenderMapAggregationExpressionOnExpression() {

		Document agg = Aggregation.project()
				.and(VariableOperators.mapItemsOf(AggregationFunctionExpressions.SIZE.of("foo")).as("grade")
						.andApply(AggregationFunctionExpressions.ADD.of(field("grade"), 2)))
				.as("adjustedGrades").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse(
				"{ $project:{ adjustedGrades:{ $map: { input: { $size : [\"foo\"]}, as: \"grade\",in: { $add: [ \"$$grade\", 2 ] }}}}}")));
	}

	/**
	 * @see DATAMONGO-861, DATAMONGO-1542
	 */
	@Test
	public void shouldRenderIfNullConditionAggregationExpression() {

		Document agg = project().and(
				ConditionalOperators.ifNull(ArrayOperators.arrayOf("array").elementAt(1)).then("a more sophisticated value"))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse(
				"{ $project: { result: { $ifNull: [ { $arrayElemAt: [\"$array\", 1] }, \"a more sophisticated value\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1542
	 */
	@Test
	public void shouldRenderIfNullValueAggregationExpression() {

		Document agg = project()
				.and(ConditionalOperators.ifNull("field").then(ArrayOperators.arrayOf("array").elementAt(1))).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project: { result: { $ifNull: [ \"$field\", { $arrayElemAt: [\"$array\", 1] } ] } } }")));
	}

	/**
	 * @see DATAMONGO-861, DATAMONGO-1542
	 */
	@Test
	public void fieldReplacementIfNullShouldRenderCorrectly() {

		Document agg = project().and(ConditionalOperators.ifNull("optional").thenValueOf("$never-null")).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse("{ $project: { result: { $ifNull: [ \"$optional\", \"$never-null\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1538
	 */
	@Test
	public void shouldRenderLetExpressionCorrectly() {

		Document agg = Aggregation.project()
				.and(VariableOperators
						.define(
								newVariable("total")
										.forExpression(AggregationFunctionExpressions.ADD.of(Fields.field("price"), Fields.field("tax"))),
								newVariable("discounted").forExpression(Cond.when("applyDiscount").then(0.9D).otherwise(1.0D)))
						.andApply(AggregationFunctionExpressions.MULTIPLY.of(Fields.field("total"), Fields.field("discounted")))) //
				.as("finalTotal").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project:{  \"finalTotal\" : { \"$let\": {" + //
						"\"vars\": {" + //
						"\"total\": { \"$add\": [ \"$price\", \"$tax\" ] }," + //
						"\"discounted\": { \"$cond\": { \"if\": \"$applyDiscount\", \"then\": 0.9, \"else\": 1.0 } }" + //
						"}," + //
						"\"in\": { \"$multiply\": [ \"$$total\", \"$$discounted\" ] }" + //
						"}}}}")));
	}

	/**
	 * @see DATAMONGO-1538
	 */
	@Test
	public void shouldRenderLetExpressionCorrectlyWhenUsingLetOnProjectionBuilder() {

		ExpressionVariable var1 = newVariable("total")
				.forExpression(AggregationFunctionExpressions.ADD.of(Fields.field("price"), Fields.field("tax")));

		ExpressionVariable var2 = newVariable("discounted")
				.forExpression(Cond.when("applyDiscount").then(0.9D).otherwise(1.0D));

		Document agg = Aggregation.project().and("foo")
				.let(Arrays.asList(var1, var2),
						AggregationFunctionExpressions.MULTIPLY.of(Fields.field("total"), Fields.field("discounted")))
				.as("finalTotal").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $project:{ \"finalTotal\" : { \"$let\": {" + //
						"\"vars\": {" + //
						"\"total\": { \"$add\": [ \"$price\", \"$tax\" ] }," + //
						"\"discounted\": { \"$cond\": { \"if\": \"$applyDiscount\", \"then\": 0.9, \"else\": 1.0 } }" + //
						"}," + //
						"\"in\": { \"$multiply\": [ \"$$total\", \"$$discounted\" ] }" + //
						"}}}}")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderIndexOfBytesCorrectly() {

		Document agg = project().and(StringOperators.valueOf("item").indexOf("foo")).as("byteLocation")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project: { byteLocation: { $indexOfBytes: [ \"$item\", \"foo\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderIndexOfBytesWithRangeCorrectly() {

		Document agg = project().and(StringOperators.valueOf("item").indexOf("foo").within(new Range<Long>(5L, 9L)))
				.as("byteLocation").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, isBsonObject().containing("$project.byteLocation.$indexOfBytes.[2]", 5L)
				.containing("$project.byteLocation.$indexOfBytes.[3]", 9L));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderIndexOfCPCorrectly() {

		Document agg = project().and(StringOperators.valueOf("item").indexOfCP("foo")).as("cpLocation")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project: { cpLocation: { $indexOfCP: [ \"$item\", \"foo\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderIndexOfCPWithRangeCorrectly() {

		Document agg = project().and(StringOperators.valueOf("item").indexOfCP("foo").within(new Range<Long>(5L, 9L)))
				.as("cpLocation").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, isBsonObject().containing("$project.cpLocation.$indexOfCP.[2]", 5L)
				.containing("$project.cpLocation.$indexOfCP.[3]", 9L));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderSplitCorrectly() {

		Document agg = project().and(StringOperators.valueOf("city").split(", ")).as("city_state")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { city_state : { $split: [\"$city\", \", \"] }} }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderStrLenBytesCorrectly() {

		Document agg = project().and(StringOperators.valueOf("name").length()).as("length")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { \"length\": { $strLenBytes: \"$name\" } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderStrLenCPCorrectly() {

		Document agg = project().and(StringOperators.valueOf("name").lengthCP()).as("length")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { \"length\": { $strLenCP: \"$name\" } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderSubstrCPCorrectly() {

		Document agg = project().and(StringOperators.valueOf("quarter").substringCP(0, 2)).as("yearSubstring")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { yearSubstring: { $substrCP: [ \"$quarter\", 0, 2 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderIndexOfArrayCorrectly() {

		Document agg = project().and(ArrayOperators.arrayOf("items").indexOf(2)).as("index")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { index: { $indexOfArray: [ \"$items\", 2 ] } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderRangeCorrectly() {

		Document agg = project().and(RangeOperator.rangeStartingAt(0L).to("distance").withStepSize(25L)).as("rest_stops")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, isBsonObject().containing("$project.rest_stops.$range.[0]", 0L)
				.containing("$project.rest_stops.$range.[1]", "$distance").containing("$project.rest_stops.$range.[2]", 25L));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderReverseArrayCorrectly() {

		Document agg = project().and(ArrayOperators.arrayOf("favorites").reverse()).as("reverseFavorites")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { reverseFavorites: { $reverseArray: \"$favorites\" } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderReduceWithSimpleObjectCorrectly() {

		Document agg = project()
				.and(ArrayOperators.arrayOf("probabilityArr")
						.reduce(ArithmeticOperators.valueOf("$$value").multiplyBy("$$this")).startingWith(1))
				.as("results").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse(
				"{ $project : { \"results\": { $reduce: { input: \"$probabilityArr\", initialValue: 1, in: { $multiply: [ \"$$value\", \"$$this\" ] } } } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderReduceWithComplexObjectCorrectly() {

		PropertyExpression sum = PropertyExpression.property("sum").definedAs(
				ArithmeticOperators.valueOf(Variable.VALUE.referingTo("sum").getName()).add(Variable.THIS.getName()));
		PropertyExpression product = PropertyExpression.property("product").definedAs(ArithmeticOperators
				.valueOf(Variable.VALUE.referingTo("product").getName()).multiplyBy(Variable.THIS.getName()));

		Document agg = project()
				.and(ArrayOperators.arrayOf("probabilityArr").reduce(sum, product)
						.startingWith(new Document().append("sum", 5).append("product", 2)))
				.as("results").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse(
				"{ $project : { \"results\": { $reduce: { input: \"$probabilityArr\", initialValue:  { \"sum\" : 5 , \"product\" : 2} , in: { \"sum\": { $add : [\"$$value.sum\", \"$$this\"] }, \"product\": { $multiply: [ \"$$value.product\", \"$$this\" ] } } } } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderZipCorrectly() {

		AggregationExpression elemAt0 = ArrayOperators.arrayOf("matrix").elementAt(0);
		AggregationExpression elemAt1 = ArrayOperators.arrayOf("matrix").elementAt(1);
		AggregationExpression elemAt2 = ArrayOperators.arrayOf("matrix").elementAt(2);

		Document agg = project().and(
				ArrayOperators.arrayOf(elemAt0).zipWith(elemAt1, elemAt2).useLongestLength().defaultTo(new Object[] { 1, 2 }))
				.as("transposed").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse(
				"{ $project : {  transposed: { $zip: { inputs: [ { $arrayElemAt: [ \"$matrix\", 0 ] }, { $arrayElemAt: [ \"$matrix\", 1 ] }, { $arrayElemAt: [ \"$matrix\", 2 ] } ], useLongestLength : true, defaults: [1,2] } } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderInCorrectly() {

		Document agg = project().and(ArrayOperators.arrayOf("in_stock").containsValue("bananas")).as("has_bananas")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { has_bananas : { $in : [\"bananas\", \"$in_stock\" ] } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderIsoDayOfWeekCorrectly() {

		Document agg = project().and(DateOperators.dateOf("birthday").isoDayOfWeek()).as("dayOfWeek")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { dayOfWeek: { $isoDayOfWeek: \"$birthday\" } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderIsoWeekCorrectly() {

		Document agg = project().and(DateOperators.dateOf("date").isoWeek()).as("weekNumber")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { weekNumber: { $isoWeek: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldRenderIsoWeekYearCorrectly() {

		Document agg = project().and(DateOperators.dateOf("date").isoWeekYear()).as("yearNumber")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { yearNumber: { $isoWeekYear: \"$date\" } } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
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

		CaseOperator cond1 = CaseOperator.when(Gte.valueOf(Avg.avgOf("scores")).greaterThanEqualToValue(90))
				.then("Doing great!");
		CaseOperator cond2 = CaseOperator.when(And.and(Gte.valueOf(Avg.avgOf("scores")).greaterThanEqualToValue(80),
				Lt.valueOf(Avg.avgOf("scores")).lessThanValue(90))).then("Doing pretty well.");
		CaseOperator cond3 = CaseOperator.when(Lt.valueOf(Avg.avgOf("scores")).lessThanValue(80))
				.then("Needs improvement.");

		Document agg = project().and(ConditionalOperators.switchCases(cond1, cond2, cond3).defaultTo("No scores found."))
				.as("summary").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { summary: {" + expected + "} } }")));
	}

	/**
	 * @see DATAMONGO-1548
	 */
	@Test
	public void shouldTypeCorrectly() {

		Document agg = project().and(Type.typeOf("a")).as("a")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, Matchers.is(Document.parse("{ $project : { a: { $type: \"$a\" } } }")));
	}

	private static Document exctractOperation(String field, Document fromProjectClause) {
		return (Document) fromProjectClause.get(field);
	}

}
