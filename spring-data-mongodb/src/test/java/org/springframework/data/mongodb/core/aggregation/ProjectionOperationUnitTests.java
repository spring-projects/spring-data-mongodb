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
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.aggregation.Fields.*;
import static org.springframework.data.mongodb.core.aggregation.VariableOperators.Let.ExpressionVariable.*;
import static org.springframework.data.mongodb.test.util.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Reduce;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Reduce.PropertyExpression;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Reduce.Variable;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Slice;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Switch.CaseOperator;
import org.springframework.data.mongodb.core.aggregation.DateOperators.Timezone;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation.ProjectionOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.StringOperators.Concat;
import org.springframework.data.mongodb.core.aggregation.VariableOperators.Let.ExpressionVariable;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Unit tests for {@link ProjectionOperation}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Divya Srivastava
 * @author Mark Paluch
 */
public class ProjectionOperationUnitTests {

	private static final String MOD = "$mod";
	private static final String ADD = "$add";
	private static final String SUBTRACT = "$subtract";
	private static final String MULTIPLY = "$multiply";
	private static final String DIVIDE = "$divide";
	private static final String PROJECT = "$project";

	@Test // DATAMONGO-586
	void rejectsNullFields() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ProjectionOperation((Fields) null));
	}

	@Test // DATAMONGO-586
	void declaresBackReferenceCorrectly() {

		ProjectionOperation operation = new ProjectionOperation();
		operation = operation.and("prop").previousOperation();

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		assertThat(projectClause.get("prop")).isEqualTo(Fields.UNDERSCORE_ID_REF);
	}

	@Test // DATAMONGO-586
	void alwaysUsesExplicitReference() {

		ProjectionOperation operation = new ProjectionOperation(Fields.fields("foo").and("bar", "foobar"));

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat(projectClause.get("foo")).isEqualTo(1);
		assertThat(projectClause.get("bar")).isEqualTo("$foobar");
	}

	@Test // DATAMONGO-586
	void aliasesSimpleFieldProjection() {

		ProjectionOperation operation = new ProjectionOperation();

		Document document = operation.and("foo").as("bar").toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat(projectClause.get("bar")).isEqualTo("$foo");
	}

	@Test // DATAMONGO-586
	void aliasesArithmeticProjection() {

		ProjectionOperation operation = new ProjectionOperation();

		Document document = operation.and("foo").plus(41).as("bar").toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document barClause = DocumentTestUtils.getAsDocument(projectClause, "bar");
		List<Object> addClause = (List<Object>) barClause.get("$add");

		assertThat(addClause).hasSize(2);
		assertThat(addClause.get(0)).isEqualTo("$foo");
		assertThat(addClause.get(1)).isEqualTo(41);
	}

	@Test // DATAMONGO-586
	void arithmeticProjectionOperationWithoutAlias() {

		String fieldName = "a";
		ProjectionOperationBuilder operation = new ProjectionOperation().and(fieldName).plus(1);
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document oper = extractOperation(fieldName, projectClause);

		assertThat(oper.containsKey(ADD)).isTrue();
		assertThat(oper.get(ADD)).isEqualTo(Arrays.<Object> asList("$a", 1));
	}

	@Test // DATAMONGO-586
	void arithmeticProjectionOperationPlus() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).plus(1).as(fieldAlias);
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		Document oper = extractOperation(fieldAlias, projectClause);
		assertThat(oper.containsKey(ADD)).isTrue();
		assertThat(oper.get(ADD)).isEqualTo(Arrays.<Object> asList("$a", 1));
	}

	@Test // DATAMONGO-586
	void arithmeticProjectionOperationMinus() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).minus(1).as(fieldAlias);
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document oper = extractOperation(fieldAlias, projectClause);

		assertThat(oper.containsKey(SUBTRACT)).isTrue();
		assertThat(oper.get(SUBTRACT)).isEqualTo(Arrays.<Object> asList("$a", 1));
	}

	@Test // DATAMONGO-586
	void arithmeticProjectionOperationMultiply() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).multiply(1).as(fieldAlias);
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document oper = extractOperation(fieldAlias, projectClause);

		assertThat(oper.containsKey(MULTIPLY)).isTrue();
		assertThat(oper.get(MULTIPLY)).isEqualTo(Arrays.<Object> asList("$a", 1));
	}

	@Test // DATAMONGO-586
	void arithmeticProjectionOperationDivide() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).divide(1).as(fieldAlias);
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document oper = extractOperation(fieldAlias, projectClause);

		assertThat(oper.containsKey(DIVIDE)).isTrue();
		assertThat(oper.get(DIVIDE)).isEqualTo(Arrays.<Object> asList("$a", 1));
	}

	@Test // DATAMONGO-586
	void arithmeticProjectionOperationDivideByZeroException() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ProjectionOperation().and("a").divide(0));
	}

	@Test // DATAMONGO-586
	void arithmeticProjectionOperationMod() {

		String fieldName = "a";
		String fieldAlias = "b";
		ProjectionOperation operation = new ProjectionOperation().and(fieldName).mod(3).as(fieldAlias);
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		Document oper = extractOperation(fieldAlias, projectClause);

		assertThat(oper.containsKey(MOD)).isTrue();
		assertThat(oper.get(MOD)).isEqualTo(Arrays.<Object> asList("$a", 3));
	}

	@Test // DATAMONGO-758, DATAMONGO-1893
	void excludeShouldAllowExclusionOfFieldsOtherThanUnderscoreId/* since MongoDB 3.4 */() {

		ProjectionOperation projectionOp = new ProjectionOperation().andExclude("foo");
		Document document = projectionOp.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat(projectionOp.inheritsFields()).isTrue();
		assertThat((Integer) projectClause.get("foo")).isEqualTo(0);
	}

	@Test // DATAMONGO-1893
	void includeShouldNotInheritFields() {

		ProjectionOperation projectionOp = new ProjectionOperation().andInclude("foo");

		assertThat(projectionOp.inheritsFields()).isFalse();
	}

	@Test // DATAMONGO-758
	void excludeShouldAllowExclusionOfUnderscoreId() {

		ProjectionOperation projectionOp = new ProjectionOperation().andExclude(Fields.UNDERSCORE_ID);
		Document document = projectionOp.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);
		assertThat((Integer) projectClause.get(Fields.UNDERSCORE_ID)).isEqualTo(0);
	}

	@Test // DATAMONGO-1906
	void rendersConditionalProjectionCorrectly() {

		TypedAggregation aggregation = Aggregation.newAggregation(Book.class,
				Aggregation.project("title")
						.and(ConditionalOperators.when(ComparisonOperators.valueOf("author.middle").equalToValue(""))
								.then("$$REMOVE").otherwiseValueOf("author.middle"))
						.as("author.middle"));

		Document document = aggregation.toDocument("books", Aggregation.DEFAULT_CONTEXT);

		assertThat(document).isEqualTo(Document.parse(
				"{\"aggregate\" : \"books\", \"pipeline\" : [{\"$project\" : {\"title\" : 1, \"author.middle\" : {\"$cond\" : {\"if\" : {\"$eq\" : [\"$author.middle\", \"\"]}, \"then\" : \"$$REMOVE\",\"else\" : \"$author.middle\"} }}}]}"));
	}

	@Test // DATAMONGO-757
	void usesImplictAndExplicitFieldAliasAndIncludeExclude() {

		ProjectionOperation operation = Aggregation.project("foo").and("foobar").as("bar").andInclude("inc1", "inc2")
				.andExclude("_id");

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat(projectClause.get("foo")).isEqualTo(1); // implicit
		assertThat(projectClause.get("bar")).isEqualTo("$foobar"); // explicit
		assertThat(projectClause.get("inc1")).isEqualTo(1); // include shortcut
		assertThat(projectClause.get("inc2")).isEqualTo(1);
		assertThat(projectClause.get("_id")).isEqualTo(0);
	}

	@Test
	void arithmeticProjectionOperationModByZeroException() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ProjectionOperation().and("a").mod(0));
	}

	@Test // DATAMONGO-769
	void allowArithmeticOperationsWithFieldReferences() {

		ProjectionOperation operation = Aggregation.project() //
				.and("foo").plus("bar").as("fooPlusBar") //
				.and("foo").minus("bar").as("fooMinusBar") //
				.and("foo").multiply("bar").as("fooMultiplyBar") //
				.and("foo").divide("bar").as("fooDivideBar") //
				.and("foo").mod("bar").as("fooModBar");

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat(projectClause.get("fooPlusBar")). //
				isEqualTo(new Document("$add", Arrays.asList("$foo", "$bar")));
		assertThat(projectClause.get("fooMinusBar")). //
				isEqualTo(new Document("$subtract", Arrays.asList("$foo", "$bar")));
		assertThat(projectClause.get("fooMultiplyBar")). //
				isEqualTo(new Document("$multiply", Arrays.asList("$foo", "$bar")));
		assertThat(projectClause.get("fooDivideBar")). //
				isEqualTo(new Document("$divide", Arrays.asList("$foo", "$bar")));
		assertThat(projectClause.get("fooModBar")). //
				isEqualTo(new Document("$mod", Arrays.asList("$foo", "$bar")));
	}

	@Test // DATAMONGO-774
	void projectionExpressions() {

		ProjectionOperation operation = Aggregation.project() //
				.andExpression("(netPrice + surCharge) * taxrate * [0]", 2).as("grossSalesPrice") //
				.and("foo").as("bar"); //

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isEqualTo(Document.parse(
				"{ \"$project\" : { \"grossSalesPrice\" : { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\" , 2]} , \"bar\" : \"$foo\"}}"));
	}

	@Test // DATAMONGO-975
	void shouldRenderDateTimeFragmentExtractionsForSimpleFieldProjectionsCorrectly() {

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
		assertThat(document).isNotNull();

		Document projected = extractOperation("$project", document);

		assertThat(projected.get("hour")).isEqualTo(new Document("$hour", Arrays.asList("$date")));
		assertThat(projected.get("min")).isEqualTo(new Document("$minute", Arrays.asList("$date")));
		assertThat(projected.get("second")).isEqualTo(new Document("$second", Arrays.asList("$date")));
		assertThat(projected.get("millis")).isEqualTo(new Document("$millisecond", Arrays.asList("$date")));
		assertThat(projected.get("year")).isEqualTo(new Document("$year", Arrays.asList("$date")));
		assertThat(projected.get("month")).isEqualTo(new Document("$month", Arrays.asList("$date")));
		assertThat(projected.get("week")).isEqualTo(new Document("$week", Arrays.asList("$date")));
		assertThat(projected.get("dayOfYear")).isEqualTo(new Document("$dayOfYear", Arrays.asList("$date")));
		assertThat(projected.get("dayOfMonth")).isEqualTo(new Document("$dayOfMonth", Arrays.asList("$date")));
		assertThat(projected.get("dayOfWeek")).isEqualTo(new Document("$dayOfWeek", Arrays.asList("$date")));
	}

	@Test // DATAMONGO-975
	void shouldRenderDateTimeFragmentExtractionsForExpressionProjectionsCorrectly() throws Exception {

		ProjectionOperation operation = Aggregation.project() //
				.andExpression("date + 86400000") //
				.extractDayOfYear() //
				.as("dayOfYearPlus1Day") //
		;

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(document).isNotNull();

		Document projected = extractOperation("$project", document);
		assertThat(projected.get("dayOfYearPlus1Day")).isEqualTo(
				new Document("$dayOfYear", Arrays.asList(new Document("$add", Arrays.<Object> asList("$date", 86400000)))));
	}

	@Test // DATAMONGO-979
	void shouldRenderSizeExpressionInProjection() {

		ProjectionOperation operation = Aggregation //
				.project() //
				.and("tags") //
				.size()//
				.as("tags_count");

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		Document projected = extractOperation("$project", document);
		assertThat(projected.get("tags_count")).isEqualTo(new Document("$size", Arrays.asList("$tags")));
	}

	@Test // DATAMONGO-979
	void shouldRenderGenericSizeExpressionInProjection() {

		ProjectionOperation operation = Aggregation //
				.project() //
				.and(ArrayOperators.arrayOf("tags").length()) //
				.as("tags_count");

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		Document projected = extractOperation("$project", document);
		assertThat(projected.get("tags_count")).isEqualTo(new Document("$size", "$tags"));
	}

	@Test // DATAMONGO-1457
	void shouldRenderSliceCorrectly() throws Exception {

		ProjectionOperation operation = Aggregation.project().and("field").slice(10).as("renamed");

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projected = extractOperation("$project", document);

		assertThat(projected.get("renamed")).isEqualTo(new Document("$slice", Arrays.<Object> asList("$field", 10)));
	}

	@Test // DATAMONGO-1457
	void shouldRenderSliceWithPositionCorrectly() throws Exception {

		ProjectionOperation operation = Aggregation.project().and("field").slice(10, 5).as("renamed");

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projected = extractOperation("$project", document);

		assertThat(projected.get("renamed")).isEqualTo(new Document("$slice", Arrays.<Object> asList("$field", 5, 10)));
	}

	@Test // DATAMONGO-784
	void shouldRenderCmpCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").cmp(10).as("cmp10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).containsEntry("$project.cmp10.$cmp.[0]", "$field")
				.containsEntry("$project.cmp10.$cmp.[1]", 10);
	}

	@Test // DATAMONGO-784
	void shouldRenderEqCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").eq(10).as("eq10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).containsEntry("$project.eq10.$eq.[0]", "$field")
				.containsEntry("$project.eq10.$eq.[1]", 10);
	}

	@Test // DATAMONGO-784
	void shouldRenderGtCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").gt(10).as("gt10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).containsEntry("$project.gt10.$gt.[0]", "$field")
				.containsEntry("$project.gt10.$gt.[1]", 10);
	}

	@Test // DATAMONGO-784
	void shouldRenderGteCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").gte(10).as("gte10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).containsEntry("$project.gte10.$gte.[0]", "$field")
				.containsEntry("$project.gte10.$gte.[1]", 10);
	}

	@Test // DATAMONGO-784
	void shouldRenderLtCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").lt(10).as("lt10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).containsEntry("$project.lt10.$lt.[0]", "$field")
				.containsEntry("$project.lt10.$lt.[1]", 10);
	}

	@Test // DATAMONGO-784
	void shouldRenderLteCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").lte(10).as("lte10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).containsEntry("$project.lte10.$lte.[0]", "$field")
				.containsEntry("$project.lte10.$lte.[1]", 10);
	}

	@Test // DATAMONGO-784
	void shouldRenderNeCorrectly() {

		ProjectionOperation operation = Aggregation.project().and("field").ne(10).as("ne10");

		assertThat(operation.toDocument(Aggregation.DEFAULT_CONTEXT)).containsEntry("$project.ne10.$ne.[0]", "$field")
				.containsEntry("$project.ne10.$ne.[1]", 10);
	}

	@Test // DATAMONGO-1536
	void shouldRenderSetEquals() {

		Document agg = project("A", "B").and("A").equalsArrays("B").as("sameElements")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { A: 1, B: 1, sameElements: { $setEquals: [ \"$A\", \"$B\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSetEqualsAggregationExpresssion() {

		Document agg = project("A", "B").and(SetOperators.arrayAsSet("A").isEqualTo("B")).as("sameElements")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { A: 1, B: 1, sameElements: { $setEquals: [ \"$A\", \"$B\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSetIntersection() {

		Document agg = project("A", "B").and("A").intersectsArrays("B").as("commonToBoth")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project: { A: 1, B: 1, commonToBoth: { $setIntersection: [ \"$A\", \"$B\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSetIntersectionAggregationExpresssion() {

		Document agg = project("A", "B").and(SetOperators.arrayAsSet("A").intersects("B")).as("commonToBoth")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project: { A: 1, B: 1, commonToBoth: { $setIntersection: [ \"$A\", \"$B\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSetUnion() {

		Document agg = project("A", "B").and("A").unionArrays("B").as("allValues").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { A: 1, B: 1, allValues: { $setUnion: [ \"$A\", \"$B\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSetUnionAggregationExpresssion() {

		Document agg = project("A", "B").and(SetOperators.arrayAsSet("A").union("B")).as("allValues")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { A: 1, B: 1, allValues: { $setUnion: [ \"$A\", \"$B\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSetDifference() {

		Document agg = project("A", "B").and("B").differenceToArray("A").as("inBOnly")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { A: 1, B: 1, inBOnly: { $setDifference: [ \"$B\", \"$A\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSetDifferenceAggregationExpresssion() {

		Document agg = project("A", "B").and(SetOperators.arrayAsSet("B").differenceTo("A")).as("inBOnly")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { A: 1, B: 1, inBOnly: { $setDifference: [ \"$B\", \"$A\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSetIsSubset() {

		Document agg = project("A", "B").and("A").subsetOfArray("B").as("aIsSubsetOfB")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { A: 1, B: 1, aIsSubsetOfB: { $setIsSubset: [ \"$A\", \"$B\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSetIsSubsetAggregationExpresssion() {

		Document agg = project("A", "B").and(SetOperators.arrayAsSet("A").isSubsetOf("B")).as("aIsSubsetOfB")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { A: 1, B: 1, aIsSubsetOfB: { $setIsSubset: [ \"$A\", \"$B\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderAnyElementTrue() {

		Document agg = project("responses").and("responses").anyElementInArrayTrue().as("isAnyTrue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { responses: 1, isAnyTrue: { $anyElementTrue: [ \"$responses\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderAnyElementTrueAggregationExpresssion() {

		Document agg = project("responses").and(SetOperators.arrayAsSet("responses").anyElementTrue()).as("isAnyTrue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { responses: 1, isAnyTrue: { $anyElementTrue: [ \"$responses\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderAllElementsTrue() {

		Document agg = project("responses").and("responses").allElementsInArrayTrue().as("isAllTrue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { responses: 1, isAllTrue: { $allElementsTrue: [ \"$responses\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderAllElementsTrueAggregationExpresssion() {

		Document agg = project("responses").and(SetOperators.arrayAsSet("responses").allElementsTrue()).as("isAllTrue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { responses: 1, isAllTrue: { $allElementsTrue: [ \"$responses\" ] }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderAbs() {

		Document agg = project().and("anyNumber").absoluteValue().as("absoluteValue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { absoluteValue : { $abs:  \"$anyNumber\" }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderAbsAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end")).abs()).as("delta")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { delta: { $abs: { $subtract: [ \"$start\", \"$end\" ] } } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderAddAggregationExpresssion() {

		Document agg = project().and(ArithmeticOperators.valueOf("price").add("fee")).as("total")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(" { $project: { total: { $add: [ \"$price\", \"$fee\" ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderCeil() {

		Document agg = project().and("anyNumber").ceil().as("ceilValue").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { ceilValue : { $ceil:  \"$anyNumber\" }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderCeilAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end")).ceil()).as("delta")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { delta: { $ceil: { $subtract: [ \"$start\", \"$end\" ] } } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderDivide() {

		Document agg = project().and("value").divide(ArithmeticOperators.valueOf("start").subtract("end")).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project: { result: { $divide: [ \"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderDivideAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf("anyNumber").divideBy(ArithmeticOperators.valueOf("start").subtract("end")))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project: { result: { $divide: [ \"$anyNumber\", { $subtract: [ \"$start\", \"$end\" ] }] } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderExp() {

		Document agg = project().and("value").exp().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { result: { $exp: \"$value\" } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderExpAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end")).exp()).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { result: { $exp: { $subtract: [ \"$start\", \"$end\" ] } } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderFloor() {

		Document agg = project().and("value").floor().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { result: { $floor: \"$value\" } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderFloorAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end")).floor()).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { result: { $floor: { $subtract: [ \"$start\", \"$end\" ] } } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLn() {

		Document agg = project().and("value").ln().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { result: { $ln: \"$value\"} }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLnAggregationExpresssion() {

		Document agg = project().and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end")).ln())
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { result: { $ln: { $subtract: [ \"$start\", \"$end\" ] } } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLog() {

		Document agg = project().and("value").log(2).as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { result: { $log: [ \"$value\", 2] } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLogAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end")).log(2)).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { result: { $log: [ { $subtract: [ \"$start\", \"$end\" ] }, 2] } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLog10() {

		Document agg = project().and("value").log10().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { result: { $log10: \"$value\" } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLog10AggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end")).log10()).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { result: { $log10: { $subtract: [ \"$start\", \"$end\" ] } } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderMod() {

		Document agg = project().and("value").mod(ArithmeticOperators.valueOf("start").subtract("end")).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project: { result: { $mod: [\"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderModAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end")).mod(2)).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { result: { $mod: [{ $subtract: [ \"$start\", \"$end\" ] }, 2] } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderMultiply() {

		Document agg = project().and("value").multiply(ArithmeticOperators.valueOf("start").subtract("end")).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project: { result: { $multiply: [\"$value\", { $subtract: [ \"$start\", \"$end\" ] }] } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderMultiplyAggregationExpresssion() {

		Document agg = project().and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end"))
				.multiplyBy(2).multiplyBy("refToAnotherNumber")).as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { result: { $multiply: [{ $subtract: [ \"$start\", \"$end\" ] }, 2, \"$refToAnotherNumber\"] } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderPow() {

		Document agg = project().and("value").pow(2).as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { result: { $pow: [\"$value\", 2] } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderPowAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end")).pow(2)).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { result: { $pow: [{ $subtract: [ \"$start\", \"$end\" ] }, 2] } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSqrt() {

		Document agg = project().and("value").sqrt().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { result: { $sqrt: \"$value\" } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSqrtAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end")).sqrt()).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { result: { $sqrt: { $subtract: [ \"$start\", \"$end\" ] } } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSubtract() {

		Document agg = project().and("numericField").minus(ArrayOperators.arrayOf("someArray").length()).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project: { result: { $subtract: [ \"$numericField\", { $size : \"$someArray\"}] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSubtractAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf("numericField").subtract(ArrayOperators.arrayOf("someArray").length()))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project: { result: { $subtract: [ \"$numericField\", { $size : \"$someArray\"}] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderTrunc() {

		Document agg = project().and("value").trunc().as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { result : { $trunc: \"$value\" }}}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderTruncAggregationExpresssion() {

		Document agg = project()
				.and(ArithmeticOperators.valueOf(ArithmeticOperators.valueOf("start").subtract("end")).trunc()).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { result: { $trunc: { $subtract: [ \"$start\", \"$end\" ] } } }}"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderConcat() {

		Document agg = project().and("item").concat(" - ", field("description")).as("itemDescription")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project: { itemDescription: { $concat: [ \"$item\", \" - \", \"$description\" ] } } }"));

	}

	@Test // DATAMONGO-1536
	void shouldRenderConcatAggregationExpression() {

		Document agg = project().and(StringOperators.valueOf("item").concat(" - ").concatValueOf("description"))
				.as("itemDescription").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project: { itemDescription: { $concat: [ \"$item\", \" - \", \"$description\" ] } } }"));

	}

	@Test // DATAMONGO-1536
	void shouldRenderSubstr() {

		Document agg = project().and("quarter").substring(0, 2).as("yearSubstring").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { yearSubstring: { $substr: [ \"$quarter\", 0, 2 ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSubstrAggregationExpression() {

		Document agg = project().and(StringOperators.valueOf("quarter").substring(0, 2)).as("yearSubstring")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { yearSubstring: { $substr: [ \"$quarter\", 0, 2 ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderToLower() {

		Document agg = project().and("item").toLower().as("item").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { item: { $toLower: \"$item\" } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderToLowerAggregationExpression() {

		Document agg = project().and(StringOperators.valueOf("item").toLower()).as("item")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { item: { $toLower: \"$item\" } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderToUpper() {

		Document agg = project().and("item").toUpper().as("item").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { item: { $toUpper: \"$item\" } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderToUpperAggregationExpression() {

		Document agg = project().and(StringOperators.valueOf("item").toUpper()).as("item")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { item: { $toUpper: \"$item\" } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderStrCaseCmp() {

		Document agg = project().and("quarter").strCaseCmp("13q4").as("comparisonResult")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { comparisonResult: { $strcasecmp: [ \"$quarter\", \"13q4\" ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderStrCaseCmpAggregationExpression() {

		Document agg = project().and(StringOperators.valueOf("quarter").strCaseCmp("13q4")).as("comparisonResult")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { comparisonResult: { $strcasecmp: [ \"$quarter\", \"13q4\" ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderArrayElementAt() {

		Document agg = project().and("favorites").arrayElementAt(0).as("first").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { first: { $arrayElemAt: [ \"$favorites\", 0 ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderArrayElementAtAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("favorites").elementAt(0)).as("first")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { first: { $arrayElemAt: [ \"$favorites\", 0 ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderConcatArrays() {

		Document agg = project().and("instock").concatArrays("ordered").as("items").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { items: { $concatArrays: [ \"$instock\", \"$ordered\" ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderConcatArraysAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("instock").concat("ordered")).as("items")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { items: { $concatArrays: [ \"$instock\", \"$ordered\" ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderIsArray() {

		Document agg = project().and("instock").isArray().as("isAnArray").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { isAnArray: { $isArray: \"$instock\" } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderIsArrayAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("instock").isArray()).as("isAnArray")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { isAnArray: { $isArray: \"$instock\" } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSizeAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("instock").length()).as("arraySize")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { arraySize: { $size: \"$instock\" } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSliceAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("favorites").slice().itemCount(3)).as("threeFavorites")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { threeFavorites: { $slice: [ \"$favorites\", 3 ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSliceWithPositionAggregationExpression() {

		Document agg = project().and(ArrayOperators.arrayOf("favorites").slice().offset(2).itemCount(3))
				.as("threeFavorites").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { threeFavorites: { $slice: [ \"$favorites\", 2, 3 ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLiteral() {

		Document agg = project().and("$1").asLiteral().as("literalOnly").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { literalOnly: { $literal:  \"$1\"} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLiteralAggregationExpression() {

		Document agg = project().and(LiteralOperators.valueOf("$1").asLiteral()).as("literalOnly")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { literalOnly: { $literal:  \"$1\"} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderDayOfYearAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").dayOfYear()).as("dayOfYear")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { dayOfYear: { $dayOfYear: \"$date\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderDayOfYearAggregationExpressionWithTimezone() {

		Document agg = project()
				.and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).dayOfYear()).as("dayOfYear")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { dayOfYear: { $dayOfYear: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderTimeZoneFromField() {

		Document agg = project().and(DateOperators.dateOf("date").withTimezone(Timezone.ofField("tz")).dayOfYear())
				.as("dayOfYear").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project: { dayOfYear: { $dayOfYear: { \"date\" : \"$date\", \"timezone\" : \"$tz\" } } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderTimeZoneFromExpression() {

		Document agg = project()
				.and(DateOperators.dateOf("date")
						.withTimezone(Timezone.ofExpression(LiteralOperators.valueOf("America/Chicago").asLiteral())).dayOfYear())
				.as("dayOfYear").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { dayOfYear: { $dayOfYear: { \"date\" : \"$date\", \"timezone\" : { $literal: \"America/Chicago\"} } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderDayOfMonthAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").dayOfMonth()).as("day")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { day: { $dayOfMonth: \"$date\" }} }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderDayOfMonthAggregationExpressionWithTimezone() {

		Document agg = project()
				.and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).dayOfMonth()).as("day")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { day: { $dayOfMonth: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderDayOfWeekAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").dayOfWeek()).as("dayOfWeek")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { dayOfWeek: { $dayOfWeek: \"$date\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderDayOfWeekAggregationExpressionWithTimezone() {

		Document agg = project()
				.and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).dayOfWeek()).as("dayOfWeek")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { dayOfWeek: { $dayOfWeek: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderYearAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").year()).as("year")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { year: { $year: \"$date\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderYearAggregationExpressionWithTimezone() {

		Document agg = project().and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).year())
				.as("year").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project: { year: { $year: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderMonthAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").month()).as("month")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { month: { $month: \"$date\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderMonthAggregationExpressionWithTimezone() {

		Document agg = project().and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).month())
				.as("month").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project: { month: { $month: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderWeekAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").week()).as("week")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { week: { $week: \"$date\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderWeekAggregationExpressionWithTimezone() {

		Document agg = project().and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).week())
				.as("week").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project: { week: { $week: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderHourAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").hour()).as("hour")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { hour: { $hour: \"$date\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderHourAggregationExpressionWithTimezone() {

		Document agg = project().and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).hour())
				.as("hour").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project: { hour: { $hour: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderMinuteAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").minute()).as("minute")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { minute: { $minute: \"$date\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderMinuteAggregationExpressionWithTimezone() {

		Document agg = project()
				.and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).minute()).as("minute")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { minute: { $minute: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSecondAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").second()).as("second")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { second: { $second: \"$date\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderSecondAggregationExpressionWithTimezone() {

		Document agg = project()
				.and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).second()).as("second")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { second: { $second: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderMillisecondAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").millisecond()).as("msec")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { msec: { $millisecond: \"$date\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderMillisecondAggregationExpressionWithTimezone() {

		Document agg = project()
				.and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).millisecond()).as("msec")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { msec: { $millisecond: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderDateToString() {

		Document agg = project().and("date").dateAsFormattedString("%H:%M:%S:%L").as("time")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project: { time: { $dateToString: { format: \"%H:%M:%S:%L\", date: \"$date\" } } } }"));
	}

	@Test // DATAMONGO-2047
	void shouldRenderDateToStringWithoutFormatOption() {

		Document agg = project().and("date").dateAsFormattedString().as("time").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { time: { $dateToString: { date: \"$date\" } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderDateToStringAggregationExpression() {

		Document agg = project().and(DateOperators.dateOf("date").toString("%H:%M:%S:%L")).as("time")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project: { time: { $dateToString: { format: \"%H:%M:%S:%L\", date: \"$date\" } } } }"));
	}

	@Test // DATAMONGO-1834, DATAMONGO-2047
	void shouldRenderDateToStringAggregationExpressionWithTimezone() {

		Document agg = project()
				.and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).toString("%H:%M:%S:%L"))
				.as("time").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { time: { $dateToString: { format: \"%H:%M:%S:%L\", date: \"$date\", \"timezone\" : \"America/Chicago\" } } } } } }"));

		Document removedTimezone = project().and(DateOperators.dateOf("date")
				.withTimezone(Timezone.valueOf("America/Chicago")).toString("%H:%M:%S:%L").withTimezone(Timezone.none()))
				.as("time").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(removedTimezone).isEqualTo(
				Document.parse("{ $project: { time: { $dateToString: { format: \"%H:%M:%S:%L\", date: \"$date\" } } } } } }"));
	}

	@Test // DATAMONGO-2047
	void shouldRenderDateToStringWithOnNull() {

		Document agg = project()
				.and(DateOperators.dateOf("date").toStringWithDefaultFormat().onNullReturnValueOf("fallback-field")).as("time")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project: { time: { $dateToString: { date: \"$date\", \"onNull\" : \"$fallback-field\" } } } }"));
	}

	@Test // DATAMONGO-2047
	void shouldRenderDateToStringWithOnNullExpression() {

		Document agg = project()
				.and(DateOperators.dateOf("date").toStringWithDefaultFormat()
						.onNullReturnValueOf(LiteralOperators.valueOf("my-literal").asLiteral()))
				.as("time").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { time: { $dateToString: { date: \"$date\", \"onNull\" : { \"$literal\": \"my-literal\"} } } } }"));
	}

	@Test // DATAMONGO-2047
	void shouldRenderDateToStringWithOnNullAndTimezone() {

		Document agg = project().and(DateOperators.dateOf("date").toStringWithDefaultFormat()
				.onNullReturnValueOf("fallback-field").withTimezone(Timezone.ofField("foo"))).as("time")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { time: { $dateToString: { date: \"$date\", \"onNull\" : \"$fallback-field\", \"timezone\": \"$foo\" } } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSumAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("quizzes").sum()).as("quizTotal")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { quizTotal: { $sum: \"$quizzes\"} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderSumWithMultipleArgsAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("final").sum().and("midterm")).as("examTotal")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: {  examTotal: { $sum: [ \"$final\", \"$midterm\" ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderAvgAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("quizzes").avg()).as("quizAvg")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { quizAvg: { $avg: \"$quizzes\"} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderAvgWithMultipleArgsAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("final").avg().and("midterm")).as("examAvg")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: {  examAvg: { $avg: [ \"$final\", \"$midterm\" ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderMaxAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("quizzes").max()).as("quizMax")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { quizMax: { $max: \"$quizzes\"} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderMaxWithMultipleArgsAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("final").max().and("midterm")).as("examMax")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: {  examMax: { $max: [ \"$final\", \"$midterm\" ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderMinAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("quizzes").min()).as("quizMin")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { quizMin: { $min: \"$quizzes\"} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderMinWithMultipleArgsAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("final").min().and("midterm")).as("examMin")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: {  examMin: { $min: [ \"$final\", \"$midterm\" ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderStdDevPopAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("scores").stdDevPop()).as("stdDev")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { stdDev: { $stdDevPop: \"$scores\"} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderStdDevSampAggregationExpression() {

		Document agg = project().and(ArithmeticOperators.valueOf("scores").stdDevSamp()).as("stdDev")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { stdDev: { $stdDevSamp: \"$scores\"} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderCmpAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").compareToValue(250)).as("cmp250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { cmp250: { $cmp: [\"$qty\", 250]} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderEqAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").equalToValue(250)).as("eq250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { eq250: { $eq: [\"$qty\", 250]} } }"));
	}

	@Test // DATAMONGO-2513
	void shouldRenderEqAggregationExpressionWithListComparison() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").equalToValue(Arrays.asList(250))).as("eq250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { eq250: { $eq: [\"$qty\", [250]]} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderGtAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").greaterThanValue(250)).as("gt250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { gt250: { $gt: [\"$qty\", 250]} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderGteAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").greaterThanEqualToValue(250)).as("gte250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { gte250: { $gte: [\"$qty\", 250]} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLtAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").lessThanValue(250)).as("lt250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { lt250: { $lt: [\"$qty\", 250]} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLteAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").lessThanEqualToValue(250)).as("lte250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { lte250: { $lte: [\"$qty\", 250]} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderNeAggregationExpression() {

		Document agg = project().and(ComparisonOperators.valueOf("qty").notEqualToValue(250)).as("ne250")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { ne250: { $ne: [\"$qty\", 250]} } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLogicAndAggregationExpression() {

		Document agg = project()
				.and(BooleanOperators.valueOf(ComparisonOperators.valueOf("qty").greaterThanValue(100))
						.and(ComparisonOperators.valueOf("qty").lessThanValue(250)))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project: { result: { $and: [ { $gt: [ \"$qty\", 100 ] }, { $lt: [ \"$qty\", 250 ] } ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderLogicOrAggregationExpression() {

		Document agg = project()
				.and(BooleanOperators.valueOf(ComparisonOperators.valueOf("qty").greaterThanValue(250))
						.or(ComparisonOperators.valueOf("qty").lessThanValue(200)))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project: { result: { $or: [ { $gt: [ \"$qty\", 250 ] }, { $lt: [ \"$qty\", 200 ] } ] } } }"));
	}

	@Test // DATAMONGO-1536
	void shouldRenderNotAggregationExpression() {

		Document agg = project().and(BooleanOperators.not(ComparisonOperators.valueOf("qty").greaterThanValue(250)))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { result: { $not: [ { $gt: [ \"$qty\", 250 ] } ] } } }"));
	}

	@Test // DATAMONGO-1540
	void shouldRenderMapAggregationExpression() {

		Document agg = Aggregation.project()
				.and(VariableOperators.mapItemsOf("quizzes").as("grade").andApply(ArithmeticOperators.valueOf("grade").add(2)))
				.as("adjustedGrades").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project:{ adjustedGrades:{ $map: { input: \"$quizzes\", as: \"grade\",in: { $add: [ \"$$grade\", 2 ] }}}}}"));
	}

	@Test // DATAMONGO-1540
	void shouldRenderMapAggregationExpressionOnExpression() {

		Document agg = Aggregation.project()
				.and(VariableOperators.mapItemsOf(ArrayOperators.arrayOf("foo").length()).as("grade")
						.andApply(ArithmeticOperators.valueOf("grade").add(2)))
				.as("adjustedGrades").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project:{ adjustedGrades:{ $map: { input: { $size : \"$foo\"}, as: \"grade\",in: { $add: [ \"$$grade\", 2 ] }}}}}"));
	}

	@Test // DATAMONGO-861, DATAMONGO-1542
	void shouldRenderIfNullConditionAggregationExpression() {

		Document agg = project().and(
				ConditionalOperators.ifNull(ArrayOperators.arrayOf("array").elementAt(1)).then("a more sophisticated value"))
				.as("result").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project: { result: { $ifNull: [ { $arrayElemAt: [\"$array\", 1] }, \"a more sophisticated value\" ] } } }"));
	}

	@Test // DATAMONGO-1542
	void shouldRenderIfNullValueAggregationExpression() {

		Document agg = project()
				.and(ConditionalOperators.ifNull("field").then(ArrayOperators.arrayOf("array").elementAt(1))).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project: { result: { $ifNull: [ \"$field\", { $arrayElemAt: [\"$array\", 1] } ] } } }"));
	}

	@Test // DATAMONGO-861, DATAMONGO-1542
	void fieldReplacementIfNullShouldRenderCorrectly() {

		Document agg = project().and(ConditionalOperators.ifNull("optional").thenValueOf("$never-null")).as("result")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { result: { $ifNull: [ \"$optional\", \"$never-null\" ] } } }"));
	}

	@Test // DATAMONGO-1538
	void shouldRenderLetExpressionCorrectly() {

		Document agg = Aggregation.project()
				.and(VariableOperators
						.define(newVariable("total").forExpression(ArithmeticOperators.valueOf("price").add("tax")),
								newVariable("discounted")
										.forExpression(ConditionalOperators.Cond.when("applyDiscount").then(0.9D).otherwise(1.0D)))
						.andApply(ArithmeticOperators.valueOf("total").multiplyBy("discounted"))) //
				.as("finalTotal").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project:{  \"finalTotal\" : { \"$let\": {" + //
				"\"vars\": {" + //
				"\"total\": { \"$add\": [ \"$price\", \"$tax\" ] }," + //
				"\"discounted\": { \"$cond\": { \"if\": \"$applyDiscount\", \"then\": 0.9, \"else\": 1.0 } }" + //
				"}," + //
				"\"in\": { \"$multiply\": [ \"$$total\", \"$$discounted\" ] }" + //
				"}}}}"));
	}

	@Test // DATAMONGO-1538
	void shouldRenderLetExpressionCorrectlyWhenUsingLetOnProjectionBuilder() {

		ExpressionVariable var1 = newVariable("total").forExpression(ArithmeticOperators.valueOf("price").add("tax"));

		ExpressionVariable var2 = newVariable("discounted")
				.forExpression(ConditionalOperators.Cond.when("applyDiscount").then(0.9D).otherwise(1.0D));

		Document agg = Aggregation.project().and("foo")
				.let(Arrays.asList(var1, var2), ArithmeticOperators.valueOf("total").multiplyBy("discounted")).as("finalTotal")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project:{ \"finalTotal\" : { \"$let\": {" + //
				"\"vars\": {" + //
				"\"total\": { \"$add\": [ \"$price\", \"$tax\" ] }," + //
				"\"discounted\": { \"$cond\": { \"if\": \"$applyDiscount\", \"then\": 0.9, \"else\": 1.0 } }" + //
				"}," + //
				"\"in\": { \"$multiply\": [ \"$$total\", \"$$discounted\" ] }" + //
				"}}}}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderIndexOfBytesCorrectly() {

		Document agg = project().and(StringOperators.valueOf("item").indexOf("foo")).as("byteLocation")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project: { byteLocation: { $indexOfBytes: [ \"$item\", \"foo\" ] } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderIndexOfBytesWithRangeCorrectly() {

		Document agg = project()
				.and(StringOperators.valueOf("item").indexOf("foo")
						.within(Range.from(Bound.inclusive(5L)).to(Bound.exclusive(9L))))
				.as("byteLocation").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).containsEntry("$project.byteLocation.$indexOfBytes.[2]", 5L)
				.containsEntry("$project.byteLocation.$indexOfBytes.[3]", 9L);
	}

	@Test // DATAMONGO-1548
	void shouldRenderIndexOfCPCorrectly() {

		Document agg = project().and(StringOperators.valueOf("item").indexOfCP("foo")).as("cpLocation")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { cpLocation: { $indexOfCP: [ \"$item\", \"foo\" ] } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderIndexOfCPWithRangeCorrectly() {

		Document agg = project()
				.and(StringOperators.valueOf("item").indexOfCP("foo")
						.within(Range.from(Bound.inclusive(5L)).to(Bound.exclusive(9L))))
				.as("cpLocation").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).containsEntry("$project.cpLocation.$indexOfCP.[2]", 5L)
				.containsEntry("$project.cpLocation.$indexOfCP.[3]", 9L);
	}

	@Test // DATAMONGO-1548
	void shouldRenderSplitCorrectly() {

		Document agg = project().and(StringOperators.valueOf("city").split(", ")).as("city_state")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { city_state : { $split: [\"$city\", \", \"] }} }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderStrLenBytesCorrectly() {

		Document agg = project().and(StringOperators.valueOf("name").length()).as("length")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { \"length\": { $strLenBytes: \"$name\" } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderStrLenCPCorrectly() {

		Document agg = project().and(StringOperators.valueOf("name").lengthCP()).as("length")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { \"length\": { $strLenCP: \"$name\" } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderSubstrCPCorrectly() {

		Document agg = project().and(StringOperators.valueOf("quarter").substringCP(0, 2)).as("yearSubstring")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project : { yearSubstring: { $substrCP: [ \"$quarter\", 0, 2 ] } } }"));
	}

	@Test // GH-3725
	void shouldRenderRegexFindCorrectly() {

		Document agg = project().and(StringOperators.valueOf("field1").regexFind("e")).as("regex")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project : { regex: { $regexFind: { \"input\" : \"$field1\", \"regex\" : \"e\" } } } }"));
	}

	@Test // GH-3725
	void shouldRenderRegexFindAllCorrectly() {

		Document agg = project().and(StringOperators.valueOf("field1").regexFindAll("e")).as("regex")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project : { regex: { $regexFindAll: { \"input\" : \"$field1\", \"regex\" : \"e\" } } } }"));
	}

	@Test // GH-3725
	void shouldRenderRegexMatchCorrectly() {

		Document agg = project().and(StringOperators.valueOf("field1").regexMatch("e")).as("regex")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project : { regex: { $regexMatch: { \"input\" : \"$field1\", \"regex\" : \"e\" } } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderIndexOfArrayCorrectly() {

		Document agg = project().and(ArrayOperators.arrayOf("items").indexOf(2)).as("index")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { index: { $indexOfArray: [ \"$items\", 2 ] } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderRangeCorrectly() {

		Document agg = project().and(ArrayOperators.RangeOperator.rangeStartingAt(0L).to("distance").withStepSize(25L))
				.as("rest_stops").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).containsEntry("$project.rest_stops.$range.[0]", 0L)
				.containsEntry("$project.rest_stops.$range.[1]", "$distance")
				.containsEntry("$project.rest_stops.$range.[2]", 25L);
	}

	@Test // DATAMONGO-1548
	void shouldRenderReverseArrayCorrectly() {

		Document agg = project().and(ArrayOperators.arrayOf("favorites").reverse()).as("reverseFavorites")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { reverseFavorites: { $reverseArray: \"$favorites\" } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderReduceWithSimpleObjectCorrectly() {

		Document agg = project()
				.and(ArrayOperators.arrayOf("probabilityArr")
						.reduce(ArithmeticOperators.valueOf("$$value").multiplyBy("$$this")).startingWith(1))
				.as("results").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project : { \"results\": { $reduce: { input: \"$probabilityArr\", initialValue: 1, in: { $multiply: [ \"$$value\", \"$$this\" ] } } } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderReduceWithComplexObjectCorrectly() {

		PropertyExpression sum = PropertyExpression.property("sum").definedAs(
				ArithmeticOperators.valueOf(Variable.VALUE.referringTo("sum").getName()).add(Variable.THIS.getName()));
		PropertyExpression product = PropertyExpression.property("product").definedAs(ArithmeticOperators
				.valueOf(Variable.VALUE.referringTo("product").getName()).multiplyBy(Variable.THIS.getName()));

		Document agg = project()
				.and(ArrayOperators.arrayOf("probabilityArr").reduce(sum, product)
						.startingWith(new Document().append("sum", 5).append("product", 2)))
				.as("results").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project : { \"results\": { $reduce: { input: \"$probabilityArr\", initialValue:  { \"sum\" : 5 , \"product\" : 2} , in: { \"sum\": { $add : [\"$$value.sum\", \"$$this\"] }, \"product\": { $multiply: [ \"$$value.product\", \"$$this\" ] } } } } } }"));
	}

	@Test // DATAMONGO-1843
	void shouldRenderReduceWithInputAndInExpressionsCorrectly() {

		Document expected = Document.parse(
				"{ \"$project\" : { \"results\" : { \"$reduce\" : { \"input\" : { \"$slice\" : [\"$array\", 5] }, \"initialValue\" : \"\", \"in\" : { \"$concat\" : [\"$$value\", \"/\", \"$$this\"] } } } } }");

		Reduce reduceEntryPoint = Reduce.arrayOf(Slice.sliceArrayOf("array").itemCount(5)) //
				.withInitialValue("") //
				.reduce(Concat.valueOf("$$value").concat("/").concatValueOf("$$this"));

		Reduce arrayEntryPoint = ArrayOperators.arrayOf(Slice.sliceArrayOf("array").itemCount(5)) //
				.reduce(Concat.valueOf("$$value").concat("/").concatValueOf("$$this")) //
				.startingWith("");

		assertThat(project().and(reduceEntryPoint).as("results").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(expected);

		assertThat(project().and(arrayEntryPoint).as("results").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(expected);
	}

	@Test // DATAMONGO-1548
	void shouldRenderZipCorrectly() {

		AggregationExpression elemAt0 = ArrayOperators.arrayOf("matrix").elementAt(0);
		AggregationExpression elemAt1 = ArrayOperators.arrayOf("matrix").elementAt(1);
		AggregationExpression elemAt2 = ArrayOperators.arrayOf("matrix").elementAt(2);

		Document agg = project().and(
				ArrayOperators.arrayOf(elemAt0).zipWith(elemAt1, elemAt2).useLongestLength().defaultTo(new Object[] { 1, 2 }))
				.as("transposed").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project : {  transposed: { $zip: { inputs: [ { $arrayElemAt: [ \"$matrix\", 0 ] }, { $arrayElemAt: [ \"$matrix\", 1 ] }, { $arrayElemAt: [ \"$matrix\", 2 ] } ], useLongestLength : true, defaults: [1,2] } } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderInCorrectly() {

		Document agg = project().and(ArrayOperators.arrayOf("in_stock").containsValue("bananas")).as("has_bananas")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project : { has_bananas : { $in : [\"bananas\", \"$in_stock\" ] } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderIsoDayOfWeekCorrectly() {

		Document agg = project().and(DateOperators.dateOf("birthday").isoDayOfWeek()).as("dayOfWeek")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { dayOfWeek: { $isoDayOfWeek: \"$birthday\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderIsoDayOfWeekWithTimezoneCorrectly() {

		Document agg = project()
				.and(DateOperators.dateOf("birthday").withTimezone(Timezone.valueOf("America/Chicago")).isoDayOfWeek())
				.as("dayOfWeek").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project : { dayOfWeek: { $isoDayOfWeek: { \"date\" : \"$birthday\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderIsoWeekCorrectly() {

		Document agg = project().and(DateOperators.dateOf("date").isoWeek()).as("weekNumber")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { weekNumber: { $isoWeek: \"$date\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderIsoWeekWithTimezoneCorrectly() {

		Document agg = project()
				.and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).isoWeek()).as("weekNumber")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project : { weekNumber: { $isoWeek: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderIsoWeekYearCorrectly() {

		Document agg = project().and(DateOperators.dateOf("date").isoWeekYear()).as("yearNumber")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { yearNumber: { $isoWeekYear: \"$date\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderIsoWeekYearWithTimezoneCorrectly() {

		Document agg = project()
				.and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).isoWeekYear())
				.as("yearNumber").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project : { yearNumber: { $isoWeekYear: { \"date\" : \"$date\", \"timezone\" : \"America/Chicago\" } } } } }"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderSwitchCorrectly() {

		String expected = "$switch:\n" + //
				"{\n" + //
				"     branches: [\n" + //
				"       {\n" + //
				"         case: { $gte : [ { $avg : \"$scores\" }, 90 ] },\n" + //
				"         then: \"Doing great\"\n" + //
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
				.then("Doing great");
		CaseOperator cond2 = CaseOperator
				.when(BooleanOperators.And.and(
						ComparisonOperators.Gte.valueOf(AccumulatorOperators.Avg.avgOf("scores")).greaterThanEqualToValue(80),
						ComparisonOperators.Lt.valueOf(AccumulatorOperators.Avg.avgOf("scores")).lessThanValue(90)))
				.then("Doing pretty well.");
		CaseOperator cond3 = CaseOperator
				.when(ComparisonOperators.Lt.valueOf(AccumulatorOperators.Avg.avgOf("scores")).lessThanValue(80))
				.then("Needs improvement.");

		Document agg = project().and(ConditionalOperators.switchCases(cond1, cond2, cond3).defaultTo("No scores found."))
				.as("summary").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { summary: {" + expected + "} } }"));
	}

	@Test // DATAMONGO-1548
	void shouldTypeCorrectly() {

		Document agg = project().and(DataTypeOperators.Type.typeOf("a")).as("a").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { a: { $type: \"$a\" } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderDateFromPartsWithJustTheYear() {

		Document agg = project().and(DateOperators.dateFromParts().year(2018)).as("newDate")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { newDate: { $dateFromParts: { year : 2018 } } } }"));
	}

	@Test // DATAMONGO-1834, DATAMONGO-2671
	void shouldRenderDateFromParts() {

		Document agg = project()
				.and(DateOperators.dateFromParts().year(2018).month(3).day(23).hour(14).minute(25).second(10).millisecond(2))
				.as("newDate").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project : { newDate: { $dateFromParts: { year : 2018, month : 3, day : 23, hour : 14, minute : 25, second : 10, millisecond : 2 } } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderDateFromPartsWithTimezone() {

		Document agg = project()
				.and(DateOperators.dateFromParts().withTimezone(Timezone.valueOf("America/Chicago")).year(2018)).as("newDate")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project : { newDate: { $dateFromParts: { year : 2018, timezone : \"America/Chicago\" } } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderIsoDateFromPartsWithJustTheYear() {

		Document agg = project().and(DateOperators.dateFromParts().isoWeekYear(2018)).as("newDate")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { newDate: { $dateFromParts: { isoWeekYear : 2018 } } } }"));
	}

	@Test // DATAMONGO-1834, DATAMONGO-2671
	void shouldRenderIsoDateFromParts() {

		Document agg = project().and(DateOperators.dateFromParts().isoWeekYear(2018).isoWeek(12).isoDayOfWeek(5).hour(14)
				.minute(30).second(42).millisecond(2)).as("newDate").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project : { newDate: { $dateFromParts: { isoWeekYear : 2018, isoWeek : 12, isoDayOfWeek : 5, hour : 14, minute : 30, second : 42, millisecond : 2 } } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderIsoDateFromPartsWithTimezone() {

		Document agg = project()
				.and(DateOperators.dateFromParts().withTimezone(Timezone.valueOf("America/Chicago")).isoWeekYear(2018))
				.as("newDate").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project : { newDate: { $dateFromParts: { isoWeekYear : 2018, timezone : \"America/Chicago\" } } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderDateToParts() {

		Document agg = project().and(DateOperators.dateOf("date").toParts()).as("newDate")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project : { newDate: { $dateToParts: { date : \"$date\" } } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderDateToIsoParts() {

		Document agg = project().and(DateOperators.dateOf("date").toParts().iso8601()).as("newDate")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(
				Document.parse("{ $project : { newDate: { $dateToParts: { date : \"$date\", iso8601 : true } } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderDateToPartsWithTimezone() {

		Document agg = project()
				.and(DateOperators.dateOf("date").withTimezone(Timezone.valueOf("America/Chicago")).toParts()).as("newDate")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project : { newDate: { $dateToParts: { date : \"$date\", timezone : \"America/Chicago\" } } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderDateFromString() {

		Document agg = project().and(DateOperators.dateFromString("2017-02-08T12:10:40.787")).as("newDate")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document
				.parse("{ $project : { newDate: { $dateFromString: { dateString : \"2017-02-08T12:10:40.787\" } } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderDateFromStringWithFieldReference() {

		Document agg = project().and(DateOperators.dateOf("date").fromString()).as("newDate")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $project : { newDate: { $dateFromString: { dateString : \"$date\" } } } }"));
	}

	@Test // DATAMONGO-1834
	void shouldRenderDateFromStringWithTimezone() {

		Document agg = project()
				.and(DateOperators.dateFromString("2017-02-08T12:10:40.787").withTimezone(Timezone.valueOf("America/Chicago")))
				.as("newDate").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project : { newDate: { $dateFromString: { dateString : \"2017-02-08T12:10:40.787\", timezone : \"America/Chicago\" } } } }"));
	}

	@Test // DATAMONGO-2047
	void shouldRenderDateFromStringWithFormat() {

		Document agg = project().and(DateOperators.dateFromString("2017-02-08T12:10:40.787").withFormat("dd/mm/yyyy"))
				.as("newDate").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse(
				"{ $project : { newDate: { $dateFromString: { dateString : \"2017-02-08T12:10:40.787\", format : \"dd/mm/yyyy\" } } } }"));
	}

	@Test // DATAMONGO-2200
	void typeProjectionShouldIncludeTopLevelFieldsOfType() {

		ProjectionOperation operation = Aggregation.project(Book.class);

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat(projectClause) //
				.hasSize(2) //
				.containsEntry("title", 1) //
				.containsEntry("author", 1);
	}

	@Test // DATAMONGO-2200
	void typeProjectionShouldMapFieldNames() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		MongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);

		Document document = Aggregation.project(BookRenamed.class)
				.toDocument(new TypeBasedAggregationOperationContext(Book.class, mappingContext, new QueryMapper(converter)));
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat(projectClause) //
				.hasSize(2) //
				.containsEntry("ti_tl_e", 1) //
				.containsEntry("author", 1);
	}

	@Test // DATAMONGO-2200
	void typeProjectionShouldIncludeInterfaceProjectionValues() {

		ProjectionOperation operation = Aggregation.project(ProjectionInterface.class);

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat(projectClause) //
				.hasSize(1) //
				.containsEntry("title", 1);
	}

	@Test // DATAMONGO-2200
	void typeProjectionShouldBeEmptyIfNoPropertiesFound() {

		ProjectionOperation operation = Aggregation.project(EmptyType.class);

		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		Document projectClause = DocumentTestUtils.getAsDocument(document, PROJECT);

		assertThat(projectClause).isEmpty();
	}

	@Test // DATAMONGO-2312
	void simpleFieldReferenceAsArray() {

		org.bson.Document doc = Aggregation.newAggregation(project("x", "y", "someField").asArray("myArray"))
				.toDocument("coll", Aggregation.DEFAULT_CONTEXT);

		assertThat(doc).isEqualTo(Document.parse(
				"{\"aggregate\":\"coll\", \"pipeline\":[ { $project: { myArray: [ \"$x\", \"$y\", \"$someField\" ] } } ] }"));
	}

	@Test // DATAMONGO-2312
	void mappedFieldReferenceAsArray() {

		MongoMappingContext mappingContext = new MongoMappingContext();

		org.bson.Document doc = Aggregation
				.newAggregation(BookWithFieldAnnotation.class, project("title", "author").asArray("myArray"))
				.toDocument("coll", new TypeBasedAggregationOperationContext(BookWithFieldAnnotation.class, mappingContext,
						new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext))));

		assertThat(doc).isEqualTo(Document
				.parse("{\"aggregate\":\"coll\", \"pipeline\":[ { $project: { myArray: [ \"$ti_t_le\", \"$author\" ] } } ] }"));
	}

	@Test // DATAMONGO-2312
	void arrayWithNullValue() {

		Document doc = project() //
				.andArrayOf(Fields.field("field-1"), null, "value").as("myArray") //
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(doc).isEqualTo(Document.parse("{ $project: { \"myArray\" : [ \"$field-1\", null, \"value\" ] } }"));
	}

	@Test // DATAMONGO-2312
	void nestedArrayField() {

		Document doc = project("_id", "value") //
				.andArrayOf(Fields.field("field-1"), "plain - string", ArithmeticOperators.valueOf("field-1").sum().and(10))
				.as("myArray") //
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(doc).isEqualTo(Document.parse(
				"{ $project: { \"_id\" : 1, \"value\" : 1, \"myArray\" : [ \"$field-1\", \"plain - string\", { \"$sum\" : [\"$field-1\", 10] } ] } } ] }"));
	}

	@Test // DATAMONGO-2312
	void nestedMappedFieldReferenceInArrayField() {

		MongoMappingContext mappingContext = new MongoMappingContext();

		Document doc = project("author") //
				.andArrayOf(Fields.field("title"), "plain - string", ArithmeticOperators.valueOf("title").sum().and(10))
				.as("myArray") //
				.toDocument(new TypeBasedAggregationOperationContext(BookWithFieldAnnotation.class, mappingContext,
						new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext))));

		assertThat(doc).isEqualTo(Document.parse(
				"{ $project: { \"author\" : 1,  \"myArray\" : [ \"$ti_t_le\", \"plain - string\", { \"$sum\" : [\"$ti_t_le\", 10] } ] } } ] }"));
	}

	@Test // GH-4473
	void shouldRenderPercentileAggregationExpression() {

		Document agg = project()
			.and(ArithmeticOperators.valueOf("score").percentile(0.3, 0.9)).as("scorePercentiles")
			.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { scorePercentiles: { $percentile: { input: \"$score\", method: \"approximate\", p: [0.3, 0.9] } }} } }"));
	}

	@Test // GH-4473
	void shouldRenderPercentileWithMultipleArgsAggregationExpression() {

		Document agg = project()
			.and(ArithmeticOperators.valueOf("scoreOne").percentile(0.4).and("scoreTwo")).as("scorePercentiles")
			.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $project: { scorePercentiles: { $percentile: { input: [\"$scoreOne\", \"$scoreTwo\"], method: \"approximate\", p: [0.4] } }} } }"));
	}

	@Test // GH-4472
	void shouldRenderMedianAggregationExpressions() {

		Document singleArgAgg = project()
				.and(ArithmeticOperators.valueOf("score").median()).as("medianValue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(singleArgAgg).isEqualTo(Document.parse("{ $project: { medianValue: { $median: { input: \"$score\", method: \"approximate\" } }} } }"));

		Document multipleArgsAgg = project()
				.and(ArithmeticOperators.valueOf("score").median().and("scoreTwo")).as("medianValue")
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(multipleArgsAgg).isEqualTo(Document.parse("{ $project: { medianValue: { $median: { input: [\"$score\", \"$scoreTwo\"], method: \"approximate\" } }} } }"));
	}

	private static Document extractOperation(String field, Document fromProjectClause) {
		return (Document) fromProjectClause.get(field);
	}

	static class Book {

		String title;
		Author author;

		public Book() {}

		public String getTitle() {
			return this.title;
		}

		public Author getAuthor() {
			return this.author;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public void setAuthor(Author author) {
			this.author = author;
		}

		public String toString() {
			return "ProjectionOperationUnitTests.Book(title=" + this.getTitle() + ", author=" + this.getAuthor() + ")";
		}
	}

	static class BookWithFieldAnnotation {

		@Field("ti_t_le") String title;
		Author author;

		public String getTitle() {
			return this.title;
		}

		public Author getAuthor() {
			return this.author;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public void setAuthor(Author author) {
			this.author = author;
		}

		public String toString() {
			return "ProjectionOperationUnitTests.BookWithFieldAnnotation(title=" + this.getTitle() + ", author="
					+ this.getAuthor() + ")";
		}
	}

	static class BookRenamed {

		@Field("ti_tl_e") String title;
		Author author;

		public String getTitle() {
			return this.title;
		}

		public Author getAuthor() {
			return this.author;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public void setAuthor(Author author) {
			this.author = author;
		}

		public String toString() {
			return "ProjectionOperationUnitTests.BookRenamed(title=" + this.getTitle() + ", author=" + this.getAuthor() + ")";
		}
	}

	static class Author {

		String first;
		String last;
		String middle;

		public String getFirst() {
			return this.first;
		}

		public String getLast() {
			return this.last;
		}

		public String getMiddle() {
			return this.middle;
		}

		public void setFirst(String first) {
			this.first = first;
		}

		public void setLast(String last) {
			this.last = last;
		}

		public void setMiddle(String middle) {
			this.middle = middle;
		}

		public String toString() {
			return "ProjectionOperationUnitTests.Author(first=" + this.getFirst() + ", last=" + this.getLast() + ", middle="
					+ this.getMiddle() + ")";
		}
	}

	interface ProjectionInterface {
		String getTitle();
	}

	private static class EmptyType {

	}

}
