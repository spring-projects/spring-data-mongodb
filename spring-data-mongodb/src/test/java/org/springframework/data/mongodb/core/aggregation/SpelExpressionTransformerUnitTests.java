/*
 * Copyright 2013-2018 the original author or authors.
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

import java.util.Arrays;

import org.bson.Document;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.mongodb.core.Person;

/**
 * Unit tests for {@link SpelExpressionTransformer}.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class SpelExpressionTransformerUnitTests {

	SpelExpressionTransformer transformer = new SpelExpressionTransformer();

	Data data;

	@Before
	public void setup() {

		this.data = new Data();
		this.data.primitiveLongValue = 42;
		this.data.primitiveDoubleValue = 1.2345;
		this.data.doubleValue = 23.0;
		this.data.item = new DataItem();
		this.data.item.primitiveIntValue = 21;
	}

	@Test // DATAMONGO-774
	public void shouldRenderConstantExpression() {

		assertThat(transform("1"), is((Object) "1"));
		assertThat(transform("-1"), is((Object) "-1"));
		assertThat(transform("1.0"), is((Object) "1.0"));
		assertThat(transform("-1.0"), is((Object) "-1.0"));
		assertThat(transform("null"), is(nullValue()));
	}

	@Test // DATAMONGO-774
	public void shouldSupportKnownOperands() {

		assertThat(transform("a + b"), is((Object) Document.parse("{ \"$add\" : [ \"$a\" , \"$b\"]}")));
		assertThat(transform("a - b"), is((Object) Document.parse("{ \"$subtract\" : [ \"$a\" , \"$b\"]}")));
		assertThat(transform("a * b"), is((Object) Document.parse("{ \"$multiply\" : [ \"$a\" , \"$b\"]}")));
		assertThat(transform("a / b"), is((Object) Document.parse("{ \"$divide\" : [ \"$a\" , \"$b\"]}")));
		assertThat(transform("a % b"), is((Object) Document.parse("{ \"$mod\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-774
	public void shouldThrowExceptionOnUnknownOperand() {
		transform("a++");
	}

	@Test // DATAMONGO-774
	public void shouldRenderSumExpression() {
		assertThat(transform("a + 1"), is((Object) Document.parse("{ \"$add\" : [ \"$a\" , 1]}")));
	}

	@Test // DATAMONGO-774
	public void shouldRenderFormula() {

		assertThat(transform("(netPrice + surCharge) * taxrate + 42"), is((Object) Document.parse(
				"{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}")));
	}

	@Test // DATAMONGO-774
	public void shouldRenderFormulaInCurlyBrackets() {

		assertThat(transform("{(netPrice + surCharge) * taxrate + 42}"), is((Object) Document.parse(
				"{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}")));
	}

	@Test // DATAMONGO-774
	public void shouldRenderFieldReference() {

		assertThat(transform("foo"), is((Object) "$foo"));
		assertThat(transform("$foo"), is((Object) "$foo"));
	}

	@Test // DATAMONGO-774
	public void shouldRenderNestedFieldReference() {

		assertThat(transform("foo.bar"), is((Object) "$foo.bar"));
		assertThat(transform("$foo.bar"), is((Object) "$foo.bar"));
	}

	@Test // DATAMONGO-774
	@Ignore
	public void shouldRenderNestedIndexedFieldReference() {

		// TODO add support for rendering nested indexed field references
		assertThat(transform("foo[3].bar"), is((Object) "$foo[3].bar"));
	}

	@Test // DATAMONGO-774
	public void shouldRenderConsecutiveOperation() {
		assertThat(transform("1 + 1 + 1"), is((Object) Document.parse("{ \"$add\" : [ 1 , 1 , 1]}")));
	}

	@Test // DATAMONGO-774
	public void shouldRenderComplexExpression0() {

		assertThat(transform("-(1 + q)"),
				is((Object) Document.parse("{ \"$multiply\" : [ -1 , { \"$add\" : [ 1 , \"$q\"]}]}")));
	}

	@Test // DATAMONGO-774
	public void shouldRenderComplexExpression1() {

		assertThat(transform("1 + (q + 1) / (q - 1)"), is((Object) Document.parse(
				"{ \"$add\" : [ 1 , { \"$divide\" : [ { \"$add\" : [ \"$q\" , 1]} , { \"$subtract\" : [ \"$q\" , 1]}]}]}")));
	}

	@Test // DATAMONGO-774
	public void shouldRenderComplexExpression2() {

		assertThat(transform("(q + 1 + 4 - 5) / (q + 1 + 3 + 4)"), is((Object) Document.parse(
				"{ \"$divide\" : [ { \"$subtract\" : [ { \"$add\" : [ \"$q\" , 1 , 4]} , 5]} , { \"$add\" : [ \"$q\" , 1 , 3 , 4]}]}")));
	}

	@Test // DATAMONGO-774
	public void shouldRenderBinaryExpressionWithMixedSignsCorrectly() {

		assertThat(transform("-4 + 1"), is((Object) Document.parse("{ \"$add\" : [ -4 , 1]}")));
		assertThat(transform("1 + -4"), is((Object) Document.parse("{ \"$add\" : [ 1 , -4]}")));
	}

	@Test // DATAMONGO-774
	public void shouldRenderConsecutiveOperationsInComplexExpression() {

		assertThat(transform("1 + 1 + (1 + 1 + 1) / q"), is(
				(Object) Document.parse("{ \"$add\" : [ 1 , 1 , { \"$divide\" : [ { \"$add\" : [ 1 , 1 , 1]} , \"$q\"]}]}")));
	}

	@Test // DATAMONGO-774
	public void shouldRenderParameterExpressionResults() {
		assertThat(transform("[0] + [1] + [2]", 1, 2, 3), is((Object) Document.parse("{ \"$add\" : [ 1 , 2 , 3]}")));
	}

	@Test // DATAMONGO-774
	@Ignore("TODO: mongo3 renders this a bit strange")
	public void shouldRenderNestedParameterExpressionResults() {

		assertThat(
				((Document) transform("[0].primitiveLongValue + [0].primitiveDoubleValue + [0].doubleValue.longValue()", data))
						.toJson(),
				is(Document.parse("{ \"$add\" : [ 42 , 1.2345 , 23]}").toJson()));
	}

	@Test // DATAMONGO-774
	@Ignore("TODO: mongo3 renders this a bit strange")
	public void shouldRenderNestedParameterExpressionResultsInNestedExpressions() {

		assertThat(
				((Document) transform("((1 + [0].primitiveLongValue) + [0].primitiveDoubleValue) * [0].doubleValue.longValue()",
						data)).toJson(),
				is(new Document("$multiply", Arrays.asList(new Document("$add", Arrays.asList(1, 42L, 1.2345D, 23L))))
						.toJson()));
	}

	@Test // DATAMONGO-840
	public void shouldRenderCompoundExpressionsWithIndexerAndFieldReference() {

		Person person = new Person();
		person.setAge(10);
		assertThat(transform("[0].age + a.c", person), is((Object) Document.parse("{ \"$add\" : [ 10 , \"$a.c\"] }")));
	}

	@Test // DATAMONGO-840
	public void shouldRenderCompoundExpressionsWithOnlyFieldReferences() {

		assertThat(transform("a.b + a.c"), is((Object) Document.parse("{ \"$add\" : [ \"$a.b\" , \"$a.c\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeAnd() {
		assertThat(transform("and(a, b)"), is((Object) Document.parse("{ \"$and\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeOr() {
		assertThat(transform("or(a, b)"), is((Object) Document.parse("{ \"$or\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeNot() {
		assertThat(transform("not(a)"), is((Object) Document.parse("{ \"$not\" : [ \"$a\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeSetEquals() {
		assertThat(transform("setEquals(a, b)"), is((Object) Document.parse("{ \"$setEquals\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeSetEqualsForArrays() {
		assertThat(transform("setEquals(new int[]{1,2,3}, new int[]{4,5,6})"),
				is((Object) Document.parse("{ \"$setEquals\" : [ [ 1 , 2 , 3] , [ 4 , 5 , 6]]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeSetEqualsMixedArrays() {
		assertThat(transform("setEquals(a, new int[]{4,5,6})"),
				is((Object) Document.parse("{ \"$setEquals\" : [ \"$a\" , [ 4 , 5 , 6]]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceSetIntersection() {
		assertThat(transform("setIntersection(a, new int[]{4,5,6})"),
				is((Object) Document.parse("{ \"$setIntersection\" : [ \"$a\" , [ 4 , 5 , 6]]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceSetUnion() {
		assertThat(transform("setUnion(a, new int[]{4,5,6})"),
				is((Object) Document.parse("{ \"$setUnion\" : [ \"$a\" , [ 4 , 5 , 6]]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceSeDifference() {
		assertThat(transform("setDifference(a, new int[]{4,5,6})"),
				is((Object) Document.parse("{ \"$setDifference\" : [ \"$a\" , [ 4 , 5 , 6]]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceSetIsSubset() {
		assertThat(transform("setIsSubset(a, new int[]{4,5,6})"),
				is((Object) Document.parse("{ \"$setIsSubset\" : [ \"$a\" , [ 4 , 5 , 6]]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceAnyElementTrue() {
		assertThat(transform("anyElementTrue(a)"), is((Object) Document.parse("{ \"$anyElementTrue\" : [ \"$a\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceAllElementsTrue() {
		assertThat(transform("allElementsTrue(a, new int[]{4,5,6})"),
				is((Object) Document.parse("{ \"$allElementsTrue\" : [ \"$a\" , [ 4 , 5 , 6]]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceCmp() {
		assertThat(transform("cmp(a, 250)"), is((Object) Document.parse("{ \"$cmp\" : [ \"$a\" , 250]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceEq() {
		assertThat(transform("eq(a, 250)"), is((Object) Document.parse("{ \"$eq\" : [ \"$a\" , 250]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceGt() {
		assertThat(transform("gt(a, 250)"), is((Object) Document.parse("{ \"$gt\" : [ \"$a\" , 250]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceGte() {
		assertThat(transform("gte(a, 250)"), is((Object) Document.parse("{ \"$gte\" : [ \"$a\" , 250]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceLt() {
		assertThat(transform("lt(a, 250)"), is((Object) Document.parse("{ \"$lt\" : [ \"$a\" , 250]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceLte() {
		assertThat(transform("lte(a, 250)"), is((Object) Document.parse("{ \"$lte\" : [ \"$a\" , 250]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNe() {
		assertThat(transform("ne(a, 250)"), is((Object) Document.parse("{ \"$ne\" : [ \"$a\" , 250]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceAbs() {
		assertThat(transform("abs(1)"), is((Object) Document.parse("{ \"$abs\" : 1}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceAdd() {
		assertThat(transform("add(a, 250)"), is((Object) Document.parse("{ \"$add\" : [ \"$a\" , 250]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceCeil() {
		assertThat(transform("ceil(7.8)"), is((Object) Document.parse("{ \"$ceil\" : 7.8}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceDivide() {
		assertThat(transform("divide(a, 250)"), is((Object) Document.parse("{ \"$divide\" : [ \"$a\" , 250]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceExp() {
		assertThat(transform("exp(2)"), is((Object) Document.parse("{ \"$exp\" : 2}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceFloor() {
		assertThat(transform("floor(2)"), is((Object) Document.parse("{ \"$floor\" : 2}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceLn() {
		assertThat(transform("ln(2)"), is((Object) Document.parse("{ \"$ln\" : 2}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceLog() {
		assertThat(transform("log(100, 10)"), is((Object) Document.parse("{ \"$log\" : [ 100 , 10]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceLog10() {
		assertThat(transform("log10(100)"), is((Object) Document.parse("{ \"$log10\" : 100}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeMod() {
		assertThat(transform("mod(a, b)"), is((Object) Document.parse("{ \"$mod\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeMultiply() {
		assertThat(transform("multiply(a, b)"), is((Object) Document.parse("{ \"$multiply\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodePow() {
		assertThat(transform("pow(a, 2)"), is((Object) Document.parse("{ \"$pow\" : [ \"$a\" , 2]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceSqrt() {
		assertThat(transform("sqrt(2)"), is((Object) Document.parse("{ \"$sqrt\" : 2}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeSubtract() {
		assertThat(transform("subtract(a, b)"), is((Object) Document.parse("{ \"$subtract\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceTrunc() {
		assertThat(transform("trunc(2.1)"), is((Object) Document.parse("{ \"$trunc\" : 2.1}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeConcat() {
		assertThat(transform("concat(a, b, 'c')"),
				is((Object) Document.parse("{ \"$concat\" : [ \"$a\" , \"$b\" , \"c\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeSubstrc() {
		assertThat(transform("substr(a, 0, 1)"), is((Object) Document.parse("{ \"$substr\" : [ \"$a\" , 0 , 1]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceToLower() {
		assertThat(transform("toLower(a)"), is((Object) Document.parse("{ \"$toLower\" : \"$a\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceToUpper() {
		assertThat(transform("toUpper(a)"), is((Object) Document.parse("{ \"$toUpper\" : \"$a\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeStrCaseCmp() {
		assertThat(transform("strcasecmp(a, b)"), is((Object) Document.parse("{ \"$strcasecmp\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceMeta() {
		assertThat(transform("meta('textScore')"), is((Object) Document.parse("{ \"$meta\" : \"textScore\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeArrayElemAt() {
		assertThat(transform("arrayElemAt(a, 10)"), is((Object) Document.parse("{ \"$arrayElemAt\" : [ \"$a\" , 10]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeConcatArrays() {
		assertThat(transform("concatArrays(a, b, c)"),
				is((Object) Document.parse("{ \"$concatArrays\" : [ \"$a\" , \"$b\" , \"$c\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeFilter() {
		assertThat(transform("filter(a, 'num', '$$num' > 10)"), is((Object) Document.parse(
				"{ \"$filter\" : { \"input\" : \"$a\" , \"as\" : \"num\" , \"cond\" : { \"$gt\" : [ \"$$num\" , 10]}}}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceIsArray() {
		assertThat(transform("isArray(a)"), is((Object) Document.parse("{ \"$isArray\" : \"$a\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceIsSize() {
		assertThat(transform("size(a)"), is((Object) Document.parse("{ \"$size\" : \"$a\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeSlice() {
		assertThat(transform("slice(a, 10)"), is((Object) Document.parse("{ \"$slice\" : [ \"$a\" , 10]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeMap() {
		assertThat(transform("map(quizzes, 'grade', '$$grade' + 2)"), is((Object) Document.parse(
				"{ \"$map\" : { \"input\" : \"$quizzes\" , \"as\" : \"grade\" , \"in\" : { \"$add\" : [ \"$$grade\" , 2]}}}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeLet() {
		assertThat(transform("let({low:1, high:'$$low'}, gt('$$low', '$$high'))"), is((Object) Document.parse(
				"{ \"$let\" : { \"vars\" : { \"low\" : 1 , \"high\" : \"$$low\"} , \"in\" : { \"$gt\" : [ \"$$low\" , \"$$high\"]}}}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceLiteral() {
		assertThat(transform("literal($1)"), is((Object) Document.parse("{ \"$literal\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceDayOfYear() {
		assertThat(transform("dayOfYear($1)"), is((Object) Document.parse("{ \"$dayOfYear\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceDayOfMonth() {
		assertThat(transform("dayOfMonth($1)"), is((Object) Document.parse("{ \"$dayOfMonth\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceDayOfWeek() {
		assertThat(transform("dayOfWeek($1)"), is((Object) Document.parse("{ \"$dayOfWeek\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceYear() {
		assertThat(transform("year($1)"), is((Object) Document.parse("{ \"$year\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceMonth() {
		assertThat(transform("month($1)"), is((Object) Document.parse("{ \"$month\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceWeek() {
		assertThat(transform("week($1)"), is((Object) Document.parse("{ \"$week\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceHour() {
		assertThat(transform("hour($1)"), is((Object) Document.parse("{ \"$hour\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceMinute() {
		assertThat(transform("minute($1)"), is((Object) Document.parse("{ \"$minute\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceSecond() {
		assertThat(transform("second($1)"), is((Object) Document.parse("{ \"$second\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceMillisecond() {
		assertThat(transform("millisecond($1)"), is((Object) Document.parse("{ \"$millisecond\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceDateToString() {
		assertThat(transform("dateToString('%Y-%m-%d', $date)"),
				is((Object) Document.parse("{ \"$dateToString\" : { \"format\" : \"%Y-%m-%d\" , \"date\" : \"$date\"}}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceCond() {
		assertThat(transform("cond(qty > 250, 30, 20)"), is((Object) Document
				.parse("{ \"$cond\" : { \"if\" : { \"$gt\" : [ \"$qty\" , 250]} , \"then\" : 30 , \"else\" : 20}}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeIfNull() {
		assertThat(transform("ifNull(a, 10)"), is((Object) Document.parse("{ \"$ifNull\" : [ \"$a\" , 10]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeSum() {
		assertThat(transform("sum(a, b)"), is((Object) Document.parse("{ \"$sum\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeAvg() {
		assertThat(transform("avg(a, b)"), is((Object) Document.parse("{ \"$avg\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceFirst() {
		assertThat(transform("first($1)"), is((Object) Document.parse("{ \"$first\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceLast() {
		assertThat(transform("last($1)"), is((Object) Document.parse("{ \"$last\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeMax() {
		assertThat(transform("max(a, b)"), is((Object) Document.parse("{ \"$max\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeMin() {
		assertThat(transform("min(a, b)"), is((Object) Document.parse("{ \"$min\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodePush() {
		assertThat(transform("push({'item':'$item', 'quantity':'$qty'})"),
				is((Object) Document.parse("{ \"$push\" : { \"item\" : \"$item\" , \"quantity\" : \"$qty\"}}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceAddToSet() {
		assertThat(transform("addToSet($1)"), is((Object) Document.parse("{ \"$addToSet\" : \"$1\"}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeStdDevPop() {
		assertThat(transform("stdDevPop(scores.score)"),
				is((Object) Document.parse("{ \"$stdDevPop\" : [ \"$scores.score\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderMethodReferenceNodeStdDevSamp() {
		assertThat(transform("stdDevSamp(age)"), is((Object) Document.parse("{ \"$stdDevSamp\" : [ \"$age\"]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderOperationNodeEq() {
		assertThat(transform("foo == 10"), is((Object) Document.parse("{ \"$eq\" : [ \"$foo\" , 10]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderOperationNodeNe() {
		assertThat(transform("foo != 10"), is((Object) Document.parse("{ \"$ne\" : [ \"$foo\" , 10]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderOperationNodeGt() {
		assertThat(transform("foo > 10"), is((Object) Document.parse("{ \"$gt\" : [ \"$foo\" , 10]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderOperationNodeGte() {
		assertThat(transform("foo >= 10"), is((Object) Document.parse("{ \"$gte\" : [ \"$foo\" , 10]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderOperationNodeLt() {
		assertThat(transform("foo < 10"), is((Object) Document.parse("{ \"$lt\" : [ \"$foo\" , 10]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderOperationNodeLte() {
		assertThat(transform("foo <= 10"), is((Object) Document.parse("{ \"$lte\" : [ \"$foo\" , 10]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderOperationNodePow() {
		assertThat(transform("foo^2"), is((Object) Document.parse("{ \"$pow\" : [ \"$foo\" , 2]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderOperationNodeOr() {
		assertThat(transform("true || false"), is((Object) Document.parse("{ \"$or\" : [ true , false]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderComplexOperationNodeOr() {
		assertThat(transform("1+2 || concat(a, b) || true"), is((Object) Document
				.parse("{ \"$or\" : [ { \"$add\" : [ 1 , 2]} , { \"$concat\" : [ \"$a\" , \"$b\"]} , true]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderOperationNodeAnd() {
		assertThat(transform("true && false"), is((Object) Document.parse("{ \"$and\" : [ true , false]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderComplexOperationNodeAnd() {
		assertThat(transform("1+2 && concat(a, b) && true"), is((Object) Document
				.parse("{ \"$and\" : [ { \"$add\" : [ 1 , 2]} , { \"$concat\" : [ \"$a\" , \"$b\"]} , true]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderNotCorrectly() {
		assertThat(transform("!true"), is((Object) Document.parse("{ \"$not\" : [ true]}")));
	}

	@Test // DATAMONGO-1530
	public void shouldRenderComplexNotCorrectly() {
		assertThat(transform("!(foo > 10)"), is((Object) Document.parse("{ \"$not\" : [ { \"$gt\" : [ \"$foo\" , 10]}]}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodReferenceIndexOfBytes() {
		assertThat(transform("indexOfBytes(item, 'foo')"),
				is(Document.parse("{ \"$indexOfBytes\" : [ \"$item\" , \"foo\"]}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodReferenceIndexOfCP() {
		assertThat(transform("indexOfCP(item, 'foo')"), is(Document.parse("{ \"$indexOfCP\" : [ \"$item\" , \"foo\"]}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodReferenceSplit() {
		assertThat(transform("split(item, ',')"), is(Document.parse("{ \"$split\" : [ \"$item\" , \",\"]}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodReferenceStrLenBytes() {
		assertThat(transform("strLenBytes(item)"), is(Document.parse("{ \"$strLenBytes\" : \"$item\"}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodReferenceStrLenCP() {
		assertThat(transform("strLenCP(item)"), is(Document.parse("{ \"$strLenCP\" : \"$item\"}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodSubstrCP() {
		assertThat(transform("substrCP(item, 0, 5)"), is(Document.parse("{ \"$substrCP\" : [ \"$item\" , 0 , 5]}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodReferenceReverseArray() {
		assertThat(transform("reverseArray(array)"), is(Document.parse("{ \"$reverseArray\" : \"$array\"}")));
	}

	@Test // DATAMONGO-1548
	@Ignore("Document API cannot render String[]")
	public void shouldRenderMethodReferenceReduce() {
		assertThat(transform("reduce(field, '', {'$concat':new String[]{'$$value','$$this'}})"), is(Document.parse(
				"{ \"$reduce\" : { \"input\" : \"$field\" , \"initialValue\" : \"\" , \"in\" : { \"$concat\" : [ \"$$value\" , \"$$this\"]}}}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodReferenceZip() {
		assertThat(transform("zip(new String[]{'$array1', '$array2'})"),
				is(Document.parse("{ \"$zip\" : { \"inputs\" : [ \"$array1\" , \"$array2\"]}}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodReferenceZipWithOptionalArgs() {
		assertThat(transform("zip(new String[]{'$array1', '$array2'}, true, new int[]{1,2})"), is(Document.parse(
				"{ \"$zip\" : { \"inputs\" : [ \"$array1\" , \"$array2\"] , \"useLongestLength\" : true , \"defaults\" : [ 1 , 2]}}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodIn() {
		assertThat(transform("in('item', array)"), is(Document.parse("{ \"$in\" : [ \"item\" , \"$array\"]}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodRefereneIsoDayOfWeek() {
		assertThat(transform("isoDayOfWeek(date)"), is(Document.parse("{ \"$isoDayOfWeek\" : \"$date\"}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodRefereneIsoWeek() {
		assertThat(transform("isoWeek(date)"), is(Document.parse("{ \"$isoWeek\" : \"$date\"}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodRefereneIsoWeekYear() {
		assertThat(transform("isoWeekYear(date)"), is(Document.parse("{ \"$isoWeekYear\" : \"$date\"}")));
	}

	@Test // DATAMONGO-1548
	public void shouldRenderMethodRefereneType() {
		assertThat(transform("type(a)"), is(Document.parse("{ \"$type\" : \"$a\"}")));
	}

	private Object transform(String expression, Object... params) {
		Object result = transformer.transform(expression, Aggregation.DEFAULT_CONTEXT, params);
		return result == null ? null : (!(result instanceof org.bson.Document) ? result.toString() : result);
	}
}
