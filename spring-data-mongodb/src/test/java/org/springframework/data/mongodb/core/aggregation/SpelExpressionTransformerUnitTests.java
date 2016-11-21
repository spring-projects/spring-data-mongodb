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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.mongodb.core.Person;

/**
 * Unit tests for {@link SpelExpressionTransformer}.
 * 
 * @see DATAMONGO-774
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

	@Test
	public void shouldRenderConstantExpression() {

		assertThat(transform("1"), is("1"));
		assertThat(transform("-1"), is("-1"));
		assertThat(transform("1.0"), is("1.0"));
		assertThat(transform("-1.0"), is("-1.0"));
		assertThat(transform("null"), is(nullValue()));
	}

	@Test
	public void shouldSupportKnownOperands() {

		assertThat(transform("a + b"), is("{ \"$add\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transform("a - b"), is("{ \"$subtract\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transform("a * b"), is("{ \"$multiply\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transform("a / b"), is("{ \"$divide\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transform("a % b"), is("{ \"$mod\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionOnUnknownOperand() {
		transform("a++");
	}

	@Test
	public void shouldRenderSumExpression() {
		assertThat(transform("a + 1"), is("{ \"$add\" : [ \"$a\" , 1]}"));
	}

	@Test
	public void shouldRenderFormula() {

		assertThat(transform("(netPrice + surCharge) * taxrate + 42"), is(
				"{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}"));
	}

	@Test
	public void shouldRenderFormulaInCurlyBrackets() {

		assertThat(transform("{(netPrice + surCharge) * taxrate + 42}"), is(
				"{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}"));
	}

	@Test
	public void shouldRenderFieldReference() {

		assertThat(transform("foo"), is("$foo"));
		assertThat(transform("$foo"), is("$foo"));
	}

	@Test
	public void shouldRenderNestedFieldReference() {

		assertThat(transform("foo.bar"), is("$foo.bar"));
		assertThat(transform("$foo.bar"), is("$foo.bar"));
	}

	@Test
	@Ignore
	public void shouldRenderNestedIndexedFieldReference() {

		// TODO add support for rendering nested indexed field references
		assertThat(transform("foo[3].bar"), is("$foo[3].bar"));
	}

	@Test
	public void shouldRenderConsecutiveOperation() {
		assertThat(transform("1 + 1 + 1"), is("{ \"$add\" : [ 1 , 1 , 1]}"));
	}

	@Test
	public void shouldRenderComplexExpression0() {

		assertThat(transform("-(1 + q)"), is("{ \"$multiply\" : [ -1 , { \"$add\" : [ 1 , \"$q\"]}]}"));
	}

	@Test
	public void shouldRenderComplexExpression1() {

		assertThat(transform("1 + (q + 1) / (q - 1)"),
				is("{ \"$add\" : [ 1 , { \"$divide\" : [ { \"$add\" : [ \"$q\" , 1]} , { \"$subtract\" : [ \"$q\" , 1]}]}]}"));
	}

	@Test
	public void shouldRenderComplexExpression2() {

		assertThat(transform("(q + 1 + 4 - 5) / (q + 1 + 3 + 4)"), is(
				"{ \"$divide\" : [ { \"$subtract\" : [ { \"$add\" : [ \"$q\" , 1 , 4]} , 5]} , { \"$add\" : [ \"$q\" , 1 , 3 , 4]}]}"));
	}

	@Test
	public void shouldRenderBinaryExpressionWithMixedSignsCorrectly() {

		assertThat(transform("-4 + 1"), is("{ \"$add\" : [ -4 , 1]}"));
		assertThat(transform("1 + -4"), is("{ \"$add\" : [ 1 , -4]}"));
	}

	@Test
	public void shouldRenderConsecutiveOperationsInComplexExpression() {

		assertThat(transform("1 + 1 + (1 + 1 + 1) / q"),
				is("{ \"$add\" : [ 1 , 1 , { \"$divide\" : [ { \"$add\" : [ 1 , 1 , 1]} , \"$q\"]}]}"));
	}

	@Test
	public void shouldRenderParameterExpressionResults() {
		assertThat(transform("[0] + [1] + [2]", 1, 2, 3), is("{ \"$add\" : [ 1 , 2 , 3]}"));
	}

	@Test
	public void shouldRenderNestedParameterExpressionResults() {

		assertThat(transform("[0].primitiveLongValue + [0].primitiveDoubleValue + [0].doubleValue.longValue()", data),
				is("{ \"$add\" : [ 42 , 1.2345 , 23]}"));
	}

	@Test
	public void shouldRenderNestedParameterExpressionResultsInNestedExpressions() {

		assertThat(
				transform("((1 + [0].primitiveLongValue) + [0].primitiveDoubleValue) * [0].doubleValue.longValue()", data),
				is("{ \"$multiply\" : [ { \"$add\" : [ 1 , 42 , 1.2345]} , 23]}"));
	}

	/**
	 * @see DATAMONGO-840
	 */
	@Test
	public void shouldRenderCompoundExpressionsWithIndexerAndFieldReference() {

		Person person = new Person();
		person.setAge(10);
		assertThat(transform("[0].age + a.c", person), is("{ \"$add\" : [ 10 , \"$a.c\"]}"));
	}

	/**
	 * @see DATAMONGO-840
	 */
	@Test
	public void shouldRenderCompoundExpressionsWithOnlyFieldReferences() {

		assertThat(transform("a.b + a.c"), is("{ \"$add\" : [ \"$a.b\" , \"$a.c\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeAnd() {
		assertThat(transform("and(a, b)"), is("{ \"$and\" : [ \"$a\" , \"$b\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeOr() {
		assertThat(transform("or(a, b)"), is("{ \"$or\" : [ \"$a\" , \"$b\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeNot() {
		assertThat(transform("not(a)"), is("{ \"$not\" : [ \"$a\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeSetEquals() {
		assertThat(transform("setEquals(a, b)"), is("{ \"$setEquals\" : [ \"$a\" , \"$b\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeSetEqualsForArrays() {
		assertThat(transform("setEquals(new int[]{1,2,3}, new int[]{4,5,6})"),
				is("{ \"$setEquals\" : [ [ 1 , 2 , 3] , [ 4 , 5 , 6]]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeSetEqualsMixedArrays() {
		assertThat(transform("setEquals(a, new int[]{4,5,6})"), is("{ \"$setEquals\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceSetIntersection() {
		assertThat(transform("setIntersection(a, new int[]{4,5,6})"),
				is("{ \"$setIntersection\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceSetUnion() {
		assertThat(transform("setUnion(a, new int[]{4,5,6})"), is("{ \"$setUnion\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceSeDifference() {
		assertThat(transform("setDifference(a, new int[]{4,5,6})"), is("{ \"$setDifference\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceSetIsSubset() {
		assertThat(transform("setIsSubset(a, new int[]{4,5,6})"), is("{ \"$setIsSubset\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceAnyElementTrue() {
		assertThat(transform("anyElementTrue(a)"), is("{ \"$anyElementTrue\" : [ \"$a\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceAllElementsTrue() {
		assertThat(transform("allElementsTrue(a, new int[]{4,5,6})"),
				is("{ \"$allElementsTrue\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceCmp() {
		assertThat(transform("cmp(a, 250)"), is("{ \"$cmp\" : [ \"$a\" , 250]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceEq() {
		assertThat(transform("eq(a, 250)"), is("{ \"$eq\" : [ \"$a\" , 250]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceGt() {
		assertThat(transform("gt(a, 250)"), is("{ \"$gt\" : [ \"$a\" , 250]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceGte() {
		assertThat(transform("gte(a, 250)"), is("{ \"$gte\" : [ \"$a\" , 250]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceLt() {
		assertThat(transform("lt(a, 250)"), is("{ \"$lt\" : [ \"$a\" , 250]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceLte() {
		assertThat(transform("lte(a, 250)"), is("{ \"$lte\" : [ \"$a\" , 250]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNe() {
		assertThat(transform("ne(a, 250)"), is("{ \"$ne\" : [ \"$a\" , 250]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceAbs() {
		assertThat(transform("abs(1)"), is("{ \"$abs\" : 1}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceAdd() {
		assertThat(transform("add(a, 250)"), is("{ \"$add\" : [ \"$a\" , 250]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceCeil() {
		assertThat(transform("ceil(7.8)"), is("{ \"$ceil\" : 7.8}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceDivide() {
		assertThat(transform("divide(a, 250)"), is("{ \"$divide\" : [ \"$a\" , 250]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceExp() {
		assertThat(transform("exp(2)"), is("{ \"$exp\" : 2}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceFloor() {
		assertThat(transform("floor(2)"), is("{ \"$floor\" : 2}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceLn() {
		assertThat(transform("ln(2)"), is("{ \"$ln\" : 2}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceLog() {
		assertThat(transform("log(100, 10)"), is("{ \"$log\" : [ 100 , 10]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceLog10() {
		assertThat(transform("log10(100)"), is("{ \"$log10\" : 100}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeMod() {
		assertThat(transform("mod(a, b)"), is("{ \"$mod\" : [ \"$a\" , \"$b\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeMultiply() {
		assertThat(transform("multiply(a, b)"), is("{ \"$multiply\" : [ \"$a\" , \"$b\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodePow() {
		assertThat(transform("pow(a, 2)"), is("{ \"$pow\" : [ \"$a\" , 2]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceSqrt() {
		assertThat(transform("sqrt(2)"), is("{ \"$sqrt\" : 2}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeSubtract() {
		assertThat(transform("subtract(a, b)"), is("{ \"$subtract\" : [ \"$a\" , \"$b\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceTrunc() {
		assertThat(transform("trunc(2.1)"), is("{ \"$trunc\" : 2.1}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeConcat() {
		assertThat(transform("concat(a, b, 'c')"), is("{ \"$concat\" : [ \"$a\" , \"$b\" , \"c\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeSubstrc() {
		assertThat(transform("substr(a, 0, 1)"), is("{ \"$substr\" : [ \"$a\" , 0 , 1]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceToLower() {
		assertThat(transform("toLower(a)"), is("{ \"$toLower\" : \"$a\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceToUpper() {
		assertThat(transform("toUpper(a)"), is("{ \"$toUpper\" : \"$a\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeStrCaseCmp() {
		assertThat(transform("strcasecmp(a, b)"), is("{ \"$strcasecmp\" : [ \"$a\" , \"$b\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceMeta() {
		assertThat(transform("meta('textScore')"), is("{ \"$meta\" : \"textScore\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeArrayElemAt() {
		assertThat(transform("arrayElemAt(a, 10)"), is("{ \"$arrayElemAt\" : [ \"$a\" , 10]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeConcatArrays() {
		assertThat(transform("concatArrays(a, b, c)"), is("{ \"$concatArrays\" : [ \"$a\" , \"$b\" , \"$c\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeFilter() {
		assertThat(transform("filter(a, 'num', '$$num' > 10)"),
				is("{ \"$filter\" : { \"input\" : \"$a\" , \"as\" : \"num\" , \"cond\" : { \"$gt\" : [ \"$$num\" , 10]}}}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceIsArray() {
		assertThat(transform("isArray(a)"), is("{ \"$isArray\" : \"$a\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceIsSize() {
		assertThat(transform("size(a)"), is("{ \"$size\" : \"$a\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeSlice() {
		assertThat(transform("slice(a, 10)"), is("{ \"$slice\" : [ \"$a\" , 10]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeMap() {
		assertThat(transform("map(quizzes, 'grade', '$$grade' + 2)"), is(
				"{ \"$map\" : { \"input\" : \"$quizzes\" , \"as\" : \"grade\" , \"in\" : { \"$add\" : [ \"$$grade\" , 2]}}}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeLet() {
		assertThat(transform("let({low:1, high:'$$low'}, gt('$$low', '$$high'))"), is(
				"{ \"$let\" : { \"vars\" : { \"low\" : 1 , \"high\" : \"$$low\"} , \"in\" : { \"$gt\" : [ \"$$low\" , \"$$high\"]}}}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceLiteral() {
		assertThat(transform("literal($1)"), is("{ \"$literal\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceDayOfYear() {
		assertThat(transform("dayOfYear($1)"), is("{ \"$dayOfYear\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceDayOfMonth() {
		assertThat(transform("dayOfMonth($1)"), is("{ \"$dayOfMonth\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceDayOfWeek() {
		assertThat(transform("dayOfWeek($1)"), is("{ \"$dayOfWeek\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceYear() {
		assertThat(transform("year($1)"), is("{ \"$year\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceMonth() {
		assertThat(transform("month($1)"), is("{ \"$month\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceWeek() {
		assertThat(transform("week($1)"), is("{ \"$week\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceHour() {
		assertThat(transform("hour($1)"), is("{ \"$hour\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceMinute() {
		assertThat(transform("minute($1)"), is("{ \"$minute\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceSecond() {
		assertThat(transform("second($1)"), is("{ \"$second\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceMillisecond() {
		assertThat(transform("millisecond($1)"), is("{ \"$millisecond\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceDateToString() {
		assertThat(transform("dateToString('%Y-%m-%d', $date)"),
				is("{ \"$dateToString\" : { \"format\" : \"%Y-%m-%d\" , \"date\" : \"$date\"}}"));
	}


	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceCond() {
		assertThat(transform("cond(qty > 250, 30, 20)"),
				is("{ \"$cond\" : { \"if\" : { \"$gt\" : [ \"$qty\" , 250]} , \"then\" : 30 , \"else\" : 20}}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeIfNull() {
		assertThat(transform("ifNull(a, 10)"), is("{ \"$ifNull\" : [ \"$a\" , 10]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeSum() {
		assertThat(transform("sum(a, b)"), is("{ \"$sum\" : [ \"$a\" , \"$b\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeAvg() {
		assertThat(transform("avg(a, b)"), is("{ \"$avg\" : [ \"$a\" , \"$b\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceFirst() {
		assertThat(transform("first($1)"), is("{ \"$first\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceLast() {
		assertThat(transform("last($1)"), is("{ \"$last\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeMax() {
		assertThat(transform("max(a, b)"), is("{ \"$max\" : [ \"$a\" , \"$b\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeMin() {
		assertThat(transform("min(a, b)"), is("{ \"$min\" : [ \"$a\" , \"$b\"]}"));
	}


	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodePush() {
		assertThat(transform("push({'item':'$item', 'quantity':'$qty'})"), is("{ \"$push\" : { \"item\" : \"$item\" , \"quantity\" : \"$qty\"}}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceAddToSet() {
		assertThat(transform("addToSet($1)"), is("{ \"$addToSet\" : \"$1\"}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeStdDevPop() {
		assertThat(transform("stdDevPop(scores.score)"), is("{ \"$stdDevPop\" : [ \"$scores.score\"]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderMethodReferenceNodeStdDevSamp() {
		assertThat(transform("stdDevSamp(age)"), is("{ \"$stdDevSamp\" : [ \"$age\"]}"));
	}


	/**
		 * @see DATAMONGO-1530
		 */
	@Test
	public void shouldRenderOperationNodeEq() {
		assertThat(transform("foo == 10"), is("{ \"$eq\" : [ \"$foo\" , 10]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderOperationNodeNe() {
		assertThat(transform("foo != 10"), is("{ \"$ne\" : [ \"$foo\" , 10]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderOperationNodeGt() {
		assertThat(transform("foo > 10"), is("{ \"$gt\" : [ \"$foo\" , 10]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderOperationNodeGte() {
		assertThat(transform("foo >= 10"), is("{ \"$gte\" : [ \"$foo\" , 10]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderOperationNodeLt() {
		assertThat(transform("foo < 10"), is("{ \"$lt\" : [ \"$foo\" , 10]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderOperationNodeLte() {
		assertThat(transform("foo <= 10"), is("{ \"$lte\" : [ \"$foo\" , 10]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderOperationNodePow() {
		assertThat(transform("foo^2"), is("{ \"$pow\" : [ \"$foo\" , 2]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderOperationNodeOr() {
		assertThat(transform("true || false"), is("{ \"$or\" : [ true , false]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderComplexOperationNodeOr() {
		assertThat(transform("1+2 || concat(a, b) || true"),
				is("{ \"$or\" : [ { \"$add\" : [ 1 , 2]} , { \"$concat\" : [ \"$a\" , \"$b\"]} , true]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderOperationNodeAnd() {
		assertThat(transform("true && false"), is("{ \"$and\" : [ true , false]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderComplexOperationNodeAnd() {
		assertThat(transform("1+2 && concat(a, b) && true"),
				is("{ \"$and\" : [ { \"$add\" : [ 1 , 2]} , { \"$concat\" : [ \"$a\" , \"$b\"]} , true]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderNotCorrectly() {
		assertThat(transform("!true"), is("{ \"$not\" : [ true]}"));
	}

	/**
	 * @see DATAMONGO-1530
	 */
	@Test
	public void shouldRenderComplexNotCorrectly() {
		assertThat(transform("!(foo > 10)"), is("{ \"$not\" : [ { \"$gt\" : [ \"$foo\" , 10]}]}"));
	}

	private String transform(String expression, Object... params) {
		Object result = transformer.transform(expression, Aggregation.DEFAULT_CONTEXT, params);
		return result == null ? null : result.toString();
	}
}
