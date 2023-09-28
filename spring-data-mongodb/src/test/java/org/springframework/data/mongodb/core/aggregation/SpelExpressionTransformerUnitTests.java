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

import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.Person;

/**
 * Unit tests for {@link SpelExpressionTransformer}.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Divya Srivastava
 * @author Julia Lee
 */
public class SpelExpressionTransformerUnitTests {

	private SpelExpressionTransformer transformer = new SpelExpressionTransformer();

	private Data data;

	@BeforeEach
	void beforeEach() {

		this.data = new Data();
		this.data.primitiveLongValue = 42;
		this.data.primitiveDoubleValue = 1.2345;
		this.data.doubleValue = 23.0;
		this.data.item = new DataItem();
		this.data.item.primitiveIntValue = 21;
	}

	@Test // DATAMONGO-774
	void shouldRenderConstantExpression() {

		assertThat(transformValue("1")).isEqualTo("1");
		assertThat(transformValue("-1")).isEqualTo("-1");
		assertThat(transformValue("1.0")).isEqualTo("1.0");
		assertThat(transformValue("-1.0")).isEqualTo("-1.0");
		assertThat(transformValue("null")).isNull();
	}

	@Test // DATAMONGO-774
	void shouldSupportKnownOperands() {

		assertThat(transform("a + b")).isEqualTo("{ \"$add\" : [ \"$a\" , \"$b\"]}");
		assertThat(transform("a - b")).isEqualTo("{ \"$subtract\" : [ \"$a\" , \"$b\"]}");
		assertThat(transform("a * b")).isEqualTo("{ \"$multiply\" : [ \"$a\" , \"$b\"]}");
		assertThat(transform("a / b")).isEqualTo("{ \"$divide\" : [ \"$a\" , \"$b\"]}");
		assertThat(transform("a % b")).isEqualTo("{ \"$mod\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-774
	void shouldThrowExceptionOnUnknownOperand() {
		assertThatIllegalArgumentException().isThrownBy(() -> transform("a++"));
	}

	@Test // DATAMONGO-774
	void shouldRenderSumExpression() {
		assertThat(transform("a + 1")).isEqualTo("{ \"$add\" : [ \"$a\" , 1]}");
	}

	@Test // DATAMONGO-774
	void shouldRenderFormula() {

		assertThat(transform("(netPrice + surCharge) * taxrate + 42")).isEqualTo(
				"{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}");
	}

	@Test // DATAMONGO-774
	void shouldRenderFormulaInCurlyBrackets() {

		assertThat(transform("{(netPrice + surCharge) * taxrate + 42}")).isEqualTo(
				"{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}");
	}

	@Test // DATAMONGO-774
	void shouldRenderFieldReference() {

		assertThat(transformValue("foo")).isEqualTo("$foo");
		assertThat(transformValue("$foo")).isEqualTo("$foo");
	}

	@Test // DATAMONGO-774
	void shouldRenderNestedFieldReference() {

		assertThat(transformValue("foo.bar")).isEqualTo("$foo.bar");
		assertThat(transformValue("$foo.bar")).isEqualTo("$foo.bar");
	}

	@Test // DATAMONGO-774
	@Disabled
	void shouldRenderNestedIndexedFieldReference() {

		// TODO add support for rendering nested indexed field references
		assertThat(transformValue("foo[3].bar")).isEqualTo("$foo[3].bar");
	}

	@Test // DATAMONGO-774
	void shouldRenderConsecutiveOperation() {
		assertThat(transform("1 + 1 + 1")).isEqualTo("{ \"$add\" : [ 1 , 1 , 1]}");
	}

	@Test // DATAMONGO-774
	void shouldRenderComplexExpression0() {

		assertThat(transform("-(1 + q)"))
				.isEqualTo("{ \"$multiply\" : [ -1 , { \"$add\" : [ 1 , \"$q\"]}]}");
	}

	@Test // DATAMONGO-774
	void shouldRenderComplexExpression1() {

		assertThat(transform("1 + (q + 1) / (q - 1)")).isEqualTo(
				"{ \"$add\" : [ 1 , { \"$divide\" : [ { \"$add\" : [ \"$q\" , 1]} , { \"$subtract\" : [ \"$q\" , 1]}]}]}");
	}

	@Test // DATAMONGO-774
	void shouldRenderComplexExpression2() {

		assertThat(transform("(q + 1 + 4 - 5) / (q + 1 + 3 + 4)")).isEqualTo(
				"{ \"$divide\" : [ { \"$subtract\" : [ { \"$add\" : [ \"$q\" , 1 , 4]} , 5]} , { \"$add\" : [ \"$q\" , 1 , 3 , 4]}]}");
	}

	@Test // DATAMONGO-774
	void shouldRenderBinaryExpressionWithMixedSignsCorrectly() {

		assertThat(transform("-4 + 1")).isEqualTo("{ \"$add\" : [ -4 , 1]}");
		assertThat(transform("1 + -4")).isEqualTo("{ \"$add\" : [ 1 , -4]}");
	}

	@Test // DATAMONGO-774
	void shouldRenderConsecutiveOperationsInComplexExpression() {

		assertThat(transform("1 + 1 + (1 + 1 + 1) / q"))
				.isEqualTo("{ \"$add\" : [ 1 , 1 , { \"$divide\" : [ { \"$add\" : [ 1 , 1 , 1]} , \"$q\"]}]}");
	}

	@Test // DATAMONGO-774
	void shouldRenderParameterExpressionResults() {
		assertThat(transform("[0] + [1] + [2]", 1, 2, 3)).isEqualTo("{ \"$add\" : [ 1 , 2 , 3]}");
	}

	@Test // DATAMONGO-774
	void shouldRenderNestedParameterExpressionResults() {

		assertThat(
				((Document) transform("[0].primitiveLongValue + [0].primitiveDoubleValue + [0].doubleValue.longValue()", data))
						.toJson())
								.isEqualTo(Document
										.parse("{ \"$add\" : [ { $numberLong : \"42\"} , 1.2345 , { $numberLong : \"23\" } ]}").toJson());
	}

	@Test // DATAMONGO-774
	void shouldRenderNestedParameterExpressionResultsInNestedExpressions() {

		Document target = ((Document) transform(
				"((1 + [0].primitiveLongValue) + [0].primitiveDoubleValue) * [0].doubleValue.longValue()", data));

		assertThat(
				((Document) transform("((1 + [0].primitiveLongValue) + [0].primitiveDoubleValue) * [0].doubleValue.longValue()",
						data)))
								.isEqualTo(new Document("$multiply",
										Arrays.<Object> asList(new Document("$add", Arrays.<Object> asList(1, 42L, 1.2345D)), 23L)));
	}

	@Test // DATAMONGO-840
	void shouldRenderCompoundExpressionsWithIndexerAndFieldReference() {

		Person person = new Person();
		person.setAge(10);
		assertThat(transform("[0].age + a.c", person)).isEqualTo("{ \"$add\" : [ 10 , \"$a.c\"] }");
	}

	@Test // DATAMONGO-840
	void shouldRenderCompoundExpressionsWithOnlyFieldReferences() {

		assertThat(transform("a.b + a.c")).isEqualTo("{ \"$add\" : [ \"$a.b\" , \"$a.c\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeAnd() {
		assertThat(transform("and(a, b)")).isEqualTo("{ \"$and\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeOr() {
		assertThat(transform("or(a, b)")).isEqualTo("{ \"$or\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeNot() {
		assertThat(transform("not(a)")).isEqualTo("{ \"$not\" : [ \"$a\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSetEquals() {
		assertThat(transform("setEquals(a, b)")).isEqualTo("{ \"$setEquals\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSetEqualsForArrays() {
		assertThat(transform("setEquals(new int[]{1,2,3}, new int[]{4,5,6})"))
				.isEqualTo("{ \"$setEquals\" : [ [ 1 , 2 , 3] , [ 4 , 5 , 6]]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSetEqualsMixedArrays() {
		assertThat(transform("setEquals(a, new int[]{4,5,6})"))
				.isEqualTo("{ \"$setEquals\" : [ \"$a\" , [ 4 , 5 , 6]]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSetIntersection() {
		assertThat(transform("setIntersection(a, new int[]{4,5,6})"))
				.isEqualTo("{ \"$setIntersection\" : [ \"$a\" , [ 4 , 5 , 6]]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSetUnion() {
		assertThat(transform("setUnion(a, new int[]{4,5,6})"))
				.isEqualTo("{ \"$setUnion\" : [ \"$a\" , [ 4 , 5 , 6]]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSeDifference() {
		assertThat(transform("setDifference(a, new int[]{4,5,6})"))
				.isEqualTo("{ \"$setDifference\" : [ \"$a\" , [ 4 , 5 , 6]]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSetIsSubset() {
		assertThat(transform("setIsSubset(a, new int[]{4,5,6})"))
				.isEqualTo("{ \"$setIsSubset\" : [ \"$a\" , [ 4 , 5 , 6]]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceAnyElementTrue() {
		assertThat(transform("anyElementTrue(a)")).isEqualTo("{ \"$anyElementTrue\" : [ \"$a\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceAllElementsTrue() {
		assertThat(transform("allElementsTrue(a, new int[]{4,5,6})"))
				.isEqualTo("{ \"$allElementsTrue\" : [ \"$a\" , [ 4 , 5 , 6]]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceCmp() {
		assertThat(transform("cmp(a, 250)")).isEqualTo("{ \"$cmp\" : [ \"$a\" , 250]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceEq() {
		assertThat(transform("eq(a, 250)")).isEqualTo("{ \"$eq\" : [ \"$a\" , 250]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceGt() {
		assertThat(transform("gt(a, 250)")).isEqualTo("{ \"$gt\" : [ \"$a\" , 250]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceGte() {
		assertThat(transform("gte(a, 250)")).isEqualTo("{ \"$gte\" : [ \"$a\" , 250]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLt() {
		assertThat(transform("lt(a, 250)")).isEqualTo("{ \"$lt\" : [ \"$a\" , 250]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLte() {
		assertThat(transform("lte(a, 250)")).isEqualTo("{ \"$lte\" : [ \"$a\" , 250]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNe() {
		assertThat(transform("ne(a, 250)")).isEqualTo("{ \"$ne\" : [ \"$a\" , 250]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceAbs() {
		assertThat(transform("abs(1)")).isEqualTo("{ \"$abs\" : 1}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceAdd() {
		assertThat(transform("add(a, 250)")).isEqualTo("{ \"$add\" : [ \"$a\" , 250]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceCeil() {
		assertThat(transform("ceil(7.8)")).isEqualTo("{ \"$ceil\" : 7.8}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceDivide() {
		assertThat(transform("divide(a, 250)")).isEqualTo("{ \"$divide\" : [ \"$a\" , 250]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceExp() {
		assertThat(transform("exp(2)")).isEqualTo("{ \"$exp\" : 2}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceFloor() {
		assertThat(transform("floor(2)")).isEqualTo("{ \"$floor\" : 2}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLn() {
		assertThat(transform("ln(2)")).isEqualTo("{ \"$ln\" : 2}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLog() {
		assertThat(transform("log(100, 10)")).isEqualTo("{ \"$log\" : [ 100 , 10]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLog10() {
		assertThat(transform("log10(100)")).isEqualTo("{ \"$log10\" : 100}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeMod() {
		assertThat(transform("mod(a, b)")).isEqualTo("{ \"$mod\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeMultiply() {
		assertThat(transform("multiply(a, b)")).isEqualTo("{ \"$multiply\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodePow() {
		assertThat(transform("pow(a, 2)")).isEqualTo("{ \"$pow\" : [ \"$a\" , 2]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSqrt() {
		assertThat(transform("sqrt(2)")).isEqualTo("{ \"$sqrt\" : 2}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSubtract() {
		assertThat(transform("subtract(a, b)")).isEqualTo("{ \"$subtract\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceTrunc() {
		assertThat(transform("trunc(2.1)")).isEqualTo("{ \"$trunc\" : 2.1}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeConcat() {
		assertThat(transform("concat(a, b, 'c')")).isEqualTo("{ \"$concat\" : [ \"$a\" , \"$b\" , \"c\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSubstrc() {
		assertThat(transform("substr(a, 0, 1)")).isEqualTo("{ \"$substr\" : [ \"$a\" , 0 , 1]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceToLower() {
		assertThat(transform("toLower(a)")).isEqualTo("{ \"$toLower\" : \"$a\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceToUpper() {
		assertThat(transform("toUpper(a)")).isEqualTo("{ \"$toUpper\" : \"$a\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeStrCaseCmp() {
		assertThat(transform("strcasecmp(a, b)")).isEqualTo("{ \"$strcasecmp\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceMeta() {
		assertThat(transform("meta('textScore')")).isEqualTo("{ \"$meta\" : \"textScore\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeArrayElemAt() {
		assertThat(transform("arrayElemAt(a, 10)")).isEqualTo("{ \"$arrayElemAt\" : [ \"$a\" , 10]}");
	}

	@Test // GH-3694
	void shouldRenderMethodReferenceNodeFirst() {
		assertThat(transform("first(a)")).isEqualTo("{ \"$first\" : \"$a\" }");
	}

	@Test // GH-3694
	void shouldRenderMethodReferenceNodeLast() {
		assertThat(transform("last(a)")).isEqualTo("{ \"$last\" : \"$a\" }");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeConcatArrays() {
		assertThat(transform("concatArrays(a, b, c)"))
				.isEqualTo("{ \"$concatArrays\" : [ \"$a\" , \"$b\" , \"$c\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeFilter() {
		assertThat(transform("filter(a, 'num', '$$num' > 10)")).isEqualTo(
				"{ \"$filter\" : { \"input\" : \"$a\" , \"as\" : \"num\" , \"cond\" : { \"$gt\" : [ \"$$num\" , 10]}}}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceIsArray() {
		assertThat(transform("isArray(a)")).isEqualTo("{ \"$isArray\" : \"$a\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceIsSize() {
		assertThat(transform("size(a)")).isEqualTo("{ \"$size\" : \"$a\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSlice() {
		assertThat(transform("slice(a, 10)")).isEqualTo("{ \"$slice\" : [ \"$a\" , 10]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeMap() {
		assertThat(transform("map(quizzes, 'grade', '$$grade' + 2)")).isEqualTo(
				"{ \"$map\" : { \"input\" : \"$quizzes\" , \"as\" : \"grade\" , \"in\" : { \"$add\" : [ \"$$grade\" , 2]}}}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeLet() {
		assertThat(transform("let({low:1, high:'$$low'}, gt('$$low', '$$high'))")).isEqualTo(
				"{ \"$let\" : { \"vars\" : { \"low\" : 1 , \"high\" : \"$$low\"} , \"in\" : { \"$gt\" : [ \"$$low\" , \"$$high\"]}}}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLiteral() {
		assertThat(transform("literal($1)")).isEqualTo("{ \"$literal\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceDayOfYear() {
		assertThat(transform("dayOfYear($1)")).isEqualTo("{ \"$dayOfYear\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceDayOfMonth() {
		assertThat(transform("dayOfMonth($1)")).isEqualTo("{ \"$dayOfMonth\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceDayOfWeek() {
		assertThat(transform("dayOfWeek($1)")).isEqualTo("{ \"$dayOfWeek\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceYear() {
		assertThat(transform("year($1)")).isEqualTo("{ \"$year\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceMonth() {
		assertThat(transform("month($1)")).isEqualTo("{ \"$month\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceWeek() {
		assertThat(transform("week($1)")).isEqualTo("{ \"$week\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceHour() {
		assertThat(transform("hour($1)")).isEqualTo("{ \"$hour\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceMinute() {
		assertThat(transform("minute($1)")).isEqualTo("{ \"$minute\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSecond() {
		assertThat(transform("second($1)")).isEqualTo("{ \"$second\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceMillisecond() {
		assertThat(transform("millisecond($1)")).isEqualTo("{ \"$millisecond\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceDateToString() {
		assertThat(transform("dateToString('%Y-%m-%d', $date)"))
				.isEqualTo("{ \"$dateToString\" : { \"format\" : \"%Y-%m-%d\" , \"date\" : \"$date\"}}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceCond() {
		assertThat(transform("cond(qty > 250, 30, 20)")).isEqualTo(
				"{ \"$cond\" : { \"if\" : { \"$gt\" : [ \"$qty\" , 250]} , \"then\" : 30 , \"else\" : 20}}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeIfNull() {
		assertThat(transform("ifNull(a, 10)")).isEqualTo("{ \"$ifNull\" : [ \"$a\" , 10]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSum() {
		assertThat(transform("sum(a, b)")).isEqualTo("{ \"$sum\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeAvg() {
		assertThat(transform("avg(a, b)")).isEqualTo("{ \"$avg\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceFirst() {
		assertThat(transform("first($1)")).isEqualTo("{ \"$first\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLast() {
		assertThat(transform("last($1)")).isEqualTo("{ \"$last\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeMax() {
		assertThat(transform("max(a, b)")).isEqualTo("{ \"$max\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeMin() {
		assertThat(transform("min(a, b)")).isEqualTo("{ \"$min\" : [ \"$a\" , \"$b\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodePush() {
		assertThat(transform("push({'item':'$item', 'quantity':'$qty'})"))
				.isEqualTo("{ \"$push\" : { \"item\" : \"$item\" , \"quantity\" : \"$qty\"}}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceAddToSet() {
		assertThat(transform("addToSet($1)")).isEqualTo("{ \"$addToSet\" : \"$1\"}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeStdDevPop() {
		assertThat(transform("stdDevPop(scores.score)"))
				.isEqualTo("{ \"$stdDevPop\" : [ \"$scores.score\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeStdDevSamp() {
		assertThat(transform("stdDevSamp(age)")).isEqualTo("{ \"$stdDevSamp\" : [ \"$age\"]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeEq() {
		assertThat(transform("foo == 10")).isEqualTo("{ \"$eq\" : [ \"$foo\" , 10]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeNe() {
		assertThat(transform("foo != 10")).isEqualTo("{ \"$ne\" : [ \"$foo\" , 10]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeGt() {
		assertThat(transform("foo > 10")).isEqualTo("{ \"$gt\" : [ \"$foo\" , 10]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeGte() {
		assertThat(transform("foo >= 10")).isEqualTo("{ \"$gte\" : [ \"$foo\" , 10]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeLt() {
		assertThat(transform("foo < 10")).isEqualTo("{ \"$lt\" : [ \"$foo\" , 10]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeLte() {
		assertThat(transform("foo <= 10")).isEqualTo("{ \"$lte\" : [ \"$foo\" , 10]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodePow() {
		assertThat(transform("foo^2")).isEqualTo("{ \"$pow\" : [ \"$foo\" , 2]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeOr() {
		assertThat(transform("true || false")).isEqualTo("{ \"$or\" : [ true , false]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderComplexOperationNodeOr() {
		assertThat(transform("1+2 || concat(a, b) || true")).isEqualTo(
				"{ \"$or\" : [ { \"$add\" : [ 1 , 2]} , { \"$concat\" : [ \"$a\" , \"$b\"]} , true]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeAnd() {
		assertThat(transform("true && false")).isEqualTo("{ \"$and\" : [ true , false]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderComplexOperationNodeAnd() {
		assertThat(transform("1+2 && concat(a, b) && true")).isEqualTo(
				"{ \"$and\" : [ { \"$add\" : [ 1 , 2]} , { \"$concat\" : [ \"$a\" , \"$b\"]} , true]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderNotCorrectly() {
		assertThat(transform("!true")).isEqualTo("{ \"$not\" : [ true]}");
	}

	@Test // DATAMONGO-1530
	void shouldRenderComplexNotCorrectly() {
		assertThat(transform("!(foo > 10)")).isEqualTo("{ \"$not\" : [ { \"$gt\" : [ \"$foo\" , 10]}]}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceIndexOfBytes() {
		assertThat(transform("indexOfBytes(item, 'foo')"))
				.isEqualTo("{ \"$indexOfBytes\" : [ \"$item\" , \"foo\"]}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceIndexOfCP() {
		assertThat(transform("indexOfCP(item, 'foo')"))
				.isEqualTo("{ \"$indexOfCP\" : [ \"$item\" , \"foo\"]}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceSplit() {
		assertThat(transform("split(item, ',')")).isEqualTo("{ \"$split\" : [ \"$item\" , \",\"]}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceStrLenBytes() {
		assertThat(transform("strLenBytes(item)")).isEqualTo("{ \"$strLenBytes\" : \"$item\"}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceStrLenCP() {
		assertThat(transform("strLenCP(item)")).isEqualTo("{ \"$strLenCP\" : \"$item\"}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodSubstrCP() {
		assertThat(transform("substrCP(item, 0, 5)")).isEqualTo("{ \"$substrCP\" : [ \"$item\" , 0 , 5]}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceReverseArray() {
		assertThat(transform("reverseArray(array)")).isEqualTo("{ \"$reverseArray\" : \"$array\"}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceReduce() {
		assertThat(transform("reduce(field, '', {'$concat':{'$$value','$$this'}})")).isEqualTo(
				"{ \"$reduce\" : { \"input\" : \"$field\" , \"initialValue\" : \"\" , \"in\" : { \"$concat\" : [ \"$$value\" , \"$$this\"]}}}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceZip() {
		assertThat(transform("zip(new String[]{'$array1', '$array2'})"))
				.isEqualTo("{ \"$zip\" : { \"inputs\" : [ \"$array1\" , \"$array2\"]}}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceZipWithOptionalArgs() {
		assertThat(transform("zip(new String[]{'$array1', '$array2'}, true, new int[]{1,2})")).isEqualTo(
				"{ \"$zip\" : { \"inputs\" : [ \"$array1\" , \"$array2\"] , \"useLongestLength\" : true , \"defaults\" : [ 1 , 2]}}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodIn() {
		assertThat(transform("in('item', array)")).isEqualTo("{ \"$in\" : [ \"item\" , \"$array\"]}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodRefereneIsoDayOfWeek() {
		assertThat(transform("isoDayOfWeek(date)")).isEqualTo("{ \"$isoDayOfWeek\" : \"$date\"}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodRefereneIsoWeek() {
		assertThat(transform("isoWeek(date)")).isEqualTo("{ \"$isoWeek\" : \"$date\"}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodRefereneIsoWeekYear() {
		assertThat(transform("isoWeekYear(date)")).isEqualTo("{ \"$isoWeekYear\" : \"$date\"}");
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodRefereneType() {
		assertThat(transform("type(a)")).isEqualTo("{ \"$type\" : \"$a\"}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderArrayToObjectWithFieldReference() {
		assertThat(transform("arrayToObject(field)")).isEqualTo("{ \"$arrayToObject\" : \"$field\"}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderArrayToObjectWithArray() {

		assertThat(transform("arrayToObject(new String[]{'key', 'value'})"))
				.isEqualTo("{ \"$arrayToObject\" : [\"key\", \"value\"]}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderObjectToArrayWithFieldReference() {
		assertThat(transform("objectToArray(field)")).isEqualTo("{ \"$objectToArray\" : \"$field\"}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderMergeObjects() {

		assertThat(transform("mergeObjects(field1, $$ROOT)"))
				.isEqualTo("{ \"$mergeObjects\" : [\"$field1\", \"$$ROOT\"]}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderTrimWithoutChars() {
		assertThat(transform("trim(field)")).isEqualTo("{ \"$trim\" : {\"input\" : \"$field\"}}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderTrimWithChars() {

		assertThat(transform("trim(field, 'ie')"))
				.isEqualTo("{ \"$trim\" : {\"input\" : \"$field\", \"chars\" : \"ie\" }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderTrimWithCharsFromFieldReference() {

		assertThat(transform("trim(field1, field2)"))
				.isEqualTo("{ \"$trim\" : {\"input\" : \"$field1\", \"chars\" : \"$field2\" }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderLtrimWithoutChars() {
		assertThat(transform("ltrim(field)")).isEqualTo("{ \"$ltrim\" : {\"input\" : \"$field\"}}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderLtrimWithChars() {

		assertThat(transform("ltrim(field, 'ie')"))
				.isEqualTo("{ \"$ltrim\" : {\"input\" : \"$field\", \"chars\" : \"ie\" }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderLtrimWithCharsFromFieldReference() {

		assertThat(transform("ltrim(field1, field2)"))
				.isEqualTo("{ \"$ltrim\" : {\"input\" : \"$field1\", \"chars\" : \"$field2\" }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderRtrimWithoutChars() {
		assertThat(transform("rtrim(field)")).isEqualTo("{ \"$rtrim\" : {\"input\" : \"$field\"}}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderRtrimWithChars() {

		assertThat(transform("rtrim(field, 'ie')"))
				.isEqualTo("{ \"$rtrim\" : {\"input\" : \"$field\", \"chars\" : \"ie\" }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderRtrimWithCharsFromFieldReference() {

		assertThat(transform("rtrim(field1, field2)"))
				.isEqualTo("{ \"$rtrim\" : {\"input\" : \"$field1\", \"chars\" : \"$field2\" }}");
	}

	@Test // GH-3725
	void shouldRenderRegexFindWithoutOptions() {

		assertThat(transform("regexFind(field1,'e')"))
				.isEqualTo("{ \"$regexFind\" : {\"input\" : \"$field1\" , \"regex\" : \"e\"}}");
	}

	@Test // GH-3725
	void shouldRenderRegexFindWithOptions() {

		assertThat(transform("regexFind(field1,'e','i')"))
				.isEqualTo("{ \"$regexFind\" : {\"input\" : \"$field1\" , \"regex\" : \"e\" , \"options\" : \"i\"}}");
	}

	@Test // GH-3725
	void shouldRenderRegexFindWithOptionsFromFieldReference() {

		assertThat(transform("regexFind(field1,'e',field2)"))
				.isEqualTo("{ \"$regexFind\" : {\"input\" : \"$field1\" , \"regex\" : \"e\" , \"options\" : \"$field2\"}}");
	}

	@Test // GH-3725
	void shouldRenderRegexFindAllWithoutOptions() {

		assertThat(transform("regexFindAll(field1,'e')"))
				.isEqualTo("{ \"$regexFindAll\" : {\"input\" : \"$field1\" , \"regex\" : \"e\"}}");
	}

	@Test // GH-3725
	void shouldRenderRegexFindAllWithOptions() {

		assertThat(transform("regexFindAll(field1,'e','i')"))
				.isEqualTo("{ \"$regexFindAll\" : {\"input\" : \"$field1\" , \"regex\" : \"e\" , \"options\" : \"i\"}}");
	}

	@Test // GH-3725
	void shouldRenderRegexFindAllWithOptionsFromFieldReference() {

		assertThat(transform("regexFindAll(field1,'e',field2)"))
				.isEqualTo("{ \"$regexFindAll\" : {\"input\" : \"$field1\" , \"regex\" : \"e\" , \"options\" : \"$field2\"}}");
	}

	@Test // GH-3725
	void shouldRenderRegexMatchWithoutOptions() {

		assertThat(transform("regexMatch(field1,'e')"))
				.isEqualTo("{ \"$regexMatch\" : {\"input\" : \"$field1\" , \"regex\" : \"e\"}}");
	}

	@Test // GH-3725
	void shouldRenderRegexMatchWithOptions() {

		assertThat(transform("regexMatch(field1,'e','i')"))
				.isEqualTo("{ \"$regexMatch\" : {\"input\" : \"$field1\" , \"regex\" : \"e\" , \"options\" : \"i\"}}");
	}

	@Test // GH-3725
	void shouldRenderRegexMatchWithOptionsFromFieldReference() {

		assertThat(transform("regexMatch(field1,'e',field2)"))
				.isEqualTo("{ \"$regexMatch\" : {\"input\" : \"$field1\" , \"regex\" : \"e\" , \"options\" : \"$field2\"}}");
	}
	
	@Test // GH-3695
	void shouldRenderReplaceOne() {

		assertThat(transform("replaceOne(field, 'bar', 'baz')"))
				.isEqualTo("{ \"$replaceOne\" : {\"input\" : \"$field\" , \"find\" : \"bar\" , \"replacement\" : \"baz\"}}");
	}

	@Test // GH-3695
	void shouldRenderReplaceAll() {

		assertThat(transform("replaceAll(field, 'bar', 'baz')"))
				.isEqualTo("{ \"$replaceAll\" : {\"input\" : \"$field\" , \"find\" : \"bar\" , \"replacement\" : \"baz\"}}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderConvertWithoutOptionalParameters() {

		assertThat(transform("convert(field, 'string')"))
				.isEqualTo("{ \"$convert\" : {\"input\" : \"$field\", \"to\" : \"string\" }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderConvertWithOnError() {

		assertThat(transform("convert(field, 'int', 'Not an integer.')"))
				.isEqualTo("{ \"$convert\" : {\"input\" : \"$field\", \"to\" : \"int\", \"onError\" : \"Not an integer.\" }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderConvertWithOnErrorOnNull() {

		assertThat(transform("convert(field, 'int', 'Not an integer.', -1)")).isEqualTo(
				"{ \"$convert\" : {\"input\" : \"$field\", \"to\" : \"int\", \"onError\" : \"Not an integer.\", \"onNull\" : -1 }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderToBool() {
		assertThat(transform("toBool(field)")).isEqualTo("{ \"$toBool\" : \"$field\"}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderToDate() {
		assertThat(transform("toDate(field)")).isEqualTo("{ \"$toDate\" : \"$field\"}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderToDecimal() {
		assertThat(transform("toDecimal(field)")).isEqualTo("{ \"$toDecimal\" : \"$field\"}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderToDouble() {
		assertThat(transform("toDouble(field)")).isEqualTo("{ \"$toDouble\" : \"$field\"}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderToInt() {
		assertThat(transform("toInt(field)")).isEqualTo("{ \"$toInt\" : \"$field\"}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderToLong() {
		assertThat(transform("toLong(field)")).isEqualTo("{ \"$toLong\" : \"$field\"}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderToObjectId() {
		assertThat(transform("toObjectId(field)")).isEqualTo("{ \"$toObjectId\" : \"$field\"}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderToString() {
		assertThat(transform("toString(field)")).isEqualTo("{ \"$toString\" : \"$field\"}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateFromStringWithoutOptionalParameters() {

		assertThat(transform("dateFromString(field)"))
				.isEqualTo("{ \"$dateFromString\" : {\"dateString\" : \"$field\" }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateFromStringWithFormat() {

		assertThat(transform("dateFromString(field, 'DD-MM-YYYY')")).isEqualTo(
				"{ \"$dateFromString\" : {\"dateString\" : \"$field\", \"format\" : \"DD-MM-YYYY\" }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateFromStringWithFormatAndTimezone() {

		assertThat(transform("dateFromString(field, 'DD-MM-YYYY', 'UTC')")).isEqualTo(
				"{ \"$dateFromString\" : {\"dateString\" : \"$field\", \"format\" : \"DD-MM-YYYY\", \"timezone\" : \"UTC\" }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateFromStringWithFormatTimezoneAndOnError() {

		assertThat(transform("dateFromString(field, 'DD-MM-YYYY', 'UTC', -1)")).isEqualTo(
				"{ \"$dateFromString\" : {\"dateString\" : \"$field\", \"format\" : \"DD-MM-YYYY\", \"timezone\" : \"UTC\", \"onError\" : -1 }}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateFromStringWithFormatTimezoneOnErrorAndOnNull() {

		assertThat(transform("dateFromString(field, 'DD-MM-YYYY', 'UTC', -1, -2)")).isEqualTo(
				"{ \"$dateFromString\" : {\"dateString\" : \"$field\", \"format\" : \"DD-MM-YYYY\", \"timezone\" : \"UTC\", \"onError\" : -1,  \"onNull\" : -2}}");
	}

	@Test // DATAMONGO-2077, DATAMONGO-2671
	void shouldRenderDateFromParts() {

		assertThat(transform("dateFromParts(y, m, d, h, mm, s, ms, 'UTC')")).isEqualTo(
				"{ \"$dateFromParts\" : {\"year\" : \"$y\", \"month\" : \"$m\", \"day\" : \"$d\", \"hour\" : \"$h\",  \"minute\" : \"$mm\",  \"second\" : \"$s\", \"millisecond\" : \"$ms\", \"timezone\" : \"UTC\"}}");
	}

	@Test // DATAMONGO-2077, DATAMONGO-2671
	void shouldRenderIsoDateFromParts() {

		assertThat(transform("isoDateFromParts(y, m, d, h, mm, s, ms, 'UTC')")).isEqualTo(
				"{ \"$dateFromParts\" : {\"isoWeekYear\" : \"$y\", \"isoWeek\" : \"$m\", \"isoDayOfWeek\" : \"$d\", \"hour\" : \"$h\",  \"minute\" : \"$mm\",  \"second\" : \"$s\", \"millisecond\" : \"$ms\", \"timezone\" : \"UTC\"}}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateToParts() {

		assertThat(transform("dateToParts(field, 'UTC', false)")).isEqualTo(
				"{ \"$dateToParts\" : {\"date\" : \"$field\", \"timezone\" : \"UTC\", \"iso8601\" : false}}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderIndexOfArray() {

		assertThat(transform("indexOfArray(field, 2)"))
				.isEqualTo("{ \"$indexOfArray\" : [\"$field\", 2 ]}");
	}

	@Test // DATAMONGO-2077
	void shouldRenderRange() {

		assertThat(transform("range(0, 10, 2)")).isEqualTo("{ \"$range\" : [0, 10, 2 ]}");
	}

	@Test // DATAMONGO-2370
	void shouldRenderRound() {
		assertThat(transform("round(field)")).isEqualTo("{ \"$round\" : [\"$field\"]}");
	}

	@Test // DATAMONGO-2370
	void shouldRenderRoundWithPlace() {
		assertThat(transform("round(field, 2)")).isEqualTo("{ \"$round\" : [\"$field\", 2]}");
	}

	@Test // GH-3714
	void shouldRenderDegreesToRadians() {
		assertThat(transform("degreesToRadians(angle_a)")).isEqualTo("{ \"$degreesToRadians\" : \"$angle_a\"}");
	}

	@Test // GH-3712
	void shouldRenderCovariancePop() {
		assertThat(transform("covariancePop(field1, field2)"))
				.isEqualTo("{ \"$covariancePop\" : [\"$field1\", \"$field2\"]}");
	}

	@Test // GH-3712
	void shouldRenderCovarianceSamp() {
		assertThat(transform("covarianceSamp(field1, field2)"))
				.isEqualTo("{ \"$covarianceSamp\" : [\"$field1\", \"$field2\"]}");
	}

	@Test // GH-3715
	void shouldRenderRank() {
		assertThat(transform("rank()")).isEqualTo("{ $rank : {} }");
	}

	@Test // GH-3715
	void shouldRenderDenseRank() {
		assertThat(transform("denseRank()")).isEqualTo("{ $denseRank : {} }");
	}

	@Test // GH-3717
	void shouldRenderDocumentNumber() {
		assertThat(transform("documentNumber()")).isEqualTo("{ $documentNumber : {} }");
	}

	@Test // GH-3727
	void rendersShift() {

		assertThat(transform("shift(quantity, 1)"))
				.isEqualTo("{ $shift: { output: \"$quantity\", by: 1 } }");
	}

	@Test // GH-3727
	void rendersShiftWithDefault() {

		assertThat(transform("shift(quantity, 1, 'Not available')"))
				.isEqualTo("{ $shift: { output: \"$quantity\", by: 1, default: \"Not available\" } }");
	}

	@Test // GH-3716
	void shouldRenderDerivative() {
		assertThat(transform("derivative(miles, 'hour')"))
				.isEqualTo("{ \"$derivative\" : { input : '$miles', unit : 'hour'} }");
	}

	@Test // GH-3721
	void shouldRenderIntegral() {
		assertThat(transform("integral(field)")).isEqualTo("{ \"$integral\" : { \"input\" : \"$field\" }}");
	}

	@Test // GH-3721
	void shouldRenderIntegralWithUnit() {
		assertThat(transform("integral(field, 'hour')"))
				.isEqualTo("{ \"$integral\" : { \"input\" : \"$field\", \"unit\" : \"hour\" }}");
	}

	@Test // GH-3728
	void shouldRenderSin() {
		assertThat(transform("sin(angle)")).isEqualTo("{ \"$sin\" : \"$angle\"}");
	}

	@Test // GH-3728
	void shouldRenderSinh() {
		assertThat(transform("sinh(angle)")).isEqualTo("{ \"$sinh\" : \"$angle\"}");
	}

	@Test // GH-3708
	void shouldRenderASin() {
		assertThat(transform("asin(number)")).isEqualTo("{ \"$asin\" : \"$number\"}");
	}

	@Test // GH-3708
	void shouldRenderASinh() {
		assertThat(transform("asinh(number)")).isEqualTo("{ \"$asinh\" : \"$number\"}");
	}

	@Test // GH-3710
	void shouldRenderCos() {
		assertThat(transform("cos(angle)")).isEqualTo("{ \"$cos\" : \"$angle\"}");
	}

	@Test // GH-3710
	void shouldRenderCosh() {
		assertThat(transform("cosh(angle)")).isEqualTo("{ \"$cosh\" : \"$angle\"}");
	}

	@Test // GH-3707
	void shouldRenderACos() {
		assertThat(transform("acos(angle)")).isEqualTo("{ \"$acos\" : \"$angle\"}");
	}

	@Test // GH-3707
	void shouldRenderACosh() {
		assertThat(transform("acosh(angle)")).isEqualTo("{ \"$acosh\" : \"$angle\"}");
	}

	@Test // GH-3730
	void shouldRenderTan() {
		assertThat(transform("tan(angle)")).isEqualTo("{ \"$tan\" : \"$angle\"}");
	}

	@Test // GH-3730
	void shouldRenderTanh() {
		assertThat(transform("tanh(angle)")).isEqualTo("{ \"$tanh\" : \"$angle\"}");
	}

	@Test // DATAMONGO - 3709
	void shouldRenderATan() {
		assertThat(transform("atan(number)")).isEqualTo("{ \"$atan\" : \"$number\"}");
	}

	@Test // DATAMONGO - 3709
	void shouldRenderATan2() {
		assertThat(transform("atan2(number1,number2)")).isEqualTo("{ \"$atan2\" : [ \"$number1\" , \"$number2\" ] }");
	}

	@Test // DATAMONGO - 3709
	void shouldRenderATanh() {
		assertThat(transform("atanh(number)")).isEqualTo("{ \"$atanh\" : \"$number\"}");
	}

	@Test // GH-3713
	void shouldRenderDateAdd() {
		assertThat(transform("dateAdd(purchaseDate, 'day', 3)"))
				.isEqualTo("{ $dateAdd: { startDate: \"$purchaseDate\", unit: \"day\", amount: 3 } }");
	}

	@Test // GH-4139
	void shouldRenderDateSubtract() {
		assertThat(transform("dateSubtract(purchaseDate, 'day', 3)"))
				.isEqualTo("{ $dateSubtract: { startDate: \"$purchaseDate\", unit: \"day\", amount: 3 } }");
	}

	@Test // GH-3713
	void shouldRenderDateDiff() {
		assertThat(transform("dateDiff(purchaseDate, delivered, 'day')"))
				.isEqualTo("{ $dateDiff: { startDate: \"$purchaseDate\", endDate: \"$delivered\", unit: \"day\" } }");
	}

	@Test // GH-3724
	void shouldRenderRand() {
		assertThat(transform("rand()")).isEqualTo("{ $rand : {} }");
	}

	@Test // GH-4139
	void shouldRenderBottom() {
		assertThat(transform("bottom(new String[]{\"$playerId\", \"$score\" }, { \"score\" : -1 })")).isEqualTo("{ $bottom : { output: [ \"$playerId\", \"$score\" ], sortBy: { \"score\": -1 }}}");
	}

	@Test // GH-4139
	void shouldRenderBottomN() {
		assertThat(transform("bottomN(3, new String[]{\"$playerId\", \"$score\" }, { \"score\" : -1 })")).isEqualTo("{ $bottomN : { n : 3, output: [ \"$playerId\", \"$score\" ], sortBy: { \"score\": -1 }}}");
	}

	@Test // GH-4139
	void shouldRenderTop() {
		assertThat(transform("top(new String[]{\"$playerId\", \"$score\" }, { \"score\" : -1 })")).isEqualTo("{ $top : { output: [ \"$playerId\", \"$score\" ], sortBy: { \"score\": -1 }}}");
	}

	@Test // GH-4139
	void shouldRenderTopN() {
		assertThat(transform("topN(3, new String[]{\"$playerId\", \"$score\" }, { \"score\" : -1 })")).isEqualTo("{ $topN : { n : 3, output: [ \"$playerId\", \"$score\" ], sortBy: { \"score\": -1 }}}");
	}

	@Test // GH-4139
	void shouldRenderFirstN() {
		assertThat(transform("firstN(3, \"$score\")")).isEqualTo("{ $firstN : { n : 3, input : \"$score\" }}");
	}

	@Test // GH-4139
	void shouldRenderLastN() {
		assertThat(transform("lastN(3, \"$score\")")).isEqualTo("{ $lastN : { n : 3, input : \"$score\" }}");
	}

	@Test // GH-4139
	void shouldRenderMaxN() {
		assertThat(transform("maxN(3, \"$score\")")).isEqualTo("{ $maxN : { n : 3, input : \"$score\" }}");
	}

	@Test // GH-4139
	void shouldRenderMinN() {
		assertThat(transform("minN(3, \"$score\")")).isEqualTo("{ $minN : { n : 3, input : \"$score\" }}");
	}

	@Test // GH-4139
	void shouldRenderDateTrunc() {
		assertThat(transform("dateTrunc(purchaseDate, \"week\", 2, \"monday\")")).isEqualTo("{ $dateTrunc : { date : \"$purchaseDate\", unit : \"week\", binSize : 2, startOfWeek : \"monday\" }}");
	}

	@Test // GH-4139
	void shouldRenderGetField() {
		assertThat(transform("getField(\"score\", source)")).isEqualTo("{ $getField : { field : \"score\", input : \"$source\" }}");
	}

	@Test // GH-4139
	void shouldRenderSetField() {
		assertThat(transform("setField(\"score\", 100, source)")).isEqualTo("{ $setField : { field : \"score\", value : 100, input : \"$source\" }}");
	}

	@Test // GH-4139
	void shouldRenderSortArray() {
		assertThat(transform(
				"sortArray(team, new org.bson.Document(\"name\" , 1))")).isEqualTo("{ $sortArray : { input : \"$team\", sortBy : {\"name\" : 1 } }}");
	}

	@Test // GH-4139
	void shouldTsIncrement() {
		assertThat(transform("tsIncrement(saleTimestamp)")).isEqualTo("{ $tsIncrement: \"$saleTimestamp\" }");
	}

	@Test // GH-4139
	void shouldTsSecond() {
		assertThat(transform("tsSecond(saleTimestamp)")).isEqualTo("{ $tsSecond: \"$saleTimestamp\" }");
	}

	@Test // GH-4139
	void shouldRenderLocf() {
		assertThat(transform("locf(price)")).isEqualTo("{ $locf: \"$price\" }");
	}

	@Test // GH-4473
	void shouldRenderPercentile() {
		assertThat(transform("percentile(new String[]{\"$scoreOne\", \"$scoreTwo\" }, new double[]{0.4}, \"approximate\")"))
			.isEqualTo("{ $percentile : { input : [\"$scoreOne\", \"$scoreTwo\"], p : [0.4], method : \"approximate\" }}");

		assertThat(transform("percentile(score, new double[]{0.4, 0.85}, \"approximate\")"))
			.isEqualTo("{ $percentile : { input : \"$score\", p : [0.4, 0.85], method : \"approximate\" }}");

		assertThat(transform("percentile(\"$score\", new double[]{0.4, 0.85}, \"approximate\")"))
			.isEqualTo("{ $percentile : { input : \"$score\", p : [0.4, 0.85], method : \"approximate\" }}");
	}

	@Test // GH-4472
	void shouldRenderMedian() {

		assertThat(transform("median(new String[]{\"$scoreOne\", \"$scoreTwo\" }, \"approximate\")"))
				.isEqualTo("{ $median : { input : [\"$scoreOne\", \"$scoreTwo\"], method : \"approximate\" }}");

		assertThat(transform("median(score, \"approximate\")"))
				.isEqualTo("{ $median : { input : \"$score\", method : \"approximate\" }}");
	}

	private Document transform(String expression, Object... params) {
		return (Document) transformer.transform(expression, Aggregation.DEFAULT_CONTEXT, params);
	}

	private Object transformValue(String expression, Object... params) {
		Object result = transformer.transform(expression, Aggregation.DEFAULT_CONTEXT, params);
		return result == null ? null : (!(result instanceof org.bson.Document) ? result.toString() : result);
	}
}
