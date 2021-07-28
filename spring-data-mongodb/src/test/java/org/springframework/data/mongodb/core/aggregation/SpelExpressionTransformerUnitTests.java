/*
 * Copyright 2013-2021 the original author or authors.
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

		assertThat(transform("1")).isEqualTo("1");
		assertThat(transform("-1")).isEqualTo("-1");
		assertThat(transform("1.0")).isEqualTo("1.0");
		assertThat(transform("-1.0")).isEqualTo("-1.0");
		assertThat(transform("null")).isNull();
	}

	@Test // DATAMONGO-774
	void shouldSupportKnownOperands() {

		assertThat(transform("a + b")).isEqualTo(Document.parse("{ \"$add\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transform("a - b")).isEqualTo(Document.parse("{ \"$subtract\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transform("a * b")).isEqualTo(Document.parse("{ \"$multiply\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transform("a / b")).isEqualTo(Document.parse("{ \"$divide\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transform("a % b")).isEqualTo(Document.parse("{ \"$mod\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-774
	void shouldThrowExceptionOnUnknownOperand() {
		assertThatIllegalArgumentException().isThrownBy(() -> transform("a++"));
	}

	@Test // DATAMONGO-774
	void shouldRenderSumExpression() {
		assertThat(transform("a + 1")).isEqualTo(Document.parse("{ \"$add\" : [ \"$a\" , 1]}"));
	}

	@Test // DATAMONGO-774
	void shouldRenderFormula() {

		assertThat(transform("(netPrice + surCharge) * taxrate + 42")).isEqualTo(Document.parse(
				"{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}"));
	}

	@Test // DATAMONGO-774
	void shouldRenderFormulaInCurlyBrackets() {

		assertThat(transform("{(netPrice + surCharge) * taxrate + 42}")).isEqualTo(Document.parse(
				"{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}"));
	}

	@Test // DATAMONGO-774
	void shouldRenderFieldReference() {

		assertThat(transform("foo")).isEqualTo("$foo");
		assertThat(transform("$foo")).isEqualTo("$foo");
	}

	@Test // DATAMONGO-774
	void shouldRenderNestedFieldReference() {

		assertThat(transform("foo.bar")).isEqualTo("$foo.bar");
		assertThat(transform("$foo.bar")).isEqualTo("$foo.bar");
	}

	@Test // DATAMONGO-774
	@Disabled
	void shouldRenderNestedIndexedFieldReference() {

		// TODO add support for rendering nested indexed field references
		assertThat(transform("foo[3].bar")).isEqualTo("$foo[3].bar");
	}

	@Test // DATAMONGO-774
	void shouldRenderConsecutiveOperation() {
		assertThat(transform("1 + 1 + 1")).isEqualTo(Document.parse("{ \"$add\" : [ 1 , 1 , 1]}"));
	}

	@Test // DATAMONGO-774
	void shouldRenderComplexExpression0() {

		assertThat(transform("-(1 + q)"))
				.isEqualTo(Document.parse("{ \"$multiply\" : [ -1 , { \"$add\" : [ 1 , \"$q\"]}]}"));
	}

	@Test // DATAMONGO-774
	void shouldRenderComplexExpression1() {

		assertThat(transform("1 + (q + 1) / (q - 1)")).isEqualTo(Document.parse(
				"{ \"$add\" : [ 1 , { \"$divide\" : [ { \"$add\" : [ \"$q\" , 1]} , { \"$subtract\" : [ \"$q\" , 1]}]}]}"));
	}

	@Test // DATAMONGO-774
	void shouldRenderComplexExpression2() {

		assertThat(transform("(q + 1 + 4 - 5) / (q + 1 + 3 + 4)")).isEqualTo(Document.parse(
				"{ \"$divide\" : [ { \"$subtract\" : [ { \"$add\" : [ \"$q\" , 1 , 4]} , 5]} , { \"$add\" : [ \"$q\" , 1 , 3 , 4]}]}"));
	}

	@Test // DATAMONGO-774
	void shouldRenderBinaryExpressionWithMixedSignsCorrectly() {

		assertThat(transform("-4 + 1")).isEqualTo(Document.parse("{ \"$add\" : [ -4 , 1]}"));
		assertThat(transform("1 + -4")).isEqualTo(Document.parse("{ \"$add\" : [ 1 , -4]}"));
	}

	@Test // DATAMONGO-774
	void shouldRenderConsecutiveOperationsInComplexExpression() {

		assertThat(transform("1 + 1 + (1 + 1 + 1) / q"))
				.isEqualTo(Document.parse("{ \"$add\" : [ 1 , 1 , { \"$divide\" : [ { \"$add\" : [ 1 , 1 , 1]} , \"$q\"]}]}"));
	}

	@Test // DATAMONGO-774
	void shouldRenderParameterExpressionResults() {
		assertThat(transform("[0] + [1] + [2]", 1, 2, 3)).isEqualTo(Document.parse("{ \"$add\" : [ 1 , 2 , 3]}"));
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
		assertThat(transform("[0].age + a.c", person)).isEqualTo(Document.parse("{ \"$add\" : [ 10 , \"$a.c\"] }"));
	}

	@Test // DATAMONGO-840
	void shouldRenderCompoundExpressionsWithOnlyFieldReferences() {

		assertThat(transform("a.b + a.c")).isEqualTo(Document.parse("{ \"$add\" : [ \"$a.b\" , \"$a.c\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeAnd() {
		assertThat(transform("and(a, b)")).isEqualTo(Document.parse("{ \"$and\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeOr() {
		assertThat(transform("or(a, b)")).isEqualTo(Document.parse("{ \"$or\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeNot() {
		assertThat(transform("not(a)")).isEqualTo(Document.parse("{ \"$not\" : [ \"$a\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSetEquals() {
		assertThat(transform("setEquals(a, b)")).isEqualTo(Document.parse("{ \"$setEquals\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSetEqualsForArrays() {
		assertThat(transform("setEquals(new int[]{1,2,3}, new int[]{4,5,6})"))
				.isEqualTo(Document.parse("{ \"$setEquals\" : [ [ 1 , 2 , 3] , [ 4 , 5 , 6]]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSetEqualsMixedArrays() {
		assertThat(transform("setEquals(a, new int[]{4,5,6})"))
				.isEqualTo(Document.parse("{ \"$setEquals\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSetIntersection() {
		assertThat(transform("setIntersection(a, new int[]{4,5,6})"))
				.isEqualTo(Document.parse("{ \"$setIntersection\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSetUnion() {
		assertThat(transform("setUnion(a, new int[]{4,5,6})"))
				.isEqualTo(Document.parse("{ \"$setUnion\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSeDifference() {
		assertThat(transform("setDifference(a, new int[]{4,5,6})"))
				.isEqualTo(Document.parse("{ \"$setDifference\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSetIsSubset() {
		assertThat(transform("setIsSubset(a, new int[]{4,5,6})"))
				.isEqualTo(Document.parse("{ \"$setIsSubset\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceAnyElementTrue() {
		assertThat(transform("anyElementTrue(a)")).isEqualTo(Document.parse("{ \"$anyElementTrue\" : [ \"$a\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceAllElementsTrue() {
		assertThat(transform("allElementsTrue(a, new int[]{4,5,6})"))
				.isEqualTo(Document.parse("{ \"$allElementsTrue\" : [ \"$a\" , [ 4 , 5 , 6]]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceCmp() {
		assertThat(transform("cmp(a, 250)")).isEqualTo(Document.parse("{ \"$cmp\" : [ \"$a\" , 250]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceEq() {
		assertThat(transform("eq(a, 250)")).isEqualTo(Document.parse("{ \"$eq\" : [ \"$a\" , 250]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceGt() {
		assertThat(transform("gt(a, 250)")).isEqualTo(Document.parse("{ \"$gt\" : [ \"$a\" , 250]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceGte() {
		assertThat(transform("gte(a, 250)")).isEqualTo(Document.parse("{ \"$gte\" : [ \"$a\" , 250]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLt() {
		assertThat(transform("lt(a, 250)")).isEqualTo(Document.parse("{ \"$lt\" : [ \"$a\" , 250]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLte() {
		assertThat(transform("lte(a, 250)")).isEqualTo(Document.parse("{ \"$lte\" : [ \"$a\" , 250]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNe() {
		assertThat(transform("ne(a, 250)")).isEqualTo(Document.parse("{ \"$ne\" : [ \"$a\" , 250]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceAbs() {
		assertThat(transform("abs(1)")).isEqualTo(Document.parse("{ \"$abs\" : 1}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceAdd() {
		assertThat(transform("add(a, 250)")).isEqualTo(Document.parse("{ \"$add\" : [ \"$a\" , 250]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceCeil() {
		assertThat(transform("ceil(7.8)")).isEqualTo(Document.parse("{ \"$ceil\" : 7.8}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceDivide() {
		assertThat(transform("divide(a, 250)")).isEqualTo(Document.parse("{ \"$divide\" : [ \"$a\" , 250]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceExp() {
		assertThat(transform("exp(2)")).isEqualTo(Document.parse("{ \"$exp\" : 2}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceFloor() {
		assertThat(transform("floor(2)")).isEqualTo(Document.parse("{ \"$floor\" : 2}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLn() {
		assertThat(transform("ln(2)")).isEqualTo(Document.parse("{ \"$ln\" : 2}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLog() {
		assertThat(transform("log(100, 10)")).isEqualTo(Document.parse("{ \"$log\" : [ 100 , 10]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLog10() {
		assertThat(transform("log10(100)")).isEqualTo(Document.parse("{ \"$log10\" : 100}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeMod() {
		assertThat(transform("mod(a, b)")).isEqualTo(Document.parse("{ \"$mod\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeMultiply() {
		assertThat(transform("multiply(a, b)")).isEqualTo(Document.parse("{ \"$multiply\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodePow() {
		assertThat(transform("pow(a, 2)")).isEqualTo(Document.parse("{ \"$pow\" : [ \"$a\" , 2]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSqrt() {
		assertThat(transform("sqrt(2)")).isEqualTo(Document.parse("{ \"$sqrt\" : 2}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSubtract() {
		assertThat(transform("subtract(a, b)")).isEqualTo(Document.parse("{ \"$subtract\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceTrunc() {
		assertThat(transform("trunc(2.1)")).isEqualTo(Document.parse("{ \"$trunc\" : 2.1}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeConcat() {
		assertThat(transform("concat(a, b, 'c')")).isEqualTo(Document.parse("{ \"$concat\" : [ \"$a\" , \"$b\" , \"c\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSubstrc() {
		assertThat(transform("substr(a, 0, 1)")).isEqualTo(Document.parse("{ \"$substr\" : [ \"$a\" , 0 , 1]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceToLower() {
		assertThat(transform("toLower(a)")).isEqualTo(Document.parse("{ \"$toLower\" : \"$a\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceToUpper() {
		assertThat(transform("toUpper(a)")).isEqualTo(Document.parse("{ \"$toUpper\" : \"$a\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeStrCaseCmp() {
		assertThat(transform("strcasecmp(a, b)")).isEqualTo(Document.parse("{ \"$strcasecmp\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceMeta() {
		assertThat(transform("meta('textScore')")).isEqualTo(Document.parse("{ \"$meta\" : \"textScore\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeArrayElemAt() {
		assertThat(transform("arrayElemAt(a, 10)")).isEqualTo(Document.parse("{ \"$arrayElemAt\" : [ \"$a\" , 10]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeConcatArrays() {
		assertThat(transform("concatArrays(a, b, c)"))
				.isEqualTo(Document.parse("{ \"$concatArrays\" : [ \"$a\" , \"$b\" , \"$c\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeFilter() {
		assertThat(transform("filter(a, 'num', '$$num' > 10)")).isEqualTo(Document.parse(
				"{ \"$filter\" : { \"input\" : \"$a\" , \"as\" : \"num\" , \"cond\" : { \"$gt\" : [ \"$$num\" , 10]}}}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceIsArray() {
		assertThat(transform("isArray(a)")).isEqualTo(Document.parse("{ \"$isArray\" : \"$a\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceIsSize() {
		assertThat(transform("size(a)")).isEqualTo(Document.parse("{ \"$size\" : \"$a\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSlice() {
		assertThat(transform("slice(a, 10)")).isEqualTo(Document.parse("{ \"$slice\" : [ \"$a\" , 10]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeMap() {
		assertThat(transform("map(quizzes, 'grade', '$$grade' + 2)")).isEqualTo(Document.parse(
				"{ \"$map\" : { \"input\" : \"$quizzes\" , \"as\" : \"grade\" , \"in\" : { \"$add\" : [ \"$$grade\" , 2]}}}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeLet() {
		assertThat(transform("let({low:1, high:'$$low'}, gt('$$low', '$$high'))")).isEqualTo(Document.parse(
				"{ \"$let\" : { \"vars\" : { \"low\" : 1 , \"high\" : \"$$low\"} , \"in\" : { \"$gt\" : [ \"$$low\" , \"$$high\"]}}}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLiteral() {
		assertThat(transform("literal($1)")).isEqualTo(Document.parse("{ \"$literal\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceDayOfYear() {
		assertThat(transform("dayOfYear($1)")).isEqualTo(Document.parse("{ \"$dayOfYear\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceDayOfMonth() {
		assertThat(transform("dayOfMonth($1)")).isEqualTo(Document.parse("{ \"$dayOfMonth\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceDayOfWeek() {
		assertThat(transform("dayOfWeek($1)")).isEqualTo(Document.parse("{ \"$dayOfWeek\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceYear() {
		assertThat(transform("year($1)")).isEqualTo(Document.parse("{ \"$year\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceMonth() {
		assertThat(transform("month($1)")).isEqualTo(Document.parse("{ \"$month\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceWeek() {
		assertThat(transform("week($1)")).isEqualTo(Document.parse("{ \"$week\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceHour() {
		assertThat(transform("hour($1)")).isEqualTo(Document.parse("{ \"$hour\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceMinute() {
		assertThat(transform("minute($1)")).isEqualTo(Document.parse("{ \"$minute\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceSecond() {
		assertThat(transform("second($1)")).isEqualTo(Document.parse("{ \"$second\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceMillisecond() {
		assertThat(transform("millisecond($1)")).isEqualTo(Document.parse("{ \"$millisecond\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceDateToString() {
		assertThat(transform("dateToString('%Y-%m-%d', $date)"))
				.isEqualTo(Document.parse("{ \"$dateToString\" : { \"format\" : \"%Y-%m-%d\" , \"date\" : \"$date\"}}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceCond() {
		assertThat(transform("cond(qty > 250, 30, 20)")).isEqualTo(
				Document.parse("{ \"$cond\" : { \"if\" : { \"$gt\" : [ \"$qty\" , 250]} , \"then\" : 30 , \"else\" : 20}}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeIfNull() {
		assertThat(transform("ifNull(a, 10)")).isEqualTo(Document.parse("{ \"$ifNull\" : [ \"$a\" , 10]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeSum() {
		assertThat(transform("sum(a, b)")).isEqualTo(Document.parse("{ \"$sum\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeAvg() {
		assertThat(transform("avg(a, b)")).isEqualTo(Document.parse("{ \"$avg\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceFirst() {
		assertThat(transform("first($1)")).isEqualTo(Document.parse("{ \"$first\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceLast() {
		assertThat(transform("last($1)")).isEqualTo(Document.parse("{ \"$last\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeMax() {
		assertThat(transform("max(a, b)")).isEqualTo(Document.parse("{ \"$max\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeMin() {
		assertThat(transform("min(a, b)")).isEqualTo(Document.parse("{ \"$min\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodePush() {
		assertThat(transform("push({'item':'$item', 'quantity':'$qty'})"))
				.isEqualTo(Document.parse("{ \"$push\" : { \"item\" : \"$item\" , \"quantity\" : \"$qty\"}}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceAddToSet() {
		assertThat(transform("addToSet($1)")).isEqualTo(Document.parse("{ \"$addToSet\" : \"$1\"}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeStdDevPop() {
		assertThat(transform("stdDevPop(scores.score)"))
				.isEqualTo(Document.parse("{ \"$stdDevPop\" : [ \"$scores.score\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderMethodReferenceNodeStdDevSamp() {
		assertThat(transform("stdDevSamp(age)")).isEqualTo(Document.parse("{ \"$stdDevSamp\" : [ \"$age\"]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeEq() {
		assertThat(transform("foo == 10")).isEqualTo(Document.parse("{ \"$eq\" : [ \"$foo\" , 10]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeNe() {
		assertThat(transform("foo != 10")).isEqualTo(Document.parse("{ \"$ne\" : [ \"$foo\" , 10]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeGt() {
		assertThat(transform("foo > 10")).isEqualTo(Document.parse("{ \"$gt\" : [ \"$foo\" , 10]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeGte() {
		assertThat(transform("foo >= 10")).isEqualTo(Document.parse("{ \"$gte\" : [ \"$foo\" , 10]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeLt() {
		assertThat(transform("foo < 10")).isEqualTo(Document.parse("{ \"$lt\" : [ \"$foo\" , 10]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeLte() {
		assertThat(transform("foo <= 10")).isEqualTo(Document.parse("{ \"$lte\" : [ \"$foo\" , 10]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodePow() {
		assertThat(transform("foo^2")).isEqualTo(Document.parse("{ \"$pow\" : [ \"$foo\" , 2]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeOr() {
		assertThat(transform("true || false")).isEqualTo(Document.parse("{ \"$or\" : [ true , false]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderComplexOperationNodeOr() {
		assertThat(transform("1+2 || concat(a, b) || true")).isEqualTo(
				Document.parse("{ \"$or\" : [ { \"$add\" : [ 1 , 2]} , { \"$concat\" : [ \"$a\" , \"$b\"]} , true]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderOperationNodeAnd() {
		assertThat(transform("true && false")).isEqualTo(Document.parse("{ \"$and\" : [ true , false]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderComplexOperationNodeAnd() {
		assertThat(transform("1+2 && concat(a, b) && true")).isEqualTo(
				Document.parse("{ \"$and\" : [ { \"$add\" : [ 1 , 2]} , { \"$concat\" : [ \"$a\" , \"$b\"]} , true]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderNotCorrectly() {
		assertThat(transform("!true")).isEqualTo(Document.parse("{ \"$not\" : [ true]}"));
	}

	@Test // DATAMONGO-1530
	void shouldRenderComplexNotCorrectly() {
		assertThat(transform("!(foo > 10)")).isEqualTo(Document.parse("{ \"$not\" : [ { \"$gt\" : [ \"$foo\" , 10]}]}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceIndexOfBytes() {
		assertThat(transform("indexOfBytes(item, 'foo')"))
				.isEqualTo(Document.parse("{ \"$indexOfBytes\" : [ \"$item\" , \"foo\"]}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceIndexOfCP() {
		assertThat(transform("indexOfCP(item, 'foo')"))
				.isEqualTo(Document.parse("{ \"$indexOfCP\" : [ \"$item\" , \"foo\"]}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceSplit() {
		assertThat(transform("split(item, ',')")).isEqualTo(Document.parse("{ \"$split\" : [ \"$item\" , \",\"]}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceStrLenBytes() {
		assertThat(transform("strLenBytes(item)")).isEqualTo(Document.parse("{ \"$strLenBytes\" : \"$item\"}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceStrLenCP() {
		assertThat(transform("strLenCP(item)")).isEqualTo(Document.parse("{ \"$strLenCP\" : \"$item\"}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodSubstrCP() {
		assertThat(transform("substrCP(item, 0, 5)")).isEqualTo(Document.parse("{ \"$substrCP\" : [ \"$item\" , 0 , 5]}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceReverseArray() {
		assertThat(transform("reverseArray(array)")).isEqualTo(Document.parse("{ \"$reverseArray\" : \"$array\"}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceReduce() {
		assertThat(transform("reduce(field, '', {'$concat':{'$$value','$$this'}})")).isEqualTo(Document.parse(
				"{ \"$reduce\" : { \"input\" : \"$field\" , \"initialValue\" : \"\" , \"in\" : { \"$concat\" : [ \"$$value\" , \"$$this\"]}}}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceZip() {
		assertThat(transform("zip(new String[]{'$array1', '$array2'})"))
				.isEqualTo(Document.parse("{ \"$zip\" : { \"inputs\" : [ \"$array1\" , \"$array2\"]}}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodReferenceZipWithOptionalArgs() {
		assertThat(transform("zip(new String[]{'$array1', '$array2'}, true, new int[]{1,2})")).isEqualTo(Document.parse(
				"{ \"$zip\" : { \"inputs\" : [ \"$array1\" , \"$array2\"] , \"useLongestLength\" : true , \"defaults\" : [ 1 , 2]}}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodIn() {
		assertThat(transform("in('item', array)")).isEqualTo(Document.parse("{ \"$in\" : [ \"item\" , \"$array\"]}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodRefereneIsoDayOfWeek() {
		assertThat(transform("isoDayOfWeek(date)")).isEqualTo(Document.parse("{ \"$isoDayOfWeek\" : \"$date\"}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodRefereneIsoWeek() {
		assertThat(transform("isoWeek(date)")).isEqualTo(Document.parse("{ \"$isoWeek\" : \"$date\"}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodRefereneIsoWeekYear() {
		assertThat(transform("isoWeekYear(date)")).isEqualTo(Document.parse("{ \"$isoWeekYear\" : \"$date\"}"));
	}

	@Test // DATAMONGO-1548
	void shouldRenderMethodRefereneType() {
		assertThat(transform("type(a)")).isEqualTo(Document.parse("{ \"$type\" : \"$a\"}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderArrayToObjectWithFieldReference() {
		assertThat(transform("arrayToObject(field)")).isEqualTo(Document.parse("{ \"$arrayToObject\" : \"$field\"}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderArrayToObjectWithArray() {

		assertThat(transform("arrayToObject(new String[]{'key', 'value'})"))
				.isEqualTo(Document.parse("{ \"$arrayToObject\" : [\"key\", \"value\"]}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderObjectToArrayWithFieldReference() {
		assertThat(transform("objectToArray(field)")).isEqualTo(Document.parse("{ \"$objectToArray\" : \"$field\"}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderMergeObjects() {

		assertThat(transform("mergeObjects(field1, $$ROOT)"))
				.isEqualTo(Document.parse("{ \"$mergeObjects\" : [\"$field1\", \"$$ROOT\"]}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderTrimWithoutChars() {
		assertThat(transform("trim(field)")).isEqualTo(Document.parse("{ \"$trim\" : {\"input\" : \"$field\"}}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderTrimWithChars() {

		assertThat(transform("trim(field, 'ie')"))
				.isEqualTo(Document.parse("{ \"$trim\" : {\"input\" : \"$field\", \"chars\" : \"ie\" }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderTrimWithCharsFromFieldReference() {

		assertThat(transform("trim(field1, field2)"))
				.isEqualTo(Document.parse("{ \"$trim\" : {\"input\" : \"$field1\", \"chars\" : \"$field2\" }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderLtrimWithoutChars() {
		assertThat(transform("ltrim(field)")).isEqualTo(Document.parse("{ \"$ltrim\" : {\"input\" : \"$field\"}}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderLtrimWithChars() {

		assertThat(transform("ltrim(field, 'ie')"))
				.isEqualTo(Document.parse("{ \"$ltrim\" : {\"input\" : \"$field\", \"chars\" : \"ie\" }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderLtrimWithCharsFromFieldReference() {

		assertThat(transform("ltrim(field1, field2)"))
				.isEqualTo(Document.parse("{ \"$ltrim\" : {\"input\" : \"$field1\", \"chars\" : \"$field2\" }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderRtrimWithoutChars() {
		assertThat(transform("rtrim(field)")).isEqualTo(Document.parse("{ \"$rtrim\" : {\"input\" : \"$field\"}}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderRtrimWithChars() {

		assertThat(transform("rtrim(field, 'ie')"))
				.isEqualTo(Document.parse("{ \"$rtrim\" : {\"input\" : \"$field\", \"chars\" : \"ie\" }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderRtrimWithCharsFromFieldReference() {

		assertThat(transform("rtrim(field1, field2)"))
				.isEqualTo(Document.parse("{ \"$rtrim\" : {\"input\" : \"$field1\", \"chars\" : \"$field2\" }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderConvertWithoutOptionalParameters() {

		assertThat(transform("convert(field, 'string')"))
				.isEqualTo(Document.parse("{ \"$convert\" : {\"input\" : \"$field\", \"to\" : \"string\" }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderConvertWithOnError() {

		assertThat(transform("convert(field, 'int', 'Not an integer.')")).isEqualTo(Document
				.parse("{ \"$convert\" : {\"input\" : \"$field\", \"to\" : \"int\", \"onError\" : \"Not an integer.\" }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderConvertWithOnErrorOnNull() {

		assertThat(transform("convert(field, 'int', 'Not an integer.', -1)")).isEqualTo(Document.parse(
				"{ \"$convert\" : {\"input\" : \"$field\", \"to\" : \"int\", \"onError\" : \"Not an integer.\", \"onNull\" : -1 }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderToBool() {
		assertThat(transform("toBool(field)")).isEqualTo(Document.parse("{ \"$toBool\" : \"$field\"}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderToDate() {
		assertThat(transform("toDate(field)")).isEqualTo(Document.parse("{ \"$toDate\" : \"$field\"}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderToDecimal() {
		assertThat(transform("toDecimal(field)")).isEqualTo(Document.parse("{ \"$toDecimal\" : \"$field\"}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderToDouble() {
		assertThat(transform("toDouble(field)")).isEqualTo(Document.parse("{ \"$toDouble\" : \"$field\"}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderToInt() {
		assertThat(transform("toInt(field)")).isEqualTo(Document.parse("{ \"$toInt\" : \"$field\"}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderToLong() {
		assertThat(transform("toLong(field)")).isEqualTo(Document.parse("{ \"$toLong\" : \"$field\"}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderToObjectId() {
		assertThat(transform("toObjectId(field)")).isEqualTo(Document.parse("{ \"$toObjectId\" : \"$field\"}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderToString() {
		assertThat(transform("toString(field)")).isEqualTo(Document.parse("{ \"$toString\" : \"$field\"}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateFromStringWithoutOptionalParameters() {

		assertThat(transform("dateFromString(field)"))
				.isEqualTo(Document.parse("{ \"$dateFromString\" : {\"dateString\" : \"$field\" }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateFromStringWithFormat() {

		assertThat(transform("dateFromString(field, 'DD-MM-YYYY')")).isEqualTo(
				Document.parse("{ \"$dateFromString\" : {\"dateString\" : \"$field\", \"format\" : \"DD-MM-YYYY\" }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateFromStringWithFormatAndTimezone() {

		assertThat(transform("dateFromString(field, 'DD-MM-YYYY', 'UTC')")).isEqualTo(Document.parse(
				"{ \"$dateFromString\" : {\"dateString\" : \"$field\", \"format\" : \"DD-MM-YYYY\", \"timezone\" : \"UTC\" }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateFromStringWithFormatTimezoneAndOnError() {

		assertThat(transform("dateFromString(field, 'DD-MM-YYYY', 'UTC', -1)")).isEqualTo(Document.parse(
				"{ \"$dateFromString\" : {\"dateString\" : \"$field\", \"format\" : \"DD-MM-YYYY\", \"timezone\" : \"UTC\", \"onError\" : -1 }}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateFromStringWithFormatTimezoneOnErrorAndOnNull() {

		assertThat(transform("dateFromString(field, 'DD-MM-YYYY', 'UTC', -1, -2)")).isEqualTo(Document.parse(
				"{ \"$dateFromString\" : {\"dateString\" : \"$field\", \"format\" : \"DD-MM-YYYY\", \"timezone\" : \"UTC\", \"onError\" : -1,  \"onNull\" : -2}}"));
	}

	@Test // DATAMONGO-2077, DATAMONGO-2671
	void shouldRenderDateFromParts() {

		assertThat(transform("dateFromParts(y, m, d, h, mm, s, ms, 'UTC')")).isEqualTo(Document.parse(
				"{ \"$dateFromParts\" : {\"year\" : \"$y\", \"month\" : \"$m\", \"day\" : \"$d\", \"hour\" : \"$h\",  \"minute\" : \"$mm\",  \"second\" : \"$s\", \"millisecond\" : \"$ms\", \"timezone\" : \"UTC\"}}"));
	}

	@Test // DATAMONGO-2077, DATAMONGO-2671
	void shouldRenderIsoDateFromParts() {

		assertThat(transform("isoDateFromParts(y, m, d, h, mm, s, ms, 'UTC')")).isEqualTo(Document.parse(
				"{ \"$dateFromParts\" : {\"isoWeekYear\" : \"$y\", \"isoWeek\" : \"$m\", \"isoDayOfWeek\" : \"$d\", \"hour\" : \"$h\",  \"minute\" : \"$mm\",  \"second\" : \"$s\", \"millisecond\" : \"$ms\", \"timezone\" : \"UTC\"}}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderDateToParts() {

		assertThat(transform("dateToParts(field, 'UTC', false)")).isEqualTo(
				Document.parse("{ \"$dateToParts\" : {\"date\" : \"$field\", \"timezone\" : \"UTC\", \"iso8601\" : false}}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderIndexOfArray() {

		assertThat(transform("indexOfArray(field, 2)"))
				.isEqualTo(Document.parse("{ \"$indexOfArray\" : [\"$field\", 2 ]}"));
	}

	@Test // DATAMONGO-2077
	void shouldRenderRange() {

		assertThat(transform("range(0, 10, 2)")).isEqualTo(Document.parse("{ \"$range\" : [0, 10, 2 ]}"));
	}

	@Test // DATAMONGO-2370
	void shouldRenderRound() {
		assertThat(transform("round(field)")).isEqualTo(Document.parse("{ \"$round\" : [\"$field\"]}"));
	}

	@Test // DATAMONGO-2370
	void shouldRenderRoundWithPlace() {
		assertThat(transform("round(field, 2)")).isEqualTo(Document.parse("{ \"$round\" : [\"$field\", 2]}"));
	}

	@Test // GH-3714
	void shouldRenderDegreesToRadians() {
		assertThat(transform("degreesToRadians(angle_a)")).isEqualTo(Document.parse("{ \"$degreesToRadians\" : \"$angle_a\"}"));
	}

	@Test // GH-3712
	void shouldRenderCovariancePop() {
		assertThat(transform("covariancePop(field1, field2)"))
				.isEqualTo(Document.parse("{ \"$covariancePop\" : [\"$field1\", \"$field2\"]}"));
	}

	@Test // GH-3712
	void shouldRenderCovarianceSamp() {
		assertThat(transform("covarianceSamp(field1, field2)"))
				.isEqualTo(Document.parse("{ \"$covarianceSamp\" : [\"$field1\", \"$field2\"]}"));
	}

	@Test // GH-3715
	void shouldRenderRank() {
		assertThat(transform("rank()")).isEqualTo(Document.parse("{ $rank : {} }"));
	}

	@Test // GH-3715
	void shouldRenderDenseRank() {
		assertThat(transform("denseRank()")).isEqualTo(Document.parse("{ $denseRank : {} }"));
	}

	@Test // GH-3717
	void shouldRenderDocumentNumber() {
		assertThat(transform("documentNumber()")).isEqualTo(Document.parse("{ $documentNumber : {} }"));
	}

	@Test // GH-3727
	void rendersShift() {

		assertThat(transform("shift(quantity, 1)"))
				.isEqualTo(Document.parse("{ $shift: { output: \"$quantity\", by: 1 } }"));
	}

	@Test // GH-3727
	void rendersShiftWithDefault() {

		assertThat(transform("shift(quantity, 1, 'Not available')"))
				.isEqualTo(Document.parse("{ $shift: { output: \"$quantity\", by: 1, default: \"Not available\" } }"));
	}

	@Test // GH-3716
	void shouldRenderDerivative() {
		assertThat(transform("derivative(miles, 'hour')"))
				.isEqualTo(Document.parse("{ \"$derivative\" : { input : '$miles', unit : 'hour'} }"));
	}

	@Test // GH-3721
	void shouldRenderIntegral() {
		assertThat(transform("integral(field)")).isEqualTo(Document.parse("{ \"$integral\" : { \"input\" : \"$field\" }}"));
	}

	@Test // GH-3721
	void shouldRenderIntegralWithUnit() {
		assertThat(transform("integral(field, 'hour')"))
				.isEqualTo(Document.parse("{ \"$integral\" : { \"input\" : \"$field\", \"unit\" : \"hour\" }}"));
	}

	@Test // GH-3728
	void shouldRenderSin() {
		assertThat(transform("sin(angle)")).isEqualTo(Document.parse("{ \"$sin\" : \"$angle\"}"));
	}

	@Test // GH-3728
	void shouldRenderSinh() {
		assertThat(transform("sinh(angle)")).isEqualTo(Document.parse("{ \"$sinh\" : \"$angle\"}"));
	}

	@Test // GH-3730
	void shouldRenderTan() {
		assertThat(transform("tan(angle)")).isEqualTo(Document.parse("{ \"$tan\" : \"$angle\"}"));
	}

	@Test // GH-3730
	void shouldRenderTanh() {
		assertThat(transform("tanh(angle)")).isEqualTo(Document.parse("{ \"$tanh\" : \"$angle\"}"));
	}

	private Object transform(String expression, Object... params) {
		Object result = transformer.transform(expression, Aggregation.DEFAULT_CONTEXT, params);
		return result == null ? null : (!(result instanceof org.bson.Document) ? result.toString() : result);
	}
}
