/*
 * Copyright 2013-2014 the original author or authors.
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
 * @see DATAMONGO-774
 * @author Thomas Darimont
 * @author Oliver Gierke
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

		assertThat(transform("1"), is((Object) "1"));
		assertThat(transform("-1"), is((Object) "-1"));
		assertThat(transform("1.0"), is((Object) "1.0"));
		assertThat(transform("-1.0"), is((Object) "-1.0"));
		assertThat(transform("null"), is(nullValue()));
	}

	@Test
	public void shouldSupportKnownOperands() {

		assertThat(transform("a + b"), is((Object) Document.parse("{ \"$add\" : [ \"$a\" , \"$b\"]}")));
		assertThat(transform("a - b"), is((Object) Document.parse("{ \"$subtract\" : [ \"$a\" , \"$b\"]}")));
		assertThat(transform("a * b"), is((Object) Document.parse("{ \"$multiply\" : [ \"$a\" , \"$b\"]}")));
		assertThat(transform("a / b"), is((Object) Document.parse("{ \"$divide\" : [ \"$a\" , \"$b\"]}")));
		assertThat(transform("a % b"), is((Object) Document.parse("{ \"$mod\" : [ \"$a\" , \"$b\"]}")));
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionOnUnknownOperand() {
		transform("a ^ 1");
	}

	@Test
	public void shouldRenderSumExpression() {
		assertThat(transform("a + 1"), is((Object) Document.parse("{ \"$add\" : [ \"$a\" , 1]}")));
	}

	@Test
	public void shouldRenderFormula() {

		assertThat(transform("(netPrice + surCharge) * taxrate + 42"), is((Object) Document.parse(
				"{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}")));
	}

	@Test
	public void shouldRenderFormulaInCurlyBrackets() {

		assertThat(transform("{(netPrice + surCharge) * taxrate + 42}"), is((Object) Document.parse(
				"{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}")));
	}

	@Test
	public void shouldRenderFieldReference() {

		assertThat(transform("foo"), is((Object) "$foo"));
		assertThat(transform("$foo"), is((Object) "$foo"));
	}

	@Test
	public void shouldRenderNestedFieldReference() {

		assertThat(transform("foo.bar"), is((Object) "$foo.bar"));
		assertThat(transform("$foo.bar"), is((Object) "$foo.bar"));
	}

	@Test
	@Ignore
	public void shouldRenderNestedIndexedFieldReference() {

		// TODO add support for rendering nested indexed field references
		assertThat(transform("foo[3].bar"), is((Object) "$foo[3].bar"));
	}

	@Test
	public void shouldRenderConsecutiveOperation() {
		assertThat(transform("1 + 1 + 1"), is((Object) Document.parse("{ \"$add\" : [ 1 , 1 , 1]}")));
	}

	@Test
	public void shouldRenderComplexExpression0() {

		assertThat(transform("-(1 + q)"),
				is((Object) Document.parse("{ \"$multiply\" : [ -1 , { \"$add\" : [ 1 , \"$q\"]}]}")));
	}

	@Test
	public void shouldRenderComplexExpression1() {

		assertThat(transform("1 + (q + 1) / (q - 1)"), is((Object) Document.parse(
				"{ \"$add\" : [ 1 , { \"$divide\" : [ { \"$add\" : [ \"$q\" , 1]} , { \"$subtract\" : [ \"$q\" , 1]}]}]}")));
	}

	@Test
	public void shouldRenderComplexExpression2() {

		assertThat(transform("(q + 1 + 4 - 5) / (q + 1 + 3 + 4)"), is((Object) Document.parse(
				"{ \"$divide\" : [ { \"$subtract\" : [ { \"$add\" : [ \"$q\" , 1 , 4]} , 5]} , { \"$add\" : [ \"$q\" , 1 , 3 , 4]}]}")));
	}

	@Test
	public void shouldRenderBinaryExpressionWithMixedSignsCorrectly() {

		assertThat(transform("-4 + 1"), is((Object) Document.parse("{ \"$add\" : [ -4 , 1]}")));
		assertThat(transform("1 + -4"), is((Object) Document.parse("{ \"$add\" : [ 1 , -4]}")));
	}

	@Test
	public void shouldRenderConsecutiveOperationsInComplexExpression() {

		assertThat(transform("1 + 1 + (1 + 1 + 1) / q"), is(
				(Object) Document.parse("{ \"$add\" : [ 1 , 1 , { \"$divide\" : [ { \"$add\" : [ 1 , 1 , 1]} , \"$q\"]}]}")));
	}

	@Test
	public void shouldRenderParameterExpressionResults() {
		assertThat(transform("[0] + [1] + [2]", 1, 2, 3), is((Object) Document.parse("{ \"$add\" : [ 1 , 2 , 3]}")));
	}

	@Test
	@Ignore("TODO: mongo3 renders this a bit strange")
	public void shouldRenderNestedParameterExpressionResults() {

		assertThat(
				((Document) transform("[0].primitiveLongValue + [0].primitiveDoubleValue + [0].doubleValue.longValue()", data))
						.toJson(),
				is(Document.parse("{ \"$add\" : [ 42 , 1.2345 , 23]}").toJson()));
	}

	@Test
	@Ignore("TODO: mongo3 renders this a bit strange")
	public void shouldRenderNestedParameterExpressionResultsInNestedExpressions() {

		assertThat(
				((Document) transform("((1 + [0].primitiveLongValue) + [0].primitiveDoubleValue) * [0].doubleValue.longValue()",
						data)).toJson(),
				is(new Document("$multiply", Arrays.asList(new Document("$add", Arrays.asList(1, 42L, 1.2345D, 23L))))
						.toJson()));
	}

	/**
	 * @see DATAMONGO-840
	 */
	@Test
	public void shouldRenderCompoundExpressionsWithIndexerAndFieldReference() {

		Person person = new Person();
		person.setAge(10);
		assertThat(transform("[0].age + a.c", person), is((Object) Document.parse("{ \"$add\" : [ 10 , \"$a.c\"] }")));
	}

	/**
	 * @see DATAMONGO-840
	 */
	@Test
	public void shouldRenderCompoundExpressionsWithOnlyFieldReferences() {

		assertThat(transform("a.b + a.c"), is((Object) Document.parse("{ \"$add\" : [ \"$a.b\" , \"$a.c\"]}")));
	}

	@Test
	public void shouldRenderStringFunctions() {

		assertThat(transform("concat(a, b)"), is((Object) Document.parse("{ \"$concat\" : [ \"$a\" , \"$b\"]}")));
		assertThat(transform("substr(a, 1, 2)"), is((Object) Document.parse("{ \"$substr\" : [ \"$a\" , 1 , 2]}")));
		assertThat(transform("strcasecmp(a, b)"), is((Object) Document.parse("{ \"$strcasecmp\" : [ \"$a\" , \"$b\"]}")));
		assertThat(transform("toLower(a)"), is((Object) Document.parse("{ \"$toLower\" : [ \"$a\"]}")));
		assertThat(transform("toUpper(a)"), is((Object) Document.parse("{ \"$toUpper\" : [ \"$a\"]}")));
		assertThat(transform("toUpper(toLower(a))"),
				is((Object) Document.parse("{ \"$toUpper\" : [ { \"$toLower\" : [ \"$a\"]}]}")));
	}

	private Object transform(String expression, Object... params) {
		Object result = transformer.transform(expression, Aggregation.DEFAULT_CONTEXT, params);
		return result == null ? null : (!(result instanceof org.bson.Document) ? result.toString() : result);
	}
}
