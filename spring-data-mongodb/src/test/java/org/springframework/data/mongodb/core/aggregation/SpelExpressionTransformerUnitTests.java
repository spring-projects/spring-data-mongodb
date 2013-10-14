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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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
		transform("a ^ 1");
	}

	@Test
	public void shouldRenderSumExpression() {
		assertThat(transform("a + 1"), is("{ \"$add\" : [ \"$a\" , 1]}"));
	}

	@Test
	public void shouldRenderFormula() {

		assertThat(
				transform("(netPrice + surCharge) * taxrate + 42"),
				is("{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}"));
	}

	@Test
	public void shouldRenderFormulaInCurlyBrackets() {

		assertThat(
				transform("{(netPrice + surCharge) * taxrate + 42}"),
				is("{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}"));
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

		assertThat(
				transform("(q + 1 + 4 - 5) / (q + 1 + 3 + 4)"),
				is("{ \"$divide\" : [ { \"$subtract\" : [ { \"$add\" : [ \"$q\" , 1 , 4]} , 5]} , { \"$add\" : [ \"$q\" , 1 , 3 , 4]}]}"));
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

	@Test
	public void shouldRenderStringFunctions() {

		assertThat(transform("concat(a, b)"), is("{ \"$concat\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transform("substr(a, 1, 2)"), is("{ \"$substr\" : [ \"$a\" , 1 , 2]}"));
		assertThat(transform("strcasecmp(a, b)"), is("{ \"$strcasecmp\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transform("toLower(a)"), is("{ \"$toLower\" : [ \"$a\"]}"));
		assertThat(transform("toUpper(a)"), is("{ \"$toUpper\" : [ \"$a\"]}"));
		assertThat(transform("toUpper(toLower(a))"), is("{ \"$toUpper\" : [ { \"$toLower\" : [ \"$a\"]}]}"));
	}

	private String transform(String expression, Object... params) {
		Object result = transformer.transform(expression, Aggregation.DEFAULT_CONTEXT, params);
		return result == null ? null : result.toString();
	}
}
