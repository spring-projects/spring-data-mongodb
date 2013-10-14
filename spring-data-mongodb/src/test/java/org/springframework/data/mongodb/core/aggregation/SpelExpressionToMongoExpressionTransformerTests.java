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

import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit tests for {@link SpelExpressionToMongoExpressionTransformer}.
 * 
 * @see DATAMONGO-774
 * @author Thomas Darimont
 */
public class SpelExpressionToMongoExpressionTransformerTests {

	SpelExpressionToMongoExpressionTransformer transformer = SpelExpressionToMongoExpressionTransformer.INSTANCE;

	@Test
	public void shouldRenderConstantExpression() {

		assertThat(transformer.transform("1").toString(), is("1"));
		assertThat(transformer.transform("-1").toString(), is("-1"));
		assertThat(transformer.transform("1.0").toString(), is("1.0"));
		assertThat(transformer.transform("-1.0").toString(), is("-1.0"));
	}

	@Test
	public void shouldSupportKnownOperands() {

		assertThat(transformer.transform("a + b").toString(), is("{ \"$add\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transformer.transform("a - b").toString(), is("{ \"$subtract\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transformer.transform("a * b").toString(), is("{ \"$multiply\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transformer.transform("a / b").toString(), is("{ \"$divide\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transformer.transform("a % b").toString(), is("{ \"$mod\" : [ \"$a\" , \"$b\"]}"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionOnUnknownOperand() {
		transformer.transform("a ^ 1");
	}

	@Test
	public void shouldRenderSumExpression() {
		assertThat(transformer.transform("a + 1").toString(), is("{ \"$add\" : [ \"$a\" , 1]}"));
	}

	@Test
	public void shouldRenderFormula() {

		assertThat(
				transformer.transform("(netPrice + surCharge) * taxrate + 42").toString(),
				is("{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}"));
	}

	@Test
	public void shouldRenderFormulaInCurlyBrackets() {

		assertThat(
				transformer.transform("{(netPrice + surCharge) * taxrate + 42}").toString(),
				is("{ \"$add\" : [ { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\"]} , 42]}"));
	}

	@Test
	public void shouldRenderFieldReference() {

		assertThat(transformer.transform("foo").toString(), is("$foo"));
		assertThat(transformer.transform("$foo").toString(), is("$foo"));
	}

	@Test
	public void shouldRenderNestedFieldReference() {

		assertThat(transformer.transform("foo.bar").toString(), is("$foo.bar"));
		assertThat(transformer.transform("$foo.bar").toString(), is("$foo.bar"));
	}

	@Test
	@Ignore
	public void shouldRenderNestedIndexedFieldReference() {

		// TODO add support for rendering nested indexed field references
		assertThat(transformer.transform("foo[3].bar").toString(), is("$foo[3].bar"));
	}

	@Test
	public void shouldRenderConsecutiveOperation() {
		assertThat(transformer.transform("1 + 1 + 1").toString(), is("{ \"$add\" : [ 1 , 1 , 1]}"));
	}

	@Test
	public void shouldRenderComplexExpression0() {

		assertThat(transformer.transform("-(1 + q)").toString(),
				is("{ \"$multiply\" : [ -1 , { \"$add\" : [ 1 , \"$q\"]}]}"));
	}

	@Test
	public void shouldRenderComplexExpression1() {

		assertThat(transformer.transform("1 + (q + 1) / (q - 1)").toString(),
				is("{ \"$add\" : [ 1 , { \"$divide\" : [ { \"$add\" : [ \"$q\" , 1]} , { \"$subtract\" : [ \"$q\" , 1]}]}]}"));
	}

	@Test
	public void shouldRenderComplexExpression2() {

		assertThat(
				transformer.transform("(q + 1 + 4 - 5) / (q + 1 + 3 + 4)").toString(),
				is("{ \"$divide\" : [ { \"$subtract\" : [ { \"$add\" : [ \"$q\" , 1 , 4]} , 5]} , { \"$add\" : [ \"$q\" , 1 , 3 , 4]}]}"));
	}

	@Test
	public void shouldRenderBinaryExpressionWithMixedSignsCorrectly() {

		assertThat(transformer.transform("-4 + 1").toString(), is("{ \"$add\" : [ -4 , 1]}"));
		assertThat(transformer.transform("1 + -4").toString(), is("{ \"$add\" : [ 1 , -4]}"));
	}

	@Test
	public void shouldRenderConsecutiveOperationsInComplexExpression() {

		assertThat(transformer.transform("1 + 1 + (1 + 1 + 1) / q").toString(),
				is("{ \"$add\" : [ 1 , 1 , { \"$divide\" : [ { \"$add\" : [ 1 , 1 , 1]} , \"$q\"]}]}"));
	}

	@Test
	public void shouldRenderParameterExpressionResults() {
		assertThat(transformer.transform("[0] + [1] + [2]", 1, 2, 3).toString(), is("{ \"$add\" : [ 1 , 2 , 3]}"));
	}

	@Test
	public void shouldRenderNestedParameterExpressionResults() {

		assertThat(transformer.transform("[0].value1 + [0].value2 + [0].value3.longValue()", new Data()).toString(),
				is("{ \"$add\" : [ 42 , 1.2345 , 23]}"));
	}

	@Test
	public void shouldRenderStringFunctions() {

		assertThat(transformer.transform("concat(a, b)").toString(), is("{ \"$concat\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transformer.transform("substr(a, 1, 2)").toString(), is("{ \"$substr\" : [ \"$a\" , 1 , 2]}"));
		assertThat(transformer.transform("strcasecmp(a, b)").toString(), is("{ \"$strcasecmp\" : [ \"$a\" , \"$b\"]}"));
		assertThat(transformer.transform("toLower(a)").toString(), is("{ \"$toLower\" : [ \"$a\"]}"));
		assertThat(transformer.transform("toUpper(a)").toString(), is("{ \"$toUpper\" : [ \"$a\"]}"));
		assertThat(transformer.transform("toUpper(toLower(a))").toString(),
				is("{ \"$toUpper\" : [ { \"$toLower\" : [ \"$a\"]}]}"));
	}
}
