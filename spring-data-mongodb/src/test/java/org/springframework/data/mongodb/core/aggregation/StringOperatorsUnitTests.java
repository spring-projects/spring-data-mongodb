/*
 * Copyright 2018-2023 the original author or authors.
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

import java.util.regex.Pattern;

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link StringOperators}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Divya Srivastava
 * @currentRead Royal Assassin - Robin Hobb
 */
class StringOperatorsUnitTests {

	private static final String EXPRESSION_STRING = "{ \"$fitz\" : \"chivalry\" }";
	private static final Document EXPRESSION_DOC = Document.parse(EXPRESSION_STRING);
	private static final AggregationExpression EXPRESSION = context -> EXPRESSION_DOC;

	@Test // DATAMONGO-2049
	void shouldRenderTrim() {

		assertThat(StringOperators.valueOf("shrewd").trim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $trim: { \"input\" : \"$shrewd\" } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderTrimForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).trim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $trim: { \"input\" : " + EXPRESSION_STRING + " } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderTrimWithChars() {

		assertThat(StringOperators.valueOf("shrewd").trim("sh").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $trim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderTrimWithCharsExpression() {

		assertThat(StringOperators.valueOf("shrewd").trim(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $trim: { \"input\" : \"$shrewd\", \"chars\" : " + EXPRESSION_STRING + " } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderTrimLeft() {

		assertThat(StringOperators.valueOf("shrewd").trim().left().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $ltrim: { \"input\" : \"$shrewd\" } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderTrimLeftWithChars() {

		assertThat(StringOperators.valueOf("shrewd").trim("sh").left().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $ltrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderTrimRight() {

		assertThat(StringOperators.valueOf("shrewd").trim().right().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $rtrim: { \"input\" : \"$shrewd\" } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderTrimRightWithChars() {

		assertThat(StringOperators.valueOf("shrewd").trim("sh").right().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $rtrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderLTrim() {

		assertThat(StringOperators.valueOf("shrewd").ltrim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $ltrim: { \"input\" : \"$shrewd\" } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderLTrimForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).ltrim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $ltrim: { \"input\" : " + EXPRESSION_STRING + " } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderLTrimWithChars() {

		assertThat(StringOperators.valueOf("shrewd").ltrim("sh").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $ltrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderLTrimWithCharsExpression() {

		assertThat(StringOperators.valueOf("shrewd").ltrim(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $ltrim: { \"input\" : \"$shrewd\", \"chars\" : " + EXPRESSION_STRING + " } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderRTrim() {

		assertThat(StringOperators.valueOf("shrewd").rtrim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $rtrim: { \"input\" : \"$shrewd\" } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderRTrimForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).rtrim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $rtrim: { \"input\" : " + EXPRESSION_STRING + " } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderRTrimWithChars() {

		assertThat(StringOperators.valueOf("shrewd").rtrim("sh").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $rtrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } ");
	}

	@Test // DATAMONGO-2049
	void shouldRenderRTrimWithCharsExpression() {

		assertThat(StringOperators.valueOf("shrewd").rtrim(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $rtrim: { \"input\" : \"$shrewd\", \"chars\" : " + EXPRESSION_STRING + " } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexFindAll() {

		assertThat(StringOperators.valueOf("shrewd").regexFindAll("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexFindAll: { \"input\" : \"$shrewd\" , \"regex\" : \"e\" } }");
	}

	@Test // GH-3725
	void shouldRenderRegexFindAllForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).regexFindAll("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexFindAll: { \"input\" : " + EXPRESSION_STRING + " , \"regex\" : \"e\" } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexFindAllForRegexExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexFindAll(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexFindAll: { \"input\" : \"$shrewd\" , \"regex\" : " + EXPRESSION_STRING + " } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexFindAllWithPattern() {

		assertThat(StringOperators.valueOf("shrewd")
				.regexFindAll(
						Pattern.compile("foo", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL | Pattern.COMMENTS))
				.toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo("{ $regexFindAll: { \"input\" : \"$shrewd\", \"regex\" : \"foo\" , \"options\" : \"imsx\" } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexFindAllWithOptions() {

		assertThat(StringOperators.valueOf("shrewd").regexFindAll("e").options("i").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexFindAll: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : \"i\" } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexFindAllWithOptionsExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexFindAll("e").optionsOf(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexFindAll: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : " + EXPRESSION_STRING
						+ " } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexMatch() {

		assertThat(StringOperators.valueOf("shrewd").regexMatch("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexMatch: { \"input\" : \"$shrewd\" , \"regex\" : \"e\" } }");
	}

	@Test // GH-3725
	void shouldRenderRegexMatchForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).regexMatch("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexMatch: { \"input\" : " + EXPRESSION_STRING + " , \"regex\" : \"e\" } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexMatchForRegexExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexMatch(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexMatch: { \"input\" : \"$shrewd\" , \"regex\" : " + EXPRESSION_STRING + " } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexMatchForPattern() {

		assertThat(StringOperators.valueOf("shrewd").regexMatch(Pattern.compile("foo", Pattern.CASE_INSENSITIVE))
				.toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo("{ $regexMatch: { \"input\" : \"$shrewd\" , \"regex\" : \"foo\", \"options\" : \"i\"} } ");
	}

	@Test // GH-3725
	void shouldRenderRegexMatchWithOptions() {

		assertThat(StringOperators.valueOf("shrewd").regexMatch("e").options("i").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexMatch: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : \"i\" } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexMatchWithOptionsExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexMatch("e").optionsOf(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexMatch: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : " + EXPRESSION_STRING
						+ " } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexFind() {

		assertThat(StringOperators.valueOf("shrewd").regexFind("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexFind: { \"input\" : \"$shrewd\" , \"regex\" : \"e\" } }");
	}

	@Test // GH-3725
	void shouldRenderRegexFindForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).regexFind("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexFind: { \"input\" : " + EXPRESSION_STRING + " , \"regex\" : \"e\" } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexFindForRegexExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexFind(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexFind: { \"input\" : \"$shrewd\" , \"regex\" : " + EXPRESSION_STRING + " } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexFindForPattern() {

		assertThat(StringOperators.valueOf("shrewd").regexFind(Pattern.compile("foo", Pattern.MULTILINE))
				.toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo("{ $regexFind: { \"input\" : \"$shrewd\" , \"regex\" : \"foo\", \"options\" : \"m\"} } ");
	}

	@Test // GH-3725
	void shouldRenderRegexFindWithOptions() {

		assertThat(StringOperators.valueOf("shrewd").regexFind("e").options("i").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexFind: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : \"i\" } } ");
	}

	@Test // GH-3725
	void shouldRenderRegexFindWithOptionsExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexFind("e").optionsOf(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $regexFind: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : " + EXPRESSION_STRING
						+ " } } ");
	}
	
	@Test // GH-3695
	void shouldRenderReplaceOne() {

		assertThat(StringOperators.valueOf("bar").replaceOne("foobar","baz").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $replaceOne : {\"find\" : \"foobar\", \"input\" : \"$bar\", \"replacement\" : \"baz\"}}");
	}

	@Test // GH-3695
	void shouldRenderReplaceOneForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).replaceOne("a","s").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $replaceOne : {\"find\" : \"a\", \"input\" : " + EXPRESSION_STRING + ", \"replacement\" : \"s\"}}");
	}
	
	@Test // GH-3695
	void shouldRenderReplaceAll() {

		assertThat(StringOperators.valueOf("bar").replaceAll("foobar","baz").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $replaceAll : {\"find\" : \"foobar\", \"input\" : \"$bar\", \"replacement\" : \"baz\"}}");
	}

	@Test // GH-3695
	void shouldRenderReplaceAllForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).replaceAll("a","s").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo("{ $replaceAll : {\"find\" : \"a\", \"input\" : " + EXPRESSION_STRING + ", \"replacement\" : \"s\"}}");
	}
}
