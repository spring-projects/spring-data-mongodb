/*
 * Copyright 2018-2021 the original author or authors.
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

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link StringOperators}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @currentRead Royal Assassin - Robin Hobb
 */
public class StringOperatorsUnitTests {

	static final String EXPRESSION_STRING = "{ \"$fitz\" : \"chivalry\" }";
	static final Document EXPRESSION_DOC = Document.parse(EXPRESSION_STRING);
	static final AggregationExpression EXPRESSION = context -> EXPRESSION_DOC;

	@Test // DATAMONGO-2049
	public void shouldRenderTrim() {

		assertThat(StringOperators.valueOf("shrewd").trim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $trim: { \"input\" : \"$shrewd\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).trim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $trim: { \"input\" : " + EXPRESSION_STRING + " } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimWithChars() {

		assertThat(StringOperators.valueOf("shrewd").trim("sh").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $trim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimWithCharsExpression() {

		assertThat(StringOperators.valueOf("shrewd").trim(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $trim: { \"input\" : \"$shrewd\", \"chars\" : " + EXPRESSION_STRING + " } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimLeft() {

		assertThat(StringOperators.valueOf("shrewd").trim().left().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $ltrim: { \"input\" : \"$shrewd\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimLeftWithChars() {

		assertThat(StringOperators.valueOf("shrewd").trim("sh").left().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $ltrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimRight() {

		assertThat(StringOperators.valueOf("shrewd").trim().right().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $rtrim: { \"input\" : \"$shrewd\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimRightWithChars() {

		assertThat(StringOperators.valueOf("shrewd").trim("sh").right().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $rtrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderLTrim() {

		assertThat(StringOperators.valueOf("shrewd").ltrim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $ltrim: { \"input\" : \"$shrewd\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderLTrimForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).ltrim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $ltrim: { \"input\" : " + EXPRESSION_STRING + " } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderLTrimWithChars() {

		assertThat(StringOperators.valueOf("shrewd").ltrim("sh").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $ltrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderLTrimWithCharsExpression() {

		assertThat(StringOperators.valueOf("shrewd").ltrim(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $ltrim: { \"input\" : \"$shrewd\", \"chars\" : " + EXPRESSION_STRING + " } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderRTrim() {

		assertThat(StringOperators.valueOf("shrewd").rtrim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $rtrim: { \"input\" : \"$shrewd\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderRTrimForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).rtrim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $rtrim: { \"input\" : " + EXPRESSION_STRING + " } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderRTrimWithChars() {

		assertThat(StringOperators.valueOf("shrewd").rtrim("sh").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $rtrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderRTrimWithCharsExpression() {

		assertThat(StringOperators.valueOf("shrewd").rtrim(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $rtrim: { \"input\" : \"$shrewd\", \"chars\" : " + EXPRESSION_STRING + " } } "));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexFindAll() {

		assertThat(StringOperators.valueOf("shrewd").regexFindAll("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexFindAll: { \"input\" : \"$shrewd\" , \"regex\" : \"e\" } }"));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexFindAllForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).regexFindAll("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexFindAll: { \"input\" : " + EXPRESSION_STRING + " , \"regex\" : \"e\" } } "));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexFindAllForRegexExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexFindAll(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexFindAll: { \"input\" : \"$shrewd\" , \"regex\" : " + EXPRESSION_STRING + " } } "));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexFindAllWithOptions() {

		assertThat(StringOperators.valueOf("shrewd").regexFindAll("e").options("i").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexFindAll: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : \"i\" } } "));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexFindAllWithOptionsExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexFindAll("e").optionsOf(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexFindAll: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : " + EXPRESSION_STRING + " } } "));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexMatch() {

		assertThat(StringOperators.valueOf("shrewd").regexMatch("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexMatch: { \"input\" : \"$shrewd\" , \"regex\" : \"e\" } }"));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexMatchForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).regexMatch("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexMatch: { \"input\" : " + EXPRESSION_STRING + " , \"regex\" : \"e\" } } "));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexMatchForRegexExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexMatch(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexMatch: { \"input\" : \"$shrewd\" , \"regex\" : " + EXPRESSION_STRING + " } } "));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexMatchWithOptions() {

		assertThat(StringOperators.valueOf("shrewd").regexMatch("e").options("i").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexMatch: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : \"i\" } } "));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexMatchWithOptionsExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexMatch("e").optionsOf(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexMatch: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : " + EXPRESSION_STRING + " } } "));
	}

	@Test // DATAMONGO - 3725
	public void shouldRenderRegexFind() {

		assertThat(StringOperators.valueOf("shrewd").regexFind("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexFind: { \"input\" : \"$shrewd\" , \"regex\" : \"e\" } }"));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexFindForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).regexFind("e").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexFind: { \"input\" : " + EXPRESSION_STRING + " , \"regex\" : \"e\" } } "));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexFindForRegexExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexFind(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexFind: { \"input\" : \"$shrewd\" , \"regex\" : " + EXPRESSION_STRING + " } } "));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexFindWithOptions() {

		assertThat(StringOperators.valueOf("shrewd").regexFind("e").options("i").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexFind: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : \"i\" } } "));
	}
	
	@Test // DATAMONGO - 3725
	public void shouldRenderRegexFindWithOptionsExpression() {

		assertThat(StringOperators.valueOf("shrewd").regexFind("e").optionsOf(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $regexFind: { \"input\" : \"$shrewd\", \"regex\" : \"e\" , \"options\" : " + EXPRESSION_STRING + " } } "));
	}


}
