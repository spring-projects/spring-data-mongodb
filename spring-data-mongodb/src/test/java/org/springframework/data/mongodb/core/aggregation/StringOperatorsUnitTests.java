/*
 * Copyright 2018 the original author or authors.
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

import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.Test;

/**
 * Unit test for {@link StringOperators}.
 *
 * @author Christoph Strobl
 * @currentRead Royal Assassin - Robin Hobb
 */
public class StringOperatorsUnitTests {

	static final String EXPRESSION_STRING = "{ \"$fitz\" : \"chivalry\" }";
	static final Document EXPRESSION_DOC = Document.parse(EXPRESSION_STRING);
	static final AggregationExpression EXPRESSION = context -> EXPRESSION_DOC;

	@Test // DATAMONGO-2049
	public void shouldRenderLTrim() {

		Assertions.assertThat(StringOperators.valueOf("shrewd").ltrim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $ltrim: { \"input\" : \"$shrewd\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderLTrimForExpression() {

		Assertions.assertThat(StringOperators.valueOf(EXPRESSION).ltrim().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $ltrim: { \"input\" : " + EXPRESSION_STRING + " } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderLTrimWithChars() {

		Assertions.assertThat(StringOperators.valueOf("shrewd").ltrim("sh").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $ltrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } "));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderLTrimWithCharsExpression() {

		Assertions.assertThat(StringOperators.valueOf("shrewd").ltrim(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $ltrim: { \"input\" : \"$shrewd\", \"chars\" : " + EXPRESSION_STRING + " } } "));
	}

}
