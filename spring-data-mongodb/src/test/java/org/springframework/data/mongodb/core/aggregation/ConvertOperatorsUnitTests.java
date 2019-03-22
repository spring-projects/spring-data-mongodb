/*
 * Copyright 2018-2019 the original author or authors.
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
import org.junit.Test;

/**
 * Unit tests for {@link ConvertOperators}.
 *
 * @author Christoph Strobl
 * @currentRead Royal Assassin - Robin Hobb
 */
public class ConvertOperatorsUnitTests {

	static final String EXPRESSION_STRING = "{ \"$molly\" : \"chandler\" }";
	static final Document EXPRESSION_DOC = Document.parse(EXPRESSION_STRING);
	static final AggregationExpression EXPRESSION = context -> EXPRESSION_DOC;

	@Test // DATAMONGO-2048
	public void convertToUsingStringIdentifier() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\" } } "));
	}

	@Test // DATAMONGO-2048
	public void convertToUsingIntIdentifier() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo(1).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : 1 } } "));
	}

	@Test // DATAMONGO-2048
	public void convertToUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToTypeOf("fitz").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"$fitz\" } } "));
	}

	@Test // DATAMONGO-2048
	public void convertToUsingExpression() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToTypeOf(EXPRESSION).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : " + EXPRESSION_STRING + " } } "));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnErrorValue() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onErrorReturn("foo")
				.toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(
						Document.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onError\" : \"foo\" } } "));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnErrorValueOfField() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onErrorReturnValueOf("verity")
				.toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo(Document
								.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onError\" : \"$verity\" } } "));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnErrorValueOfExpression() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onErrorReturnValueOf(EXPRESSION)
				.toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo(Document.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onError\" : "
								+ EXPRESSION_STRING + " } } "));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnNullValue() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onNullReturn("foo")
				.toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(
						Document.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onNull\" : \"foo\" } } "));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnNullValueOfField() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onNullReturnValueOf("verity")
				.toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo(Document
								.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onNull\" : \"$verity\" } } "));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnNullValueOfExpression() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onNullReturnValueOf(EXPRESSION)
				.toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(Document.parse(
						"{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onNull\" : " + EXPRESSION_STRING + " } } "));
	}

	@Test // DATAMONGO-2048
	public void toBoolUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToBoolean().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toBool: \"$shrewd\" } "));
	}

	@Test // DATAMONGO-2048
	public void toBoolUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToBoolean().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toBool: " + EXPRESSION_STRING + " } "));
	}

	@Test // DATAMONGO-2048
	public void toDateUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToDate().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toDate: \"$shrewd\" } "));
	}

	@Test // DATAMONGO-2048
	public void toDateUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToDate().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toDate: " + EXPRESSION_STRING + " } "));
	}

	@Test // DATAMONGO-2048
	public void toDecimalUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToDecimal().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toDecimal: \"$shrewd\" } "));
	}

	@Test // DATAMONGO-2048
	public void toDecimalUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToDecimal().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toDecimal: " + EXPRESSION_STRING + " } "));
	}

	@Test // DATAMONGO-2048
	public void toDoubleUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToDouble().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toDouble: \"$shrewd\" } "));
	}

	@Test // DATAMONGO-2048
	public void toDoubleUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToDouble().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toDouble: " + EXPRESSION_STRING + " } "));
	}

	@Test // DATAMONGO-2048
	public void toIntUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToInt().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toInt: \"$shrewd\" } "));
	}

	@Test // DATAMONGO-2048
	public void toIntUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToInt().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toInt: " + EXPRESSION_STRING + " } "));
	}

	@Test // DATAMONGO-2048
	public void toLongUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToLong().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toLong: \"$shrewd\" } "));
	}

	@Test // DATAMONGO-2048
	public void toLongUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToLong().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toLong: " + EXPRESSION_STRING + " } "));
	}

	@Test // DATAMONGO-2048
	public void toObjectIdUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToObjectId().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toObjectId: \"$shrewd\" } "));
	}

	@Test // DATAMONGO-2048
	public void toObjectIdUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToObjectId().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toObjectId: " + EXPRESSION_STRING + " } "));
	}

	@Test // DATAMONGO-2048
	public void toStringUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToString().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toString: \"$shrewd\" } "));
	}

	@Test // DATAMONGO-2048
	public void toStringUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToString().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $toString: " + EXPRESSION_STRING + " } "));
	}
}
