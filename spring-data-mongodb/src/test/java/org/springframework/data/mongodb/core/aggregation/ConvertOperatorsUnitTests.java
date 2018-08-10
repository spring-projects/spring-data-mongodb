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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Unit tests for {@link ConvertOperators}.
 *
 * @author Christoph Strobl
 * @currentRead Royal Assassin - Robin Hobb
 */
public class ConvertOperatorsUnitTests {

	static final String EXPRESSION_STRING = "{ \"$molly\" : \"chandler\" }";
	static final DBObject EXPRESSION_DOC = (DBObject) JSON.parse(EXPRESSION_STRING);
	static final AggregationExpression EXPRESSION = new AggregationExpression() {
		@Override
		public DBObject toDbObject(AggregationOperationContext context) {
			return EXPRESSION_DOC;
		}
	};

	@Test // DATAMONGO-2048
	public void convertToUsingStringIdentifier() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\" } } ")));
	}

	@Test // DATAMONGO-2048
	public void convertToUsingIntIdentifier() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo(1).toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : 1 } } ")));
	}

	@Test // DATAMONGO-2048
	public void convertToUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToTypeOf("fitz").toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"$fitz\" } } ")));
	}

	@Test // DATAMONGO-2048
	public void convertToUsingExpression() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToTypeOf(EXPRESSION).toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : " + EXPRESSION_STRING + " } } ")));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnErrorValue() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onErrorReturn("foo")
				.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onError\" : \"foo\" } } ")));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnErrorValueOfField() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onErrorReturnValueOf("verity")
				.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onError\" : \"$verity\" } } ")));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnErrorValueOfExpression() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onErrorReturnValueOf(EXPRESSION)
				.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onError\" : " + EXPRESSION_STRING
						+ " } } ")));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnNullValue() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onNullReturn("foo")
				.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onNull\" : \"foo\" } } ")));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnNullValueOfField() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onNullReturnValueOf("verity")
				.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onNull\" : \"$verity\" } } ")));
	}

	@Test // DATAMONGO-2048
	public void convertToWithOnNullValueOfExpression() {

		assertThat(ConvertOperators.valueOf("shrewd").convertTo("double").onNullReturnValueOf(EXPRESSION)
				.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $convert: { \"input\" : \"$shrewd\", \"to\" : \"double\", \"onNull\" : " + EXPRESSION_STRING
						+ " } } ")));
	}

	@Test // DATAMONGO-2048
	public void toBoolUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToBoolean().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toBool: \"$shrewd\" } ")));
	}

	@Test // DATAMONGO-2048
	public void toBoolUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToBoolean().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toBool: " + EXPRESSION_STRING + " } ")));
	}

	@Test // DATAMONGO-2048
	public void toDateUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToDate().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toDate: \"$shrewd\" } ")));
	}

	@Test // DATAMONGO-2048
	public void toDateUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToDate().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toDate: " + EXPRESSION_STRING + " } ")));
	}

	@Test // DATAMONGO-2048
	public void toDecimalUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToDecimal().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toDecimal: \"$shrewd\" } ")));
	}

	@Test // DATAMONGO-2048
	public void toDecimalUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToDecimal().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toDecimal: " + EXPRESSION_STRING + " } ")));
	}

	@Test // DATAMONGO-2048
	public void toDoubleUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToDouble().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toDouble: \"$shrewd\" } ")));
	}

	@Test // DATAMONGO-2048
	public void toDoubleUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToDouble().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toDouble: " + EXPRESSION_STRING + " } ")));
	}

	@Test // DATAMONGO-2048
	public void toIntUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToInt().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toInt: \"$shrewd\" } ")));
	}

	@Test // DATAMONGO-2048
	public void toIntUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToInt().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toInt: " + EXPRESSION_STRING + " } ")));
	}

	@Test // DATAMONGO-2048
	public void toLongUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToLong().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toLong: \"$shrewd\" } ")));
	}

	@Test // DATAMONGO-2048
	public void toLongUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToLong().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toLong: " + EXPRESSION_STRING + " } ")));
	}

	@Test // DATAMONGO-2048
	public void toObjectIdUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToObjectId().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toObjectId: \"$shrewd\" } ")));
	}

	@Test // DATAMONGO-2048
	public void toObjectIdUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToObjectId().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toObjectId: " + EXPRESSION_STRING + " } ")));
	}

	@Test // DATAMONGO-2048
	public void toStringUsingFieldReference() {

		assertThat(ConvertOperators.valueOf("shrewd").convertToString().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toString: \"$shrewd\" } ")));
	}

	@Test // DATAMONGO-2048
	public void toStringUsingExpression() {

		assertThat(ConvertOperators.valueOf(EXPRESSION).convertToString().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $toString: " + EXPRESSION_STRING + " } ")));
	}
}
