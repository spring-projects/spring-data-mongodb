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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Unit test for {@link StringOperators}.
 *
 * @author Christoph Strobl
 * @currentRead Royal Assassin - Robin Hobb
 */
public class StringOperatorsUnitTests {

	static final String EXPRESSION_STRING = "{ \"$fitz\" : \"chivalry\" }";
	static final DBObject EXPRESSION_DOC = (DBObject) JSON.parse(EXPRESSION_STRING);
	static final AggregationExpression EXPRESSION = new AggregationExpression() {
		@Override
		public DBObject toDbObject(AggregationOperationContext context) {
			return EXPRESSION_DOC;
		}
	};

	@Test // DATAMONGO-2049
	public void shouldRenderTrim() {

		assertThat(StringOperators.valueOf("shrewd").trim().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $trim: { \"input\" : \"$shrewd\" } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).trim().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $trim: { \"input\" : " + EXPRESSION_STRING + " } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimWithChars() {

		assertThat(StringOperators.valueOf("shrewd").trim("sh").toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $trim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimWithCharsExpression() {

		assertThat(StringOperators.valueOf("shrewd").trim(EXPRESSION).toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $trim: { \"input\" : \"$shrewd\", \"chars\" : " + EXPRESSION_STRING + " } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimLeft() {

		assertThat(StringOperators.valueOf("shrewd").trim().left().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $ltrim: { \"input\" : \"$shrewd\" } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimLeftWithChars() {

		assertThat(StringOperators.valueOf("shrewd").trim("sh").left().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $ltrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimRight() {

		assertThat(StringOperators.valueOf("shrewd").trim().right().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $rtrim: { \"input\" : \"$shrewd\" } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderTrimRightWithChars() {

		assertThat(StringOperators.valueOf("shrewd").trim("sh").right().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $rtrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderLTrim() {

		assertThat(StringOperators.valueOf("shrewd").ltrim().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $ltrim: { \"input\" : \"$shrewd\" } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderLTrimForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).ltrim().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $ltrim: { \"input\" : " + EXPRESSION_STRING + " } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderLTrimWithChars() {

		assertThat(StringOperators.valueOf("shrewd").ltrim("sh").toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $ltrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderLTrimWithCharsExpression() {

		assertThat(StringOperators.valueOf("shrewd").ltrim(EXPRESSION).toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $ltrim: { \"input\" : \"$shrewd\", \"chars\" : " + EXPRESSION_STRING + " } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderRTrim() {

		assertThat(StringOperators.valueOf("shrewd").rtrim().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $rtrim: { \"input\" : \"$shrewd\" } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderRTrimForExpression() {

		assertThat(StringOperators.valueOf(EXPRESSION).rtrim().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $rtrim: { \"input\" : " + EXPRESSION_STRING + " } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderRTrimWithChars() {

		assertThat(StringOperators.valueOf("shrewd").rtrim("sh").toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $rtrim: { \"input\" : \"$shrewd\", \"chars\" : \"sh\" } } ")));
	}

	@Test // DATAMONGO-2049
	public void shouldRenderRTrimWithCharsExpression() {

		assertThat(StringOperators.valueOf("shrewd").rtrim(EXPRESSION).toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(JSON.parse("{ $rtrim: { \"input\" : \"$shrewd\", \"chars\" : " + EXPRESSION_STRING + " } } ")));
	}

}
