/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.DBObjectTestUtils.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Unit tests for {@link BucketOperation}.
 *
 * @author Mark Paluch
 */
public class BucketOperationUnitTests {

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1552
	public void rejectsNullFields() {
		new BucketOperation((Field) null);
	}

	@Test // DATAMONGO-1552
	public void shouldRenderBucketOutputExpressions() {

		BucketOperation operation = Aggregation.bucket("field") //
				.andOutputExpression("(netPrice + surCharge) * taxrate * [0]", 2).as("grossSalesPrice") //
				.andOutput("title").push().as("titles");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse(
				"{ \"grossSalesPrice\" : { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\" , 2]} , \"titles\" : { $push: \"$title\" } }}")));
	}

	@Test(expected = IllegalStateException.class) // DATAMONGO-1552
	public void shouldRenderEmptyAggregationExpression() {
		bucket("groupby").andOutput("field").as("alias");
	}

	@Test // DATAMONGO-1552
	public void shouldRenderBucketOutputOperators() {

		BucketOperation operation = Aggregation.bucket("field") //
				.andOutputCount().as("titles");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ titles : { $sum: 1 } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderSumAggregationExpression() {

		DBObject agg = bucket("field") //
				.andOutput(ArithmeticOperators.valueOf("quizzes").sum()).as("quizTotal") //
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse(
				"{ $bucket: { groupBy: \"$field\", boundaries: [],  output : { quizTotal: { $sum: \"$quizzes\"} } } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderDefault() {

		DBObject agg = bucket("field").withDefaultBucket("default bucket").toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $bucket: { groupBy: \"$field\", boundaries: [],  default: \"default bucket\" } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderBoundaries() {

		DBObject agg = bucket("field") //
				.withDefaultBucket("default bucket") //
				.withBoundaries(0) //
				.withBoundaries(10, 20).toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(JSON.parse("{ $bucket: { boundaries: [0, 10, 20],  default: \"default bucket\", groupBy: \"$field\" } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderSumOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("score").sum().as("cummulated_score");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ cummulated_score : { $sum: \"$score\" } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderSumWithValueOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("score").sum(4).as("cummulated_score");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ cummulated_score : { $sum: 4 } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderAvgOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("score").avg().as("average");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ average : { $avg: \"$score\" } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderFirstOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("title").first().as("first_title");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ first_title : { $first: \"$title\" } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderLastOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("title").last().as("last_title");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ last_title : { $last: \"$title\" } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderMinOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("score").min().as("min_score");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ min_score : { $min: \"$score\" } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderPushOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("title").push().as("titles");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ titles : { $push: \"$title\" } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderAddToSetOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("title").addToSet().as("titles");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ titles : { $addToSet: \"$title\" } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderSumWithExpression() {

		BucketOperation operation = bucket("field") //
				.andOutputExpression("netPrice + tax").sum().as("total");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ total : { $sum: { $add : [\"$netPrice\", \"$tax\"]} } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderSumWithOwnOutputExpression() {

		BucketOperation operation = bucket("field") //
				.andOutputExpression("netPrice + tax").apply("$multiply", 5).as("total");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject),
				is(JSON.parse("{ total : { $multiply: [ {$add : [\"$netPrice\", \"$tax\"]}, 5] } }")));
	}

	@Test // DATAMONGO-1552
	public void shouldExposeDefaultCountField() {

		BucketOperation operation = bucket("field");

		assertThat(operation.getFields().exposesSingleFieldOnly(), is(true));
		assertThat(operation.getFields().getField("count"), is(notNullValue()));
	}

	private static DBObject extractOutput(DBObject fromBucketClause) {
		return getAsDBObject(getAsDBObject(fromBucketClause, "$bucket"), "output");
	}
}
