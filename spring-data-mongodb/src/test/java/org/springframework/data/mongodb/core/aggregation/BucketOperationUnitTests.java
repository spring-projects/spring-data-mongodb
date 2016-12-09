/*
 * Copyright 2016 the original author or authors.
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
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.ArithmeticOperators;

import org.bson.Document;

/**
 * Unit tests for {@link BucketOperation}.
 * 
 * @author Mark Paluch
 */
public class BucketOperationUnitTests {

	/**
	 * @see DATAMONGO-1552
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFields() {
		new BucketOperation((Field) null);
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderBucketOutputExpressions() {

		BucketOperation operation = Aggregation.bucket("field") //
				.andOutputExpression("(netPrice + surCharge) * taxrate * [0]", 2).as("grossSalesPrice") //
				.andOutput("title").push().as("titles");

		Document dbObject = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(Document.parse(
				"{ \"grossSalesPrice\" : { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\" , 2]} , \"titles\" : { $push: \"$title\" } }}")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test(expected = IllegalStateException.class)
	public void shouldRenderEmptyAggregationExpression() {
		bucket("groupby").andOutput("field").as("alias");
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderBucketOutputOperators() {

		BucketOperation operation = Aggregation.bucket("field") //
				.andOutputCount().as("titles");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg), is(Document.parse("{ titles : { $sum: 1 } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderSumAggregationExpression() {

		Document agg = bucket("field") //
				.andOutput(ArithmeticOperators.valueOf("quizzes").sum()).as("quizTotal") //
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(Document.parse(
				"{ $bucket: { groupBy: \"$field\", boundaries: [],  output : { quizTotal: { $sum: \"$quizzes\"} } } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderDefault() {

		Document agg = bucket("field").withDefaultBucket("default bucket").toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $bucket: { groupBy: \"$field\", boundaries: [],  default: \"default bucket\" } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderBoundaries() {

		Document agg = bucket("field") //
				.withDefaultBucket("default bucket") //
				.withBoundaries(0) //
				.withBoundaries(10, 20).toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg,
				is(Document.parse("{ $bucket: { boundaries: [0, 10, 20],  default: \"default bucket\", groupBy: \"$field\" } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderSumOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("score").sum().as("cummulated_score");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg), is(Document.parse("{ cummulated_score : { $sum: \"$score\" } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderSumWithValueOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("score").sum(4).as("cummulated_score");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg), is(Document.parse("{ cummulated_score : { $sum: 4 } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderAvgOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("score").avg().as("average");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg), is(Document.parse("{ average : { $avg: \"$score\" } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderFirstOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("title").first().as("first_title");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg), is(Document.parse("{ first_title : { $first: \"$title\" } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderLastOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("title").last().as("last_title");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg), is(Document.parse("{ last_title : { $last: \"$title\" } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderMinOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("score").min().as("min_score");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg), is(Document.parse("{ min_score : { $min: \"$score\" } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderPushOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("title").push().as("titles");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg), is(Document.parse("{ titles : { $push: \"$title\" } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderAddToSetOperator() {

		BucketOperation operation = bucket("field") //
				.andOutput("title").addToSet().as("titles");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg), is(Document.parse("{ titles : { $addToSet: \"$title\" } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderSumWithExpression() {

		BucketOperation operation = bucket("field") //
				.andOutputExpression("netPrice + tax").sum().as("total");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg), is(Document.parse("{ total : { $sum: { $add : [\"$netPrice\", \"$tax\"]} } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderSumWithOwnOutputExpression() {

		BucketOperation operation = bucket("field") //
				.andOutputExpression("netPrice + tax").apply("$multiply", 5).as("total");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg),
				is(Document.parse("{ total : { $multiply: [ {$add : [\"$netPrice\", \"$tax\"]}, 5] } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldExposeDefaultCountField() {

		BucketOperation operation = bucket("field");

		assertThat(operation.getFields().exposesSingleFieldOnly(), is(true));
		assertThat(operation.getFields().getField("count"), is(notNullValue()));
	}

	private static Document extractOutput(Document fromBucketClause) {
		return (Document) ((Document) fromBucketClause.get("$bucket")).get("output");
	}
}
