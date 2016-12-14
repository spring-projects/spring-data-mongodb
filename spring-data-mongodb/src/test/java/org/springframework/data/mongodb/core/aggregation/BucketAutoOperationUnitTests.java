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

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.DBObjectTestUtils.getAsDBObject;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.aggregation.BucketAutoOperation.Granularities;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Unit tests for {@link BucketAutoOperation}.
 *
 * @author Mark Paluch
 */
public class BucketAutoOperationUnitTests {

	/**
	 * @see DATAMONGO-1552
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFields() {
		new BucketAutoOperation((Field) null, 0);
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNonPositiveIntegerNullFields() {
		new BucketAutoOperation(Fields.field("field"), 0);
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderBucketOutputExpressions() {

		BucketAutoOperation operation = Aggregation.bucketAuto("field", 5) //
				.andOutputExpression("(netPrice + surCharge) * taxrate * [0]", 2).as("grossSalesPrice") //
				.andOutput("title").push().as("titles");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse(
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

		BucketAutoOperation operation = Aggregation.bucketAuto("field", 5) //
				.andOutputCount().as("titles");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ titles : { $sum: 1 } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderCorrectly() {

		DBObject agg = bucketAuto("field", 1).withBuckets(5).toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $bucketAuto: { groupBy: \"$field\", buckets: 5 } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderGranulariy() {

		DBObject agg = bucketAuto("field", 1) //
				.withGranularity(Granularities.E24) //
				.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg, is(JSON.parse("{ $bucketAuto: { buckets: 1, granularity: \"E24\", groupBy: \"$field\" } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderSumOperator() {

		BucketAutoOperation operation = bucketAuto("field", 5) //
				.andOutput("score").sum().as("cummulated_score");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject), is(JSON.parse("{ cummulated_score : { $sum: \"$score\" } }")));
	}

	/**
	 * @see DATAMONGO-1552
	 */
	@Test
	public void shouldRenderSumWithOwnOutputExpression() {

		BucketAutoOperation operation = bucketAuto("field", 5) //
				.andOutputExpression("netPrice + tax").apply("$multiply", 5).as("total");

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(dbObject),
				is(JSON.parse("{ total : { $multiply: [ {$add : [\"$netPrice\", \"$tax\"]}, 5] } }")));
	}

	private static DBObject extractOutput(DBObject fromBucketClause) {
		return getAsDBObject(getAsDBObject(fromBucketClause, "$bucketAuto"), "output");
	}
}
