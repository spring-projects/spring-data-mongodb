/*
 * Copyright 2016-2024 the original author or authors.
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
import static org.springframework.data.mongodb.core.DocumentTestUtils.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.aggregation.BucketAutoOperation.Granularities;

/**
 * Unit tests for {@link BucketAutoOperation}.
 *
 * @author Mark Paluch
 */
public class BucketAutoOperationUnitTests {

	@Test // DATAMONGO-1552
	public void rejectsNullFields() {
		assertThatIllegalArgumentException().isThrownBy(() -> new BucketAutoOperation((Field) null, 0));
	}

	@Test // DATAMONGO-1552
	public void rejectsNonPositiveIntegerNullFields() {
		assertThatIllegalArgumentException().isThrownBy(() -> new BucketAutoOperation(Fields.field("field"), 0));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderBucketOutputExpressions() {

		BucketAutoOperation operation = Aggregation.bucketAuto("field", 5) //
				.andOutputExpression("(netPrice + surCharge) * taxrate * [0]", 2).as("grossSalesPrice") //
				.andOutput("title").push().as("titles");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg)).isEqualTo(Document.parse(
				"{ \"grossSalesPrice\" : { \"$multiply\" : [ { \"$add\" : [ \"$netPrice\" , \"$surCharge\"]} , \"$taxrate\" , 2]} , \"titles\" : { $push: \"$title\" } }}"));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderEmptyAggregationExpression() {
		assertThatIllegalStateException().isThrownBy(() -> bucket("groupby").andOutput("field").as("alias"));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderBucketOutputOperators() {

		BucketAutoOperation operation = Aggregation.bucketAuto("field", 5) //
				.andOutputCount().as("titles");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg)).isEqualTo(Document.parse("{ titles : { $sum: 1 } }"));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderCorrectly() {

		Document agg = bucketAuto("field", 1).withBuckets(5).toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg).isEqualTo(Document.parse("{ $bucketAuto: { groupBy: \"$field\", buckets: 5 } }"));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderGranulariy() {

		Document agg = bucketAuto("field", 1) //
				.withGranularity(Granularities.E24) //
				.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(agg)
				.isEqualTo(Document.parse("{ $bucketAuto: { buckets: 1, granularity: \"E24\", groupBy: \"$field\" } }"));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderSumOperator() {

		BucketAutoOperation operation = bucketAuto("field", 5) //
				.andOutput("score").sum().as("cummulated_score");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg)).isEqualTo(Document.parse("{ cummulated_score : { $sum: \"$score\" } }"));
	}

	@Test // DATAMONGO-1552
	public void shouldRenderSumWithOwnOutputExpression() {

		BucketAutoOperation operation = bucketAuto("field", 5) //
				.andOutputExpression("netPrice + tax").apply("$multiply", 5).as("total");

		Document agg = operation.toDocument(Aggregation.DEFAULT_CONTEXT);
		assertThat(extractOutput(agg))
				.isEqualTo(Document.parse("{ total : { $multiply: [ {$add : [\"$netPrice\", \"$tax\"]}, 5] } }"));
	}

	private static Document extractOutput(Document fromBucketClause) {
		return getAsDocument(getAsDocument(fromBucketClause, "$bucketAuto"), "output");
	}
}
