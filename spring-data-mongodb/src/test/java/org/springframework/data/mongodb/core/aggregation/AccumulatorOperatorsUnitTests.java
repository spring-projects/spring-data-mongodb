/*
 * Copyright 2021-2023 the original author or authors.
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
import static org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.*;

import java.util.Arrays;
import java.util.Date;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.aggregation.DateOperators.Year;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.util.aggregation.TestAggregationContext;

/**
 * Unit tests for {@link AccumulatorOperators}.
 *
 * @author Christoph Strobl
 * @author Julia Lee
 */
class AccumulatorOperatorsUnitTests {

	@Test // GH-3712
	void rendersCovariancePopWithFieldReference() {

		assertThat(AccumulatorOperators.valueOf("balance").covariancePop("midichlorianCount")
				.toDocument(TestAggregationContext.contextFor(Jedi.class)))
						.isEqualTo(new Document("$covariancePop", Arrays.asList("$balance", "$force")));
	}

	@Test // GH-3712
	void rendersCovariancePopWithExpression() {

		assertThat(AccumulatorOperators.valueOf(Year.yearOf("birthdate")).covariancePop("midichlorianCount")
				.toDocument(TestAggregationContext.contextFor(Jedi.class)))
						.isEqualTo(new Document("$covariancePop", Arrays.asList(new Document("$year", "$birthdate"), "$force")));
	}

	@Test // GH-3712
	void rendersCovarianceSampWithFieldReference() {

		assertThat(AccumulatorOperators.valueOf("balance").covarianceSamp("midichlorianCount")
				.toDocument(TestAggregationContext.contextFor(Jedi.class)))
						.isEqualTo(new Document("$covarianceSamp", Arrays.asList("$balance", "$force")));
	}

	@Test // GH-3712
	void rendersCovarianceSampWithExpression() {

		assertThat(AccumulatorOperators.valueOf(Year.yearOf("birthdate")).covarianceSamp("midichlorianCount")
				.toDocument(TestAggregationContext.contextFor(Jedi.class)))
						.isEqualTo(new Document("$covarianceSamp", Arrays.asList(new Document("$year", "$birthdate"), "$force")));
	}

	@Test // GH-3718
	void rendersExpMovingAvgWithNumberOfHistoricDocuments() {

		assertThat(valueOf("price").expMovingAvg().historicalDocuments(2).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $expMovingAvg: { input: \"$price\", N: 2 } }"));
	}

	@Test // GH-3718
	void rendersExpMovingAvgWithAlpha() {

		assertThat(valueOf("price").expMovingAvg().alpha(0.75).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $expMovingAvg: { input: \"$price\", alpha: 0.75 } }"));
	}

	@Test // GH-4139
	void rendersMax() {

		assertThat(valueOf("price").max().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $max: \"$price\" }"));
	}

	@Test // GH-4139
	void rendersMaxN() {

		assertThat(valueOf("price").max(3).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $maxN: { n: 3, input : \"$price\" } }"));
	}

	@Test // GH-4139
	void rendersMin() {

		assertThat(valueOf("price").min().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $min: \"$price\" }"));
	}

	@Test // GH-4139
	void rendersMinN() {

		assertThat(valueOf("price").min(3).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $minN: { n: 3, input : \"$price\" } }"));
	}

	@Test // GH-4473
	void rendersPercentileWithFieldReference() {

		assertThat(valueOf("score").percentile(0.2).toDocument(Aggregation.DEFAULT_CONTEXT))
			.isEqualTo(Document.parse("{ $percentile: { input: \"$score\", method: \"approximate\", p: [0.2] } }"));

		assertThat(valueOf("score").percentile(0.3, 0.9).toDocument(Aggregation.DEFAULT_CONTEXT))
			.isEqualTo(Document.parse("{ $percentile: { input: \"$score\", method: \"approximate\", p: [0.3, 0.9] } }"));

		assertThat(valueOf("score").percentile(0.3, 0.9).and("scoreTwo").toDocument(Aggregation.DEFAULT_CONTEXT))
			.isEqualTo(Document.parse("{ $percentile: { input: [\"$score\", \"$scoreTwo\"], method: \"approximate\", p: [0.3, 0.9] } }"));
	}

	@Test // GH-4473
	void rendersPercentileWithExpression() {

		assertThat(valueOf(Sum.sumOf("score")).percentile(0.1).toDocument(Aggregation.DEFAULT_CONTEXT))
			.isEqualTo(Document.parse("{ $percentile: { input: {\"$sum\": \"$score\"}, method: \"approximate\", p: [0.1] } }"));

		assertThat(valueOf("scoreOne").percentile(0.1, 0.2).and(Sum.sumOf("scoreTwo")).toDocument(Aggregation.DEFAULT_CONTEXT))
			.isEqualTo(Document.parse("{ $percentile: { input: [\"$scoreOne\", {\"$sum\": \"$scoreTwo\"}], method: \"approximate\", p: [0.1, 0.2] } }"));
	}

	@Test // GH-4472
	void rendersMedianWithFieldReference() {

		assertThat(valueOf("score").median().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $median: { input: \"$score\", method: \"approximate\" } }"));

		assertThat(valueOf("score").median().and("scoreTwo").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $median: { input: [\"$score\", \"$scoreTwo\"], method: \"approximate\" } }"));
	}

	@Test // GH-4472
	void rendersMedianWithExpression() {

		assertThat(valueOf(Sum.sumOf("score")).median().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $median: { input: {\"$sum\": \"$score\"}, method: \"approximate\" } }"));

		assertThat(valueOf("scoreOne").median().and(Sum.sumOf("scoreTwo")).toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $median: { input: [\"$scoreOne\", {\"$sum\": \"$scoreTwo\"}], method: \"approximate\" } }"));
	}

	@Test // GH-4472
	void rendersMedianCorrectlyWithTypedAggregationContext() {

		assertThat(valueOf("midichlorianCount").median()
				.toDocument(TestAggregationContext.contextFor(Jedi.class)))
						.isEqualTo(Document.parse("{ $median: { input: \"$force\", method: \"approximate\" } }"));
	}

	static class Jedi {

		String name;

		Date birthdate;

		@Field("force") Integer midichlorianCount;

		Integer balance;
	}
}
