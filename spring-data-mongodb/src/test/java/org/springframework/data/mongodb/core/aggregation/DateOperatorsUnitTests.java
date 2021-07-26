/*
 * Copyright 2021. the original author or authors.
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
import org.springframework.data.mongodb.core.aggregation.DateOperators.Timezone;

/**
 * @author Christoph Strobl
 */
class DateOperatorsUnitTests {

	@Test // GH-3713
	void rendersDateAdd() {

		assertThat(DateOperators.dateOf("purchaseDate").add(3, "day").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $dateAdd: { startDate: \"$purchaseDate\", unit: \"day\", amount: 3 } }"));
	}

	@Test // GH-3713
	void rendersDateAddWithTimezone() {
		
		assertThat(DateOperators.dateOf("purchaseDate").withTimezone(Timezone.valueOf("America/Chicago")).add(3, "day")
				.toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(Document.parse(
						"{ $dateAdd: { startDate: \"$purchaseDate\", unit: \"day\", amount: 3, timezone : \"America/Chicago\" } }"));
	}

	@Test // GH-3713
	void rendersDateDiff() {

		assertThat(
				DateOperators.dateOf("purchaseDate").diffValueOf("delivered", "day").toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo(Document
								.parse("{ $dateDiff: { startDate: \"$purchaseDate\", endDate: \"$delivered\", unit: \"day\" } }"));
	}

	@Test // GH-3713
	void rendersDateDiffWithTimezone() {

		assertThat(DateOperators.dateOf("purchaseDate").withTimezone(Timezone.valueOf("America/Chicago"))
				.diffValueOf("delivered", "day").toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(Document.parse(
						"{ $dateDiff: { startDate: \"$purchaseDate\", endDate: \"$delivered\", unit: \"day\", timezone : \"America/Chicago\" } }"));
	}
}
