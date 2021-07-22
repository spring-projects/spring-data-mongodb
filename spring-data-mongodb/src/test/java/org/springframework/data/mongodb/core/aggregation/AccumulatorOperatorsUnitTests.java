/*
 * Copyright 2021 the original author or authors.
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

	static class Jedi {

		String name;

		Date birthdate;

		@Field("force")
		Integer midichlorianCount;

		Integer balance;
	}
}
