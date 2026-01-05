/*
 * Copyright 2024-present the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import static org.assertj.core.api.Assertions.*;

import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link StringBasedAggregation}.
 *
 * @author Christoph Strobl
 */
public class StringBasedAggregationOperationUnitTests {

	@ParameterizedTest // GH-4712
	@ValueSource(strings = { "$project", "'$project'", "\"$project\"" })
	void extractsAggregationOperatorFromAggregationStringWithoutBindingParameters(String operator) {

		StringAggregationOperation agg = new StringAggregationOperation("{ %s : { 'fn' : 1 } }".formatted(operator),
				Object.class, (it) -> Assertions.fail("o_O Parameter binding"));

		assertThat(agg.getOperator()).isEqualTo("$project");
	}

	@Test // GH-4712
	void fallbackToParameterBindingIfAggregationOperatorCannotBeExtractedFromAggregationStringWithoutBindingParameters() {

		StringAggregationOperation agg = new StringAggregationOperation("{ happy-madison : { 'fn' : 1 } }", Object.class,
				(it) -> new Document("$project", ""));

		assertThat(agg.getOperator()).isEqualTo("$project");
	}
}
