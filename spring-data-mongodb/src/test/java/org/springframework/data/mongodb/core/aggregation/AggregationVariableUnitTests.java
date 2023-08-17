/*
 * Copyright 2022-2023 the original author or authors.
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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AggregationVariable}.
 *
 * @author Christoph Strobl
 */
class AggregationVariableUnitTests {

	@Test // GH-4070
	void variableErrorsOnNullValue() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> AggregationVariable.variable(null));
	}

	@Test // GH-4070
	void createsVariable() {

		var variable = AggregationVariable.variable("$$now");

		assertThat(variable.getTarget()).isEqualTo("$$now");
		assertThat(variable.isInternal()).isFalse();
	}

	@Test // GH-4070
	void prefixesVariableIfNeeded() {

		var variable = AggregationVariable.variable("this");

		assertThat(variable.getTarget()).isEqualTo("$$this");
	}

	@Test // GH-4070
	void localVariableErrorsOnNullValue() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> AggregationVariable.localVariable(null));
	}

	@Test // GH-4070
	void localVariable() {

		var variable = AggregationVariable.localVariable("$$this");

		assertThat(variable.getTarget()).isEqualTo("$$this");
		assertThat(variable.isInternal()).isTrue();
	}

	@Test // GH-4070
	void prefixesLocalVariableIfNeeded() {

		var variable = AggregationVariable.localVariable("this");

		assertThat(variable.getTarget()).isEqualTo("$$this");
	}

	@Test // GH-4070
	void isVariableReturnsTrueForAggregationVariableTypes() {

		var variable = Mockito.mock(AggregationVariable.class);

		assertThat(AggregationVariable.isVariable(variable)).isTrue();
	}

	@Test // GH-4070
	void isVariableReturnsTrueForFieldThatTargetsVariable() {

		var variable = Fields.field("value", "$$this");

		assertThat(AggregationVariable.isVariable(variable)).isTrue();
	}

	@Test // GH-4070
	void isVariableReturnsFalseForFieldThatDontTargetsVariable() {

		var variable = Fields.field("value", "$this");

		assertThat(AggregationVariable.isVariable(variable)).isFalse();
	}
}
