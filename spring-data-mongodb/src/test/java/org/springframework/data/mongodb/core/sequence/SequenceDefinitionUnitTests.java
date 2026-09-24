/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.core.sequence;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SequenceDefinition}.
 *
 * @author Jeongkyun An
 */
class SequenceDefinitionUnitTests {

	@Test // GH-4823
	void shouldStartAtOneInTheDefaultCollection() {

		SequenceDefinition definition = SequenceDefinition.of("orders");

		assertThat(definition.getCollectionName()).isEqualTo("sequences");
		assertThat(definition.getStartValue()).isOne();
		assertThat(definition.getIncrement()).isOne();
		assertThat(definition.isTransactional()).isFalse();
		assertThat(definition.getAttributes()).isEmpty();
	}

	@Test // GH-4823
	void shouldCarryBuilderSettings() {

		SequenceDefinition definition = SequenceDefinition.builder("orders").database("counters").collection("__seq")
				.startWith(100).increment(5).transactional(true).attribute("owner", "billing").build();

		assertThat(definition.getDatabaseName()).isEqualTo("counters");
		assertThat(definition.getCollectionName()).isEqualTo("__seq");
		assertThat(definition.getStartValue()).isEqualTo(100);
		assertThat(definition.getIncrement()).isEqualTo(5);
		assertThat(definition.isTransactional()).isTrue();
		assertThat(definition.getAttributes()).containsEntry("owner", "billing");
	}

	@Test // GH-4823
	void withersShouldLeaveTheSourceAlone() {

		SequenceDefinition definition = SequenceDefinition.builder("orders").increment(5).build();
		SequenceDefinition modified = definition.withIncrement(10).withCollection("elsewhere");

		assertThat(definition.getIncrement()).isEqualTo(5);
		assertThat(definition.getCollectionName()).isEqualTo("sequences");
		assertThat(modified.getIncrement()).isEqualTo(10);
		assertThat(modified.getCollectionName()).isEqualTo("elsewhere");
		assertThat(modified.getName()).isEqualTo("orders");
	}

	@Test // GH-4823
	void attributesShouldNotBeModifiable() {

		SequenceDefinition definition = SequenceDefinition.builder("orders").attribute("owner", "billing").build();

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> definition.getAttributes().put("owner", "someone else"));
	}

	@Test // GH-4823
	void shouldCompareHowTheyCount() {

		SequenceDefinition definition = SequenceDefinition.builder("orders").startWith(1).increment(5).build();

		assertThat(definition.describesSameSequenceAs(definition.withCollection("elsewhere"))).isTrue();
		assertThat(definition.describesSameSequenceAs(definition.withIncrement(10))).isFalse();
	}

	@Test // GH-4823
	void shouldRejectInvalidArguments() {

		assertThatIllegalArgumentException().isThrownBy(() -> SequenceDefinition.of(""));
		assertThatIllegalArgumentException().isThrownBy(() -> SequenceDefinition.builder("orders").increment(0));
		assertThatIllegalArgumentException().isThrownBy(() -> SequenceDefinition.builder("orders").increment(-1));
		assertThatIllegalArgumentException().isThrownBy(() -> SequenceDefinition.builder("orders").collection(""));
		assertThatIllegalArgumentException().isThrownBy(() -> SequenceDefinition.builder("orders").database(""));
		assertThatIllegalArgumentException().isThrownBy(() -> SequenceDefinition.builder("orders").strategy(null));
	}
}
