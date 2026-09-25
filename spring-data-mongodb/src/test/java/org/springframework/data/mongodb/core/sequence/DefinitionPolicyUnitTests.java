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

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DefinitionPolicy}.
 *
 * @author Jeongkyun An
 */
class DefinitionPolicyUnitTests {

	private static final SequenceDefinition REQUESTED = SequenceDefinition.builder("orders").startWith(1).increment(1)
			.strategy(new SelfDescribingSequenceStrategy()).build();

	private static final Document AGREEING = new Document("value", 40L).append("startValue", 1L).append("increment", 1L);
	private static final Document DISAGREEING = new Document("value", 40L).append("startValue", 1L).append("increment",
			5L);

	@Test // GH-4823
	void validateShouldPassWhenBothSidesAgree() {
		assertThat(DefinitionPolicy.validate().reconcile(REQUESTED, AGREEING)).isEqualTo(REQUESTED);
	}

	@Test // GH-4823
	void validateShouldRejectADifferentIncrement() {

		assertThatExceptionOfType(SequenceDefinitionMismatchException.class)
				.isThrownBy(() -> DefinitionPolicy.validate().reconcile(REQUESTED, DISAGREEING))
				.satisfies(exception -> {
					assertThat(exception.getRequested().getIncrement()).isOne();
					assertThat(exception.getStored().getIncrement()).isEqualTo(5);
				});
	}

	@Test // GH-4823
	void serverWinsShouldCountTheStoredWay() {
		assertThat(DefinitionPolicy.serverWins().reconcile(REQUESTED, DISAGREEING).getIncrement()).isEqualTo(5);
	}

	@Test // GH-4823
	void clientWinsShouldKeepCountingTheRequestedWay() {
		assertThat(DefinitionPolicy.clientWins().reconcile(REQUESTED, DISAGREEING)).isEqualTo(REQUESTED);
	}

	@Test // GH-4823
	void validateShouldIgnoreWhereTheSequenceLives() {

		SequenceDefinition elsewhere = REQUESTED.withCollection("somewhere-else");

		assertThat(DefinitionPolicy.validate().reconcile(elsewhere, AGREEING)).isEqualTo(elsewhere);
	}
}
