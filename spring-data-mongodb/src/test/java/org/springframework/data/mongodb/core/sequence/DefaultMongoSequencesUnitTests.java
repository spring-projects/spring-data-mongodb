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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.MongoDatabaseFactory;

/**
 * Unit tests for {@link DefaultMongoSequences} and the defaults collected by {@link MongoSequencesBuilder}.
 *
 * @author Jeongkyun An
 */
@ExtendWith(MockitoExtension.class)
class DefaultMongoSequencesUnitTests {

	@Mock MongoDatabaseFactory dbFactory;

	@Test // GH-4823
	void shouldApplyDefaults() {

		SequenceDefinition definition = definitionOf(MongoSequences.create(dbFactory).numericSequence("orders"));

		assertThat(definition.getName()).isEqualTo("orders");
		assertThat(definition.getCollectionName()).isEqualTo("sequences");
		assertThat(definition.getDatabaseName()).isNull();
		assertThat(definition.getStartValue()).isOne();
		assertThat(definition.getIncrement()).isOne();
		assertThat(definition.getStrategy()).isInstanceOf(CounterSequenceStrategy.class);
	}

	@Test // GH-4823
	void shouldApplyBuilderDefaultsToEverySequence() {

		MongoSequences sequences = MongoSequences.builder(dbFactory).defaultCollection("__sequences")
				.defaultDatabase("counters").strategy(new SelfDescribingSequenceStrategy()).build();

		for (String name : new String[] { "orders", "invoices" }) {

			SequenceDefinition definition = definitionOf(sequences.numericSequence(name));

			assertThat(definition.getCollectionName()).isEqualTo("__sequences");
			assertThat(definition.getDatabaseName()).isEqualTo("counters");
			assertThat(definition.getStrategy()).isInstanceOf(SelfDescribingSequenceStrategy.class);
		}
	}

	@Test // GH-4823
	void definitionShouldOverrideBuilderDefaults() {

		MongoSequences sequences = MongoSequences.builder(dbFactory).defaultCollection("__sequences").build();

		SequenceDefinition definition = definitionOf(
				sequences.numericSequence("orders", it -> it.collection("elsewhere")));

		assertThat(definition.getCollectionName()).isEqualTo("elsewhere");
	}

	@Test // GH-4823
	void shouldUseConfiguredExecutorByDefault() {

		MongoSequences sequences = MongoSequences.builder(dbFactory).executor(new SessionBoundSequenceExecutor()).build();

		assertThat(executorOf(sequences.numericSequence("orders"))).isInstanceOf(SessionBoundSequenceExecutor.class);
		assertThat(executorOf(MongoSequences.create(dbFactory).numericSequence("orders")))
				.isInstanceOf(DefaultSequenceExecutor.class);
	}

	@Test // GH-4823
	void transactionalDefinitionShouldOverrideConfiguredExecutor() {

		MongoSequences sequences = MongoSequences.create(dbFactory);

		assertThat(executorOf(sequences.numericSequence("orders", it -> it.transactional(true))))
				.isInstanceOf(SessionBoundSequenceExecutor.class);
	}

	@Test // GH-4823
	void shouldRejectMissingArguments() {

		MongoSequences sequences = MongoSequences.create(dbFactory);

		assertThatIllegalArgumentException().isThrownBy(() -> MongoSequences.create(null));
		assertThatIllegalArgumentException().isThrownBy(() -> sequences.numericSequence(""));
		assertThatIllegalArgumentException().isThrownBy(() -> sequences.numericSequence((SequenceDefinition) null));
	}

	private static SequenceDefinition definitionOf(MongoSequence<Long> sequence) {
		return ((NumericMongoSequence) sequence).getDefinition();
	}

	private static SequenceExecutor executorOf(MongoSequence<Long> sequence) {
		return ((NumericMongoSequence) sequence).getExecutor();
	}
}
