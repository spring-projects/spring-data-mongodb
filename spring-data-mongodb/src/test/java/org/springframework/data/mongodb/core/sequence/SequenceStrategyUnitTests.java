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
 * Unit tests for {@link CounterSequenceStrategy} and {@link SelfDescribingSequenceStrategy}.
 *
 * @author Jeongkyun An
 */
class SequenceStrategyUnitTests {

	@Test // GH-4823
	void counterShouldStoreNothingButTheValue() {

		SequenceDefinition definition = SequenceDefinition.builder("orders").startWith(100).increment(5).build();
		Document initial = new CounterSequenceStrategy().initialDocument(definition);

		assertThat(initial).containsExactly(entry("value", 100L));
	}

	@Test // GH-4823
	void counterShouldIncrementByTheConfiguredStep() {

		SequenceDefinition definition = SequenceDefinition.builder("orders").increment(5).build();

		assertThat(new CounterSequenceStrategy().incrementUpdate(definition))
				.isEqualTo(new Document("$inc", new Document("value", 5L)));
	}

	@Test // GH-4823
	void counterShouldCarryAttributes() {

		SequenceDefinition definition = SequenceDefinition.builder("orders").attribute("owner", "billing").build();

		assertThat(new CounterSequenceStrategy().initialDocument(definition)).containsEntry("owner", "billing");
	}

	@Test // GH-4823
	void counterShouldReadAnyNumericValue() {

		CounterSequenceStrategy strategy = new CounterSequenceStrategy();

		assertThat(strategy.currentValue(new Document("value", 7))).isEqualTo(7L);
		assertThat(strategy.currentValue(new Document("value", 7L))).isEqualTo(7L);
	}

	@Test // GH-4823
	void counterShouldRejectANonNumericValue() {

		assertThatIllegalStateException()
				.isThrownBy(() -> new CounterSequenceStrategy().currentValue(new Document("value", "seven")));
	}

	@Test // GH-4823
	void selfDescribingShouldStoreHowItCounts() {

		SequenceDefinition definition = SequenceDefinition.builder("orders").startWith(100).increment(5).build();
		SelfDescribingSequenceStrategy strategy = new SelfDescribingSequenceStrategy();

		assertThat(strategy.initialDocument(definition)).containsExactly(entry("value", 100L), entry("startValue", 100L),
				entry("increment", 5L));
		assertThat(strategy.isSelfDescribing()).isTrue();
	}

	@Test // GH-4823
	void selfDescribingShouldReadBackWhatItStored() {

		SequenceDefinition requested = SequenceDefinition.builder("orders").startWith(1).increment(1).build();
		Document stored = new Document("value", 40L).append("startValue", 100L).append("increment", 5L);

		SequenceDefinition read = SelfDescribingSequenceStrategy.readDefinition(requested, stored);

		assertThat(read.getStartValue()).isEqualTo(100);
		assertThat(read.getIncrement()).isEqualTo(5);
		assertThat(read.getName()).isEqualTo("orders");
	}

	@Test // GH-4823
	void selfDescribingShouldFallBackForAnythingTheDocumentOmits() {

		SequenceDefinition requested = SequenceDefinition.builder("orders").startWith(1).increment(7).build();

		SequenceDefinition read = SelfDescribingSequenceStrategy.readDefinition(requested, new Document("value", 40L));

		assertThat(read.getIncrement()).isEqualTo(7);
	}
}
