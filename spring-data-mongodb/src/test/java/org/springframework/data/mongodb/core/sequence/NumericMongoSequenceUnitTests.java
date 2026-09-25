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
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.MongoDatabaseFactory;

/**
 * Unit tests for {@link NumericMongoSequence}.
 *
 * @author Jeongkyun An
 */
@ExtendWith(MockitoExtension.class)
class NumericMongoSequenceUnitTests {

	@Mock SequenceExecutor executor;
	@Mock MongoDatabaseFactory dbFactory;

	@Test // GH-4823
	void shouldDelegateToExecutor() {

		SequenceDefinition definition = SequenceDefinition.of("test-seq");
		when(executor.nextValue(definition, dbFactory)).thenReturn(42L);

		assertThat(new NumericMongoSequence(definition, executor, dbFactory).nextValue()).isEqualTo(42L);
	}

	@Test // GH-4823
	void shouldNotCacheValues() {

		SequenceDefinition definition = SequenceDefinition.of("test-seq");
		when(executor.nextValue(definition, dbFactory)).thenReturn(1L, 2L);

		NumericMongoSequence sequence = new NumericMongoSequence(definition, executor, dbFactory);

		assertThat(sequence.nextValue()).isEqualTo(1L);
		assertThat(sequence.nextValue()).isEqualTo(2L);
		verify(executor, times(2)).nextValue(definition, dbFactory);
	}

	@Test // GH-4823
	void shouldExposeName() {
		assertThat(new NumericMongoSequence(SequenceDefinition.of("orders"), executor, dbFactory).getName())
				.isEqualTo("orders");
	}

	@Test // GH-4823
	void shouldRejectMissingArguments() {

		SequenceDefinition definition = SequenceDefinition.of("test-seq");

		assertThatIllegalArgumentException().isThrownBy(() -> new NumericMongoSequence(null, executor, dbFactory));
		assertThatIllegalArgumentException().isThrownBy(() -> new NumericMongoSequence(definition, null, dbFactory));
		assertThatIllegalArgumentException().isThrownBy(() -> new NumericMongoSequence(definition, executor, null));
	}
}
