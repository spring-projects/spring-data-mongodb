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

import java.util.function.Consumer;

import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.util.Assert;

/**
 * Hands out monotonically increasing numbers, which MongoDB has no native type for.
 *
 * <pre class="code">
 * MongoSequences sequences = MongoSequences.create(dbFactory);
 * long next = sequences.numericSequence("orders").nextValue();
 * </pre>
 *
 * Settings given to {@link #builder(MongoDatabaseFactory)} apply to every sequence obtained from that instance, while a
 * {@link SequenceDefinition} overrides them for a single one.
 *
 * <pre class="code">
 * MongoSequences sequences = MongoSequences.builder(dbFactory)
 * 		.defaultCollection("__sequences")
 * 		.executor(new SessionBoundSequenceExecutor())
 * 		.strategy(new CounterSequenceStrategy())
 * 		.build();
 * </pre>
 *
 * @author Jeongkyun An
 * @since 5.2
 * @see MongoSequence
 * @see SequenceDefinition
 * @see <a href="https://github.com/spring-projects/spring-data-mongodb/issues/4823">GH-4823</a>
 */
public interface MongoSequences {

	/**
	 * Sequences backed by the given factory, using the defaults: a counter in the {@code sequences} collection,
	 * starting at one, going up by one, incremented outside any ongoing transaction.
	 *
	 * @param dbFactory must not be {@literal null}.
	 * @return new instance of {@link MongoSequences}, never {@literal null}.
	 */
	static MongoSequences create(MongoDatabaseFactory dbFactory) {
		return builder(dbFactory).build();
	}

	/**
	 * Start building sequences backed by the given factory.
	 *
	 * @param dbFactory must not be {@literal null}.
	 * @return new instance of {@link MongoSequencesBuilder}, never {@literal null}.
	 */
	static MongoSequencesBuilder builder(MongoDatabaseFactory dbFactory) {

		Assert.notNull(dbFactory, "MongoDatabaseFactory must not be null");

		return new MongoSequencesBuilder(dbFactory);
	}

	/**
	 * A sequence with the given name, using the defaults of this instance.
	 *
	 * @param sequenceName must not be {@literal null} or empty.
	 * @return the sequence, never {@literal null}.
	 */
	MongoSequence<Long> numericSequence(String sequenceName);

	/**
	 * A sequence departing from the defaults of this instance in the ways the customizer says.
	 *
	 * <pre class="code">
	 * sequences.numericSequence("orders", it -&gt; it.startWith(1000).increment(10));
	 * </pre>
	 *
	 * @param sequenceName must not be {@literal null} or empty.
	 * @param customizer applied to a builder already carrying the defaults, must not be {@literal null}.
	 * @return the sequence, never {@literal null}.
	 */
	MongoSequence<Long> numericSequence(String sequenceName, Consumer<SequenceDefinition.Builder> customizer);

	/**
	 * A sequence described in full. The definition stands on its own, so the defaults of this instance do not apply -
	 * anything it does not say falls back to the {@link SequenceDefinition} defaults instead. Use
	 * {@link #numericSequence(String, Consumer)} to start from the defaults of this instance.
	 *
	 * @param definition must not be {@literal null}.
	 * @return the sequence, never {@literal null}.
	 */
	MongoSequence<Long> numericSequence(SequenceDefinition definition);
}
