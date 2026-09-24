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
import java.util.function.Function;

import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.util.Assert;

/**
 * Default {@link MongoSequences} implementation, pairing the defaults collected by {@link MongoSequencesBuilder} with
 * the {@link SequenceExecutor} that carries the increments out.
 *
 * @author Jeongkyun An
 * @since 5.2
 */
class DefaultMongoSequences implements MongoSequences {

	private final MongoDatabaseFactory dbFactory;
	private final SequenceExecutor executor;
	private final Function<String, SequenceDefinition.Builder> defaults;

	DefaultMongoSequences(MongoDatabaseFactory dbFactory, SequenceExecutor executor,
			Function<String, SequenceDefinition.Builder> defaults) {

		Assert.notNull(dbFactory, "MongoDatabaseFactory must not be null");
		Assert.notNull(executor, "SequenceExecutor must not be null");
		Assert.notNull(defaults, "Defaults must not be null");

		this.dbFactory = dbFactory;
		this.executor = executor;
		this.defaults = defaults;
	}

	@Override
	public MongoSequence<Long> numericSequence(String sequenceName) {
		return numericSequence(sequenceName, it -> {});
	}

	@Override
	public MongoSequence<Long> numericSequence(String sequenceName, Consumer<SequenceDefinition.Builder> customizer) {

		Assert.hasText(sequenceName, "Sequence name must not be null or empty");
		Assert.notNull(customizer, "Customizer must not be null");

		SequenceDefinition.Builder builder = defaults.apply(sequenceName);
		customizer.accept(builder);

		return numericSequence(builder.build());
	}

	@Override
	public MongoSequence<Long> numericSequence(SequenceDefinition definition) {

		Assert.notNull(definition, "SequenceDefinition must not be null");

		return new NumericMongoSequence(definition, executorFor(definition), dbFactory);
	}

	/**
	 * A definition asking to take part in transactions overrides the configured executor, since that is the whole point
	 * of saying so. Anything else uses what the builder was given.
	 */
	private SequenceExecutor executorFor(SequenceDefinition definition) {
		return definition.isTransactional() ? new SessionBoundSequenceExecutor() : executor;
	}
}
