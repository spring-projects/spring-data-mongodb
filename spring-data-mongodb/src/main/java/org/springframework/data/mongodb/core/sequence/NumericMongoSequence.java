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

import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.util.Assert;

/**
 * A {@link MongoSequence} of {@link Long} values.
 * <p>
 * Holds no state of its own: every call goes to the {@link SequenceExecutor}, which is what makes the same sequence
 * safe to use from several instances of an application at once. Instances are therefore cheap and thread safe.
 *
 * @author Jeongkyun An
 * @since 5.2
 */
class NumericMongoSequence implements MongoSequence<Long> {

	private final SequenceDefinition definition;
	private final SequenceExecutor executor;
	private final MongoDatabaseFactory dbFactory;

	NumericMongoSequence(SequenceDefinition definition, SequenceExecutor executor, MongoDatabaseFactory dbFactory) {

		Assert.notNull(definition, "SequenceDefinition must not be null");
		Assert.notNull(executor, "SequenceExecutor must not be null");
		Assert.notNull(dbFactory, "MongoDatabaseFactory must not be null");

		this.definition = definition;
		this.executor = executor;
		this.dbFactory = dbFactory;
	}

	@Override
	public Long nextValue() {
		return executor.nextValue(definition, dbFactory);
	}

	@Override
	public String getName() {
		return definition.getName();
	}

	/**
	 * @return the definition this sequence was obtained with, never {@literal null}.
	 */
	SequenceDefinition getDefinition() {
		return definition;
	}

	/**
	 * @return the executor carrying out the increments, never {@literal null}.
	 */
	SequenceExecutor getExecutor() {
		return executor;
	}
}
