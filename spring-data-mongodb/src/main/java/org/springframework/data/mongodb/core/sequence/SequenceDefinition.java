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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * Describes a sequence: where it lives, how it counts, and how it is laid out as a document.
 * <p>
 * Instances are immutable. Start from {@link #of(String)} for the defaults - a counter in the {@code sequences}
 * collection of the default database, starting at one and going up by one - and use {@link #builder(String)} or the
 * {@code with} methods to depart from them.
 *
 * @author Jeongkyun An
 * @since 5.2
 */
public class SequenceDefinition {

	static final String DEFAULT_COLLECTION_NAME = "sequences";

	private final String name;
	private final @Nullable String databaseName;
	private final String collectionName;
	private final long startValue;
	private final long increment;
	private final boolean transactional;
	private final SequenceStrategy strategy;
	private final DefinitionPolicy definitionPolicy;
	private final Map<String, Object> attributes;

	private SequenceDefinition(String name, @Nullable String databaseName, String collectionName, long startValue,
			long increment, boolean transactional, SequenceStrategy strategy, DefinitionPolicy definitionPolicy,
			Map<String, Object> attributes) {

		this.name = name;
		this.databaseName = databaseName;
		this.collectionName = collectionName;
		this.startValue = startValue;
		this.increment = increment;
		this.transactional = transactional;
		this.strategy = strategy;
		this.definitionPolicy = definitionPolicy;
		this.attributes = attributes;
	}

	/**
	 * A sequence with the given name, using the defaults.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return new instance of {@link SequenceDefinition}, never {@literal null}.
	 */
	public static SequenceDefinition of(String name) {
		return builder(name).build();
	}

	/**
	 * Start building a sequence with the given name.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return new instance of {@link Builder}, never {@literal null}.
	 */
	public static Builder builder(String name) {

		Assert.hasText(name, "Sequence name must not be null or empty");

		return new Builder(name);
	}

	public String getName() {
		return name;
	}

	/**
	 * @return the database holding the sequence, or {@literal null} to use the one the factory is bound to.
	 */
	public @Nullable String getDatabaseName() {
		return databaseName;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public long getStartValue() {
		return startValue;
	}

	public long getIncrement() {
		return increment;
	}

	/**
	 * @return {@literal true} if the increment should join an ongoing transaction. Doing so keeps the sequence free of
	 *         gaps, at the price of handing back values that a rollback makes available again.
	 */
	public boolean isTransactional() {
		return transactional;
	}

	public SequenceStrategy getStrategy() {
		return strategy;
	}

	public DefinitionPolicy getDefinitionPolicy() {
		return definitionPolicy;
	}

	/**
	 * @return extra fields to store alongside the counter, never {@literal null}.
	 */
	public Map<String, Object> getAttributes() {
		return attributes;
	}

	/**
	 * Whether two definitions describe a sequence that counts the same way. Used to reconcile a stored definition with a
	 * local one, so it deliberately ignores where the sequence lives and what is stored beside it.
	 *
	 * @param other must not be {@literal null}.
	 * @return {@literal true} if both count identically.
	 */
	boolean describesSameSequenceAs(SequenceDefinition other) {
		return this.startValue == other.startValue && this.increment == other.increment;
	}

	/**
	 * @param collectionName must not be {@literal null} or empty.
	 * @return new instance of {@link SequenceDefinition}, never {@literal null}.
	 */
	public SequenceDefinition withCollection(String collectionName) {
		return builder(this).collection(collectionName).build();
	}

	/**
	 * @param increment must be greater than zero.
	 * @return new instance of {@link SequenceDefinition}, never {@literal null}.
	 */
	public SequenceDefinition withIncrement(long increment) {
		return builder(this).increment(increment).build();
	}

	/**
	 * @param strategy must not be {@literal null}.
	 * @return new instance of {@link SequenceDefinition}, never {@literal null}.
	 */
	public SequenceDefinition withStrategy(SequenceStrategy strategy) {
		return builder(this).strategy(strategy).build();
	}

	static Builder builder(SequenceDefinition source) {

		Builder builder = new Builder(source.name);
		builder.databaseName = source.databaseName;
		builder.collectionName = source.collectionName;
		builder.startValue = source.startValue;
		builder.increment = source.increment;
		builder.transactional = source.transactional;
		builder.strategy = source.strategy;
		builder.definitionPolicy = source.definitionPolicy;
		builder.attributes.putAll(source.attributes);

		return builder;
	}

	@Override
	public String toString() {
		return "SequenceDefinition[%s in %s, start %d, increment %d]".formatted(name, collectionName, startValue,
				increment);
	}

	/**
	 * Builder for {@link SequenceDefinition}.
	 *
	 * @author Jeongkyun An
	 * @since 5.2
	 */
	public static class Builder {

		private final String name;
		private @Nullable String databaseName;
		private String collectionName = DEFAULT_COLLECTION_NAME;
		private long startValue = 1;
		private long increment = 1;
		private boolean transactional;
		private SequenceStrategy strategy = new CounterSequenceStrategy();
		private DefinitionPolicy definitionPolicy = DefinitionPolicy.validate();
		private final Map<String, Object> attributes = new LinkedHashMap<>();

		Builder(String name) {
			this.name = name;
		}

		/**
		 * The database holding the sequence. Defaults to the one the factory is bound to.
		 *
		 * @param databaseName must not be {@literal null} or empty.
		 */
		public Builder database(String databaseName) {

			Assert.hasText(databaseName, "Database name must not be null or empty");

			this.databaseName = databaseName;
			return this;
		}

		/**
		 * @param collectionName must not be {@literal null} or empty.
		 */
		public Builder collection(String collectionName) {

			Assert.hasText(collectionName, "Collection name must not be null or empty");

			this.collectionName = collectionName;
			return this;
		}

		/**
		 * The value handed out first. Defaults to {@literal 1}.
		 */
		public Builder startWith(long startValue) {

			this.startValue = startValue;
			return this;
		}

		/**
		 * The step between values. Defaults to {@literal 1}.
		 *
		 * @param increment must be greater than zero.
		 */
		public Builder increment(long increment) {

			Assert.isTrue(increment > 0, () -> "Increment must be positive, but was: " + increment);

			this.increment = increment;
			return this;
		}

		/**
		 * Let the increment join an ongoing transaction. Off by default, so a rollback does not put a value back into
		 * circulation.
		 *
		 * @see SequenceDefinition#isTransactional()
		 */
		public Builder transactional(boolean transactional) {

			this.transactional = transactional;
			return this;
		}

		/**
		 * @param strategy must not be {@literal null}.
		 */
		public Builder strategy(SequenceStrategy strategy) {

			Assert.notNull(strategy, "SequenceStrategy must not be null");

			this.strategy = strategy;
			return this;
		}

		/**
		 * Only consulted by a self describing {@link SequenceStrategy}.
		 *
		 * @param definitionPolicy must not be {@literal null}.
		 */
		public Builder definitionPolicy(DefinitionPolicy definitionPolicy) {

			Assert.notNull(definitionPolicy, "DefinitionPolicy must not be null");

			this.definitionPolicy = definitionPolicy;
			return this;
		}

		/**
		 * An extra field to store alongside the counter.
		 *
		 * @param key must not be {@literal null} or empty.
		 */
		public Builder attribute(String key, Object value) {

			Assert.hasText(key, "Attribute key must not be null or empty");

			this.attributes.put(key, value);
			return this;
		}

		/**
		 * @return new instance of {@link SequenceDefinition}, never {@literal null}.
		 */
		public SequenceDefinition build() {
			return new SequenceDefinition(name, databaseName, collectionName, startValue, increment, transactional,
					strategy, definitionPolicy, Collections.unmodifiableMap(new LinkedHashMap<>(attributes)));
		}
	}
}
