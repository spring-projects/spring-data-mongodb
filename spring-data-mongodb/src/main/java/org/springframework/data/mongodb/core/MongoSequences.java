/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @since 5.1
 */
// TODO: Do we want to have a MongoSequenceRegistry holding instances of sequences identified by name?
public class MongoSequences {

	private final MongoOperations mongoOperations;

	// TODO: Can we operate upon the MongoDatabaseFactory or do we need MongoOperations?
	public MongoSequences(MongoOperations mongoOperations) {
		this.mongoOperations = mongoOperations;
	}

	public MongoSequence<Long> numericSequence(String name) {
		return numericSequence(name, it -> {});
	}

	public MongoSequence<Long> numericSequence(String name, Consumer<MongoSequenceConfigurer<Long>> configurer) {

		NumericMongoSequenceBuilder longMongoSequenceBuilder = new NumericMongoSequenceBuilder(name);
		configurer.accept(longMongoSequenceBuilder);
		return longMongoSequenceBuilder.build();
	}

	// TODO: MongoSequenceConfigurer in callback vs. MongoSequenceConfiguration
	public interface MongoSequenceConfigurer<T> {

		MongoSequenceConfigurer<T> inCollection(String collectionName);

		MongoSequenceConfigurer<T> counterName(String sequenceName);

		MongoSequenceConfigurer<T> incrementBy(T value);

		MongoSequenceConfigurer<T> startWith(T startValue);
	}

	interface MongoSequenceBuilder<T> extends MongoSequenceConfigurer<T> {
		MongoSequence<T> build();
	}

	class NumericMongoSequenceBuilder implements MongoSequenceBuilder<Long> {

		private String collectionName = "__sequences";
		private String sequenceName;
		private String valueFieldName = "counter";
		private @Nullable Long startValue;
		private long increment = 1;

		public NumericMongoSequenceBuilder(String sequenceName) {
			this.sequenceName = sequenceName;
		}

		@Override
		public MongoSequenceBuilder<Long> inCollection(String collectionName) {

			this.collectionName = collectionName;
			return this;
		}

		@Override
		public MongoSequenceConfigurer<Long> counterName(String valueFieldName) {

			this.valueFieldName = valueFieldName;
			return this;
		}

		@Override
		public MongoSequenceBuilder<Long> incrementBy(Long value) {

			Assert.notNull(value, "Value must not be null");

			this.increment = value;
			return this;
		}

		@Override
		public MongoSequenceBuilder<Long> startWith(Long startValue) {

			this.startValue = startValue;
			return this;
		}

		@Override
		public MongoSequence<Long> build() {
			return new NumericMongoSequence(mongoOperations, collectionName, sequenceName, valueFieldName, startValue,
					increment);
		}
	}

}
