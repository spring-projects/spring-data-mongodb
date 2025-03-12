/*
 * Copyright 2024-2025 the original author or authors.
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
package org.springframework.data.mongodb;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.ReadConcernAware;
import org.springframework.data.mongodb.core.ReadPreferenceAware;
import org.springframework.data.mongodb.core.WriteConcernAware;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;

/**
 * Options to be applied within a specific transaction scope.
 *
 * @author Christoph Strobl
 * @since 4.3
 */
public interface MongoTransactionOptions
		extends TransactionMetadata, ReadConcernAware, ReadPreferenceAware, WriteConcernAware {

	/**
	 * Value Object representing empty options enforcing client defaults. Returns {@literal null} for all getter methods.
	 */
	MongoTransactionOptions NONE = new MongoTransactionOptions() {

		@Override
		public @Nullable Duration getMaxCommitTime() {
			return null;
		}

		@Override
		public @Nullable ReadConcern getReadConcern() {
			return null;
		}

		@Override
		public @Nullable ReadPreference getReadPreference() {
			return null;
		}

		@Override
		public @Nullable WriteConcern getWriteConcern() {
			return null;
		}
	};

	/**
	 * Merge current options with given ones. Will return first non {@literal null} value from getters whereas the
	 * {@literal this} has precedence over the given fallbackOptions.
	 *
	 * @param fallbackOptions can be {@literal null}.
	 * @return new instance of {@link MongoTransactionOptions} or this if {@literal fallbackOptions} is {@literal null} or
	 *         {@link #NONE}.
	 */
	default MongoTransactionOptions mergeWith(@Nullable MongoTransactionOptions fallbackOptions) {

		if (fallbackOptions == null || MongoTransactionOptions.NONE.equals(fallbackOptions)) {
			return this;
		}

		return new MongoTransactionOptions() {

			@Override
			public @Nullable Duration getMaxCommitTime() {
				return MongoTransactionOptions.this.hasMaxCommitTime() ? MongoTransactionOptions.this.getMaxCommitTime()
						: fallbackOptions.getMaxCommitTime();
			}

			@Override
			public @Nullable ReadConcern getReadConcern() {
				return MongoTransactionOptions.this.hasReadConcern() ? MongoTransactionOptions.this.getReadConcern()
						: fallbackOptions.getReadConcern();
			}

			@Nullable
			@Override
			public ReadPreference getReadPreference() {
				return MongoTransactionOptions.this.hasReadPreference() ? MongoTransactionOptions.this.getReadPreference()
						: fallbackOptions.getReadPreference();
			}

			@Override
			public @Nullable WriteConcern getWriteConcern() {
				return MongoTransactionOptions.this.hasWriteConcern() ? MongoTransactionOptions.this.getWriteConcern()
						: fallbackOptions.getWriteConcern();
			}
		};
	}

	/**
	 * Apply the current options using the given mapping {@link Function} and return its result.
	 *
	 * @param mappingFunction
	 * @return result of the mapping function.
	 */
	default <T> T map(Function<MongoTransactionOptions, T> mappingFunction) {
		return mappingFunction.apply(this);
	}

	/**
	 * @return MongoDB driver native {@link TransactionOptions}.
	 * @see MongoTransactionOptions#map(Function)
	 */
	@Nullable
	default TransactionOptions toDriverOptions() {

		return map(it -> {

			if (MongoTransactionOptions.NONE.equals(it)) {
				return null;
			}

			TransactionOptions.Builder builder = TransactionOptions.builder();
			if (it.hasMaxCommitTime()) {
				builder.maxCommitTime(it.getMaxCommitTime().toMillis(), TimeUnit.MILLISECONDS);
			}
			if (it.hasReadConcern()) {
				builder.readConcern(it.getReadConcern());
			}
			if (it.hasReadPreference()) {
				builder.readPreference(it.getReadPreference());
			}
			if (it.hasWriteConcern()) {
				builder.writeConcern(it.getWriteConcern());
			}
			return builder.build();
		});
	}

	/**
	 * Factory method to wrap given MongoDB driver native {@link TransactionOptions} into {@link MongoTransactionOptions}.
	 *
	 * @param options
	 * @return {@link MongoTransactionOptions#NONE} if given object is {@literal null}.
	 */
	static MongoTransactionOptions of(@Nullable TransactionOptions options) {

		if (options == null) {
			return NONE;
		}

		return new MongoTransactionOptions() {

			@Override
			public @Nullable Duration getMaxCommitTime() {

				Long millis = options.getMaxCommitTime(TimeUnit.MILLISECONDS);
				return millis != null ? Duration.ofMillis(millis) : null;
			}

			@Override
			public @Nullable ReadConcern getReadConcern() {
				return options.getReadConcern();
			}

			@Override
			public @Nullable ReadPreference getReadPreference() {
				return options.getReadPreference();
			}

			@Override
			public @Nullable WriteConcern getWriteConcern() {
				return options.getWriteConcern();
			}

			@Override
			public @Nullable TransactionOptions toDriverOptions() {
				return options;
			}
		};
	}
}
