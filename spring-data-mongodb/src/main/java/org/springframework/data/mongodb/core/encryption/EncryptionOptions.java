/*
 * Copyright 2023-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.encryption;

import java.util.Objects;
import java.util.Optional;

import com.mongodb.client.model.vault.RangeOptions;
import org.bson.Document;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.util.Optionals;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Options, like the {@link #algorithm()}, to apply when encrypting values.
 *
 * @author Christoph Strobl
 * @author Ross Lawley
 * @since 4.1
 */
public class EncryptionOptions {

	private final String algorithm;
	private final EncryptionKey key;
	private final QueryableEncryptionOptions queryableEncryptionOptions;

	public EncryptionOptions(String algorithm, EncryptionKey key) {
		this(algorithm, key, QueryableEncryptionOptions.NONE);
	}

	public EncryptionOptions(String algorithm, EncryptionKey key, QueryableEncryptionOptions queryableEncryptionOptions) {
		Assert.hasText(algorithm, "Algorithm must not be empty");
		Assert.notNull(key, "EncryptionKey must not be empty");
		Assert.notNull(key, "QueryableEncryptionOptions must not be empty");

		this.key = key;
		this.algorithm = algorithm;
		this.queryableEncryptionOptions = queryableEncryptionOptions;
	}

	public EncryptionKey key() {
		return key;
	}

	public String algorithm() {
		return algorithm;
	}

	public QueryableEncryptionOptions queryableEncryptionOptions() {
		return queryableEncryptionOptions;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		EncryptionOptions that = (EncryptionOptions) o;

		if (!ObjectUtils.nullSafeEquals(algorithm, that.algorithm)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(key, that.key)) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(queryableEncryptionOptions, that.queryableEncryptionOptions);
	}

	@Override
	public int hashCode() {

		int result = ObjectUtils.nullSafeHashCode(algorithm);
		result = 31 * result + ObjectUtils.nullSafeHashCode(key);
		result = 31 * result + ObjectUtils.nullSafeHashCode(queryableEncryptionOptions);
		return result;
	}

	@Override
	public String toString() {
		return "EncryptionOptions{" + "algorithm='" + algorithm + '\'' + ", key=" + key + ", queryableEncryptionOptions='"
				+ queryableEncryptionOptions + "'}";
	}

	/**
	 * Options, like the {@link #getQueryType()}, to apply when encrypting queryable values.
	 *
	 * @author Ross Lawley
	 */
	public static class QueryableEncryptionOptions {

		private static final QueryableEncryptionOptions NONE = new QueryableEncryptionOptions(null, null, null);

		private final @Nullable String queryType;
		private final @Nullable Long contentionFactor;
		private final @Nullable Document rangeOptions;

		private QueryableEncryptionOptions(@Nullable String queryType, @Nullable Long contentionFactor,
				@Nullable Document rangeOptions) {
			this.queryType = queryType;
			this.contentionFactor = contentionFactor;
			this.rangeOptions = rangeOptions;
		}

		/**
		 * Create an empty {@link QueryableEncryptionOptions}.
		 *
		 * @return unmodifiable {@link QueryableEncryptionOptions} instance.
		 */
		public static QueryableEncryptionOptions none() {
			return NONE;
		}

		/**
		 * Define the {@code queryType} to be used for queryable document encryption.
		 *
		 * @param queryType can be {@literal null}.
		 * @return new instance of {@link QueryableEncryptionOptions}.
		 */
		public QueryableEncryptionOptions queryType(@Nullable String queryType) {
			return new QueryableEncryptionOptions(queryType, contentionFactor, rangeOptions);
		}

		/**
		 * Define the {@code contentionFactor} to be used for queryable document encryption.
		 *
		 * @param contentionFactor can be {@literal null}.
		 * @return new instance of {@link QueryableEncryptionOptions}.
		 */
		public QueryableEncryptionOptions contentionFactor(@Nullable Long contentionFactor) {
			return new QueryableEncryptionOptions(queryType, contentionFactor, rangeOptions);
		}

		/**
		 * Define the {@code rangeOptions} to be used for queryable document encryption.
		 *
		 * @param rangeOptions can be {@literal null}.
		 * @return new instance of {@link QueryableEncryptionOptions}.
		 */
		public QueryableEncryptionOptions rangeOptions(@Nullable Document rangeOptions) {
			return new QueryableEncryptionOptions(queryType, contentionFactor, rangeOptions);
		}

		/**
		 * Get the {@code queryType} to apply.
		 *
		 * @return {@link Optional#empty()} if not set.
		 */
		public Optional<String> getQueryType() {
			return Optional.ofNullable(queryType);
		}

		/**
		 * Get the {@code contentionFactor} to apply.
		 *
		 * @return {@link Optional#empty()} if not set.
		 */
		public Optional<Long> getContentionFactor() {
			return Optional.ofNullable(contentionFactor);
		}

		/**
		 * Get the {@code rangeOptions} to apply.
		 *
		 * @return {@link Optional#empty()} if not set.
		 */
		public Optional<RangeOptions> getRangeOptions() {
			if (rangeOptions == null) {
				return Optional.empty();
			}
			RangeOptions encryptionRangeOptions = new RangeOptions();

			if (rangeOptions.containsKey("min")) {
				encryptionRangeOptions.min(BsonUtils.simpleToBsonValue(rangeOptions.get("min")));
			}
			if (rangeOptions.containsKey("max")) {
				encryptionRangeOptions.max(BsonUtils.simpleToBsonValue(rangeOptions.get("max")));
			}
			if (rangeOptions.containsKey("trimFactor")) {
				Object trimFactor = rangeOptions.get("trimFactor");
				Assert.isInstanceOf(Integer.class, trimFactor, () -> String
						.format("Expected to find a %s but it turned out to be %s.", Integer.class, trimFactor.getClass()));

				encryptionRangeOptions.trimFactor((Integer) trimFactor);
			}

			if (rangeOptions.containsKey("sparsity")) {
				Object sparsity = rangeOptions.get("sparsity");
				Assert.isInstanceOf(Number.class, sparsity,
						() -> String.format("Expected to find a %s but it turned out to be %s.", Long.class, sparsity.getClass()));
				encryptionRangeOptions.sparsity(((Number) sparsity).longValue());
			}

			if (rangeOptions.containsKey("precision")) {
				Object precision = rangeOptions.get("precision");
				Assert.isInstanceOf(Number.class, precision, () -> String
						.format("Expected to find a %s but it turned out to be %s.", Integer.class, precision.getClass()));
				encryptionRangeOptions.precision(((Number) precision).intValue());
			}
			return Optional.of(encryptionRangeOptions);
		}

		/**
		 * @return {@literal true} if no arguments set.
		 */
		boolean isEmpty() {
			return !Optionals.isAnyPresent(getQueryType(), getContentionFactor(), getRangeOptions());
		}

		@Override
		public String toString() {
			return "QueryableEncryptionOptions{" + "queryType='" + queryType + '\'' + ", contentionFactor=" + contentionFactor
					+ ", rangeOptions=" + rangeOptions + '}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			QueryableEncryptionOptions that = (QueryableEncryptionOptions) o;

			if (!ObjectUtils.nullSafeEquals(queryType, that.queryType)) {
				return false;
			}

			if (!ObjectUtils.nullSafeEquals(contentionFactor, that.contentionFactor)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(rangeOptions, that.rangeOptions);
		}

		@Override
		public int hashCode() {
			return Objects.hash(queryType, contentionFactor, rangeOptions);
		}
	}
}
