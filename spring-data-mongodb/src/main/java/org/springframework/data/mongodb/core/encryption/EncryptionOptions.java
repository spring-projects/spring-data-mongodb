/*
 * Copyright 2023-present the original author or authors.
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

import java.util.Map;
import java.util.Objects;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Options used to provide additional information when {@link Encryption encrypting} values. like the
 * {@link #algorithm()} to be used.
 * 
 * @author Christoph Strobl
 * @author Ross Lawley
 * @since 4.1
 */
public class EncryptionOptions {

	private final String algorithm;
	private final EncryptionKey key;
	private final @Nullable QueryableEncryptionOptions queryableEncryptionOptions;

	public EncryptionOptions(String algorithm, EncryptionKey key) {
		this(algorithm, key, null);
	}

	public EncryptionOptions(String algorithm, EncryptionKey key,
			@Nullable QueryableEncryptionOptions queryableEncryptionOptions) {

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

	/**
	 * @return {@literal null} if not set.
	 * @since 4.5
	 */
	public @Nullable QueryableEncryptionOptions queryableEncryptionOptions() {
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
	 * @author Christoph Strobl
	 * @since 4.5
	 */
	public static class QueryableEncryptionOptions {

		private static final QueryableEncryptionOptions NONE = new QueryableEncryptionOptions(null, null, Map.of());

		private final @Nullable String queryType;
		private final @Nullable Long contentionFactor;
		private final Map<String, Object> attributes;

		private QueryableEncryptionOptions(@Nullable String queryType, @Nullable Long contentionFactor,
				Map<String, Object> attributes) {

			this.queryType = queryType;
			this.contentionFactor = contentionFactor;
			this.attributes = attributes;
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
			return new QueryableEncryptionOptions(queryType, contentionFactor, attributes);
		}

		/**
		 * Define the {@code contentionFactor} to be used for queryable document encryption.
		 *
		 * @param contentionFactor can be {@literal null}.
		 * @return new instance of {@link QueryableEncryptionOptions}.
		 */
		public QueryableEncryptionOptions contentionFactor(@Nullable Long contentionFactor) {
			return new QueryableEncryptionOptions(queryType, contentionFactor, attributes);
		}

		/**
		 * Define the {@code rangeOptions} to be used for queryable document encryption.
		 *
		 * @param attributes can be {@literal null}.
		 * @return new instance of {@link QueryableEncryptionOptions}.
		 */
		public QueryableEncryptionOptions attributes(Map<String, Object> attributes) {
			return new QueryableEncryptionOptions(queryType, contentionFactor, attributes);
		}

		/**
		 * Get the {@code queryType} to apply.
		 *
		 * @return {@literal null} if not set.
		 */
		public @Nullable String getQueryType() {
			return queryType;
		}

		/**
		 * Get the {@code contentionFactor} to apply.
		 *
		 * @return {@literal null} if not set.
		 */
		public @Nullable Long getContentionFactor() {
			return contentionFactor;
		}

		/**
		 * Get the {@code rangeOptions} to apply.
		 *
		 * @return never {@literal null}.
		 */
		public Map<String, Object> getAttributes() {
			return Map.copyOf(attributes);
		}

		/**
		 * @return {@literal true} if no arguments set.
		 */
		boolean isEmpty() {
			return getQueryType() == null && getContentionFactor() == null && getAttributes().isEmpty();
		}

		@Override
		public String toString() {
			return "QueryableEncryptionOptions{" + "queryType='" + queryType + '\'' + ", contentionFactor=" + contentionFactor
					+ ", attributes=" + attributes + '}';
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
			return ObjectUtils.nullSafeEquals(attributes, that.attributes);
		}

		@Override
		public int hashCode() {
			return Objects.hash(queryType, contentionFactor, attributes);
		}
	}
}
