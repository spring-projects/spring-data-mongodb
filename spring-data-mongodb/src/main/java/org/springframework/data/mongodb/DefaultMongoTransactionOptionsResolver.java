/*
 * Copyright 2024 the original author or authors.
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

import java.util.Map;
import java.util.Set;

import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;

/**
 * Default implementation of {@link MongoTransactionOptions} using {@literal mongo:} as {@link #getLabelPrefix() label
 * prefix} creating {@link SimpleMongoTransactionOptions} out of a given argument {@link Map}. Uses
 * {@link SimpleMongoTransactionOptions#KNOWN_KEYS} to validate entries in arguments to resolve and errors on unknown
 * entries.
 *
 * @author Christoph Strobl
 * @since 4.3
 */
class DefaultMongoTransactionOptionsResolver implements MongoTransactionOptionsResolver {

	static final Lazy<MongoTransactionOptionsResolver> INSTANCE = Lazy.of(DefaultMongoTransactionOptionsResolver::new);

	private static final String PREFIX = "mongo:";

	private DefaultMongoTransactionOptionsResolver() {}

	@Override
	public MongoTransactionOptions convert(Map<String, String> options) {

		validateKeys(options.keySet());
		return SimpleMongoTransactionOptions.of(options);
	}

	@Nullable
	@Override
	public String getLabelPrefix() {
		return PREFIX;
	}

	private static void validateKeys(Set<String> keys) {

		if (!keys.stream().allMatch(SimpleMongoTransactionOptions.KNOWN_KEYS::contains)) {

			throw new IllegalArgumentException("Transaction labels contained invalid values. Has to be one of %s"
					.formatted(SimpleMongoTransactionOptions.KNOWN_KEYS));
		}
	}
}
