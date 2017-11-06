/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb;

import java.util.Optional;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.util.Assert;

/**
 * Provider interface to obtain {@link CodecRegistry} from the underlying MongoDB Java driver.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
@FunctionalInterface
public interface CodecRegistryProvider {

	/**
	 * Get the underlying {@link CodecRegistry} used by the MongoDB Java driver.
	 *
	 * @return never {@literal null}.
	 * @throws IllegalStateException if {@link CodecRegistry} cannot be obtained.
	 */
	CodecRegistry getCodecRegistry();

	/**
	 * Checks if a {@link Codec} is registered for a given type.
	 *
	 * @param type must not be {@literal null}.
	 * @return true if {@link #getCodecRegistry()} holds a {@link Codec} for given type.
	 * @throws IllegalStateException if {@link CodecRegistry} cannot be obtained.
	 */
	default boolean hasCodecFor(Class<?> type) {
		return getCodecFor(type).isPresent();
	}

	/**
	 * Get the {@link Codec} registered for the given {@literal type} or an {@link Optional#empty() empty Optional}
	 * instead.
	 *
	 * @param type must not be {@literal null}.
	 * @param <T>
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException if {@literal type} is {@literal null}.
	 */
	default <T> Optional<Codec<T>> getCodecFor(Class<T> type) {

		Assert.notNull(type, "Type must not be null!");

		try {
			return Optional.of(getCodecRegistry().get(type));
		} catch (CodecConfigurationException e) {
			// ignore
		}
		return Optional.empty();
	}
}
