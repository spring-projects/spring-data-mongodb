/*
 * Copyright 2023 the original author or authors.
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

import java.util.function.Function;

import org.bson.conversions.Bson;
import org.springframework.data.mongodb.CodecRegistryProvider;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * Function object to apply a query hint. Can be an index name or a BSON document.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 4.1
 */
class HintFunction {

	private static final HintFunction EMPTY = new HintFunction(null);

	private final @Nullable Object hint;

	private HintFunction(@Nullable Object hint) {
		this.hint = hint;
	}

	/**
	 * Return an empty hint function.
	 *
	 * @return
	 */
	static HintFunction empty() {
		return EMPTY;
	}

	/**
	 * Create a {@link HintFunction} from a {@link Bson document} or {@link String index name}.
	 *
	 * @param hint
	 * @return
	 */
	static HintFunction from(@Nullable Object hint) {
		return new HintFunction(hint);
	}

	/**
	 * Return whether a hint is present.
	 *
	 * @return
	 */
	public boolean isPresent() {
		return (hint instanceof String hintString && StringUtils.hasText(hintString)) || hint instanceof Bson;
	}

	/**
	 * If a hint is not present, returns {@code true}, otherwise {@code false}.
	 *
	 * @return {@code true} if a hint is not present, otherwise {@code false}.
	 */
	public boolean isEmpty() {
		return !isPresent();
	}

	/**
	 * Apply the hint to consumers depending on the hint format if {@link #isPresent() present}.
	 *
	 * @param registryProvider
	 * @param stringConsumer
	 * @param bsonConsumer
	 * @param <R>
	 */
	public <R> void ifPresent(@Nullable CodecRegistryProvider registryProvider, Function<String, R> stringConsumer,
			Function<Bson, R> bsonConsumer) {

		if (isEmpty()) {
			return;
		}
		apply(registryProvider, stringConsumer, bsonConsumer);
	}

	/**
	 * Apply the hint to consumers depending on the hint format.
	 *
	 * @param registryProvider
	 * @param stringConsumer
	 * @param bsonConsumer
	 * @return
	 * @param <R>
	 */
	public <R> R apply(@Nullable CodecRegistryProvider registryProvider, Function<String, R> stringConsumer,
			Function<Bson, R> bsonConsumer) {

		if (isEmpty()) {
			throw new IllegalStateException("No hint present");
		}

		if (hint instanceof Bson bson) {
			return bsonConsumer.apply(bson);
		}

		if (hint instanceof String hintString) {

			if (BsonUtils.isJsonDocument(hintString)) {
				return bsonConsumer.apply(BsonUtils.parse(hintString, registryProvider));
			}
			return stringConsumer.apply(hintString);
		}

		throw new IllegalStateException(
				"Unable to read hint of type %s".formatted(hint != null ? hint.getClass() : "null"));
	}

}
