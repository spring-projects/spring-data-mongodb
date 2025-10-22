/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.mongodb.observability;

import io.micrometer.common.KeyValue;
import io.micrometer.common.docs.KeyName;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

import org.springframework.util.StringUtils;

/**
 * Value object representing an observation key name for MongoDB operations. It allows easier transformation to
 * {@link KeyValue} and {@link KeyName}.
 *
 * @author Mark Paluch
 * @since 4.4.9
 * @deprecated since 5.0 in favor of native MongoDB Java Driver observability support.
 */
@Deprecated(since = "5.0",  forRemoval = true)
record MongoKeyName<C>(String name, boolean required, Function<C, @Nullable Object> valueFunction) implements KeyName {

	/**
	 * Creates a required {@link MongoKeyName} along with a contextual value function to extract the value from the
	 * context. The value defaults to {@link KeyValue#NONE_VALUE} if the contextual value function returns
	 * {@literal null}.
	 *
	 * @param name
	 * @param valueFunction
	 * @return
	 * @param <C>
	 */
	static <C> MongoKeyName<C> required(String name, Function<C, @Nullable Object> valueFunction) {
		return required(name, valueFunction, Objects::nonNull);
	}

	/**
	 * Creates a required {@link MongoKeyName} along with a contextual value function to extract the value from the
	 * context. The value defaults to {@link KeyValue#NONE_VALUE} if the contextual value function returns {@literal null}
	 * or an empty {@link String}.
	 *
	 * @param name
	 * @param valueFunction
	 * @return
	 * @param <C>
	 */
	public static <C> MongoKeyName<C> requiredString(String name, Function<C, @Nullable String> valueFunction) {
		return required(name, valueFunction, StringUtils::hasText);
	}

	/**
	 * Creates a required {@link MongoKeyName} along with a contextual value function to extract the value from the
	 * context. The value defaults to {@link KeyValue#NONE_VALUE} if the contextual value function returns
	 * {@literal null}.
	 *
	 * @param name
	 * @param valueFunction
	 * @param hasValue predicate to determine if the value is present.
	 * @return
	 * @param <C>
	 */
	public static <C, V extends @Nullable Object> MongoKeyName<C> required(String name, Function<C, V> valueFunction,
			Predicate<V> hasValue) {
		return new MongoKeyName<>(name, true, c -> {
			V value = valueFunction.apply(c);
			return hasValue.test(value) ? value : null;
		});
	}

	/**
	 * Creates a required {@link MongoKeyValue} with a constant value.
	 *
	 * @param name
	 * @param value
	 * @return
	 */
	public static MongoKeyValue just(String name, String value) {
		return new MongoKeyName<>(name, false, it -> value).withValue(value);
	}

	/**
	 * Create a new {@link MongoKeyValue} with a given value.
	 *
	 * @param value value for key
	 * @return
	 */
	@Override
	public MongoKeyValue withValue(String value) {
		return new MongoKeyValue(this, value);
	}

	/**
	 * Create a new {@link MongoKeyValue} from the context. If the context is {@literal null}, the value will be
	 * {@link KeyValue#NONE_VALUE}.
	 *
	 * @param context
	 * @return
	 */
	public MongoKeyValue valueOf(@Nullable C context) {

		Object value = context != null ? valueFunction.apply(context) : null;
		return new MongoKeyValue(this, value == null ? KeyValue.NONE_VALUE : value.toString());
	}

	/**
	 * Create a new absent {@link MongoKeyValue} with the {@link KeyValue#NONE_VALUE} as value.
	 *
	 * @return
	 */
	public MongoKeyValue absent() {
		return new MongoKeyValue(this, KeyValue.NONE_VALUE);
	}

	@Override
	public boolean isRequired() {
		return required;
	}

	@Override
	public String asString() {
		return name;
	}

	@Override
	public String toString() {
		return "Key: " + asString();
	}

	/**
	 * Value object representing an observation key and value for MongoDB operations. It allows easier transformation to
	 * {@link KeyValue} and {@link KeyName}.
	 */
	static class MongoKeyValue implements KeyName, KeyValue {

		private final KeyName keyName;
		private final String value;

		MongoKeyValue(KeyName keyName, String value) {
			this.keyName = keyName;
			this.value = value;
		}

		@Override
		public String getKey() {
			return keyName.asString();
		}

		@Override
		public String getValue() {
			return value;
		}

		@Override
		public String asString() {
			return getKey();
		}

		@Override
		public String toString() {
			return getKey() + "=" + getValue();
		}
	}

}
