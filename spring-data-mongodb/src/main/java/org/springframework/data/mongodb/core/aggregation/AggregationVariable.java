/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * A special field that points to a variable {@code $$} expression.
 *
 * @author Christoph Strobl
 * @since 4.1.3
 */
public interface AggregationVariable extends Field {

	String PREFIX = "$$";

	/**
	 * @return {@literal true} if the fields {@link #getName() name} does not match the defined {@link #getTarget()
	 *         target}.
	 */
	@Override
	default boolean isAliased() {
		return !ObjectUtils.nullSafeEquals(getName(), getTarget());
	}

	@Override
	default String getName() {
		return getTarget();
	}

	@Override
	default boolean isInternal() {
		return false;
	}

	/**
	 * Create a new {@link AggregationVariable} for the given name.
	 * <p>
	 * Variables start with {@code $$}. If not, the given value gets prefixed with {@code $$}.
	 *
	 * @param value must not be {@literal null}.
	 * @return new instance of {@link AggregationVariable}.
	 * @throws IllegalArgumentException if given value is {@literal null}.
	 */
	static AggregationVariable variable(String value) {

		Assert.notNull(value, "Value must not be null");
		return new AggregationVariable() {

			private final String val = AggregationVariable.prefixVariable(value);

			@Override
			public String getTarget() {
				return val;
			}
		};
	}

	/**
	 * Create a new {@link #isInternal() local} {@link AggregationVariable} for the given name.
	 * <p>
	 * Variables start with {@code $$}. If not, the given value gets prefixed with {@code $$}.
	 *
	 * @param value must not be {@literal null}.
	 * @return new instance of {@link AggregationVariable}.
	 * @throws IllegalArgumentException if given value is {@literal null}.
	 */
	static AggregationVariable localVariable(String value) {

		Assert.notNull(value, "Value must not be null");
		return new AggregationVariable() {

			private final String val = AggregationVariable.prefixVariable(value);

			@Override
			public String getTarget() {
				return val;
			}

			@Override
			public boolean isInternal() {
				return true;
			}
		};
	}

	/**
	 * Check if the given field name reference may be variable.
	 *
	 * @param fieldRef can be {@literal null}.
	 * @return true if given value matches the variable identification pattern.
	 */
	static boolean isVariable(@Nullable String fieldRef) {
		return fieldRef != null && fieldRef.stripLeading().matches("^\\$\\$\\w.*");
	}

	/**
	 * Check if the given field may be variable.
	 *
	 * @param field can be {@literal null}.
	 * @return true if given {@link Field field} is an {@link AggregationVariable} or if its value is a
	 *         {@link #isVariable(String) variable}.
	 */
	static boolean isVariable(Field field) {

		if (field instanceof AggregationVariable) {
			return true;
		}
		return isVariable(field.getTarget());
	}

	private static String prefixVariable(String variable) {

		var trimmed = variable.stripLeading();
		return trimmed.startsWith(PREFIX) ? trimmed : (PREFIX + trimmed);
	}

}
