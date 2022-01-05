/*
 * Copyright 2019-2021 the original author or authors.
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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.mongodb.core.aggregation.SetOperation.FieldAppender.ValueAppender;
import org.springframework.lang.Nullable;

/**
 * Adds new fields to documents. {@code $set} outputs documents that contain all existing fields from the input
 * documents and newly added fields.
 *
 * <pre class="code">
 * SetOperation.set("totalHomework").toValue("A+").and().set("totalQuiz").toValue("B-")
 * </pre>
 *
 * @author Christoph Strobl
 * @since 3.0
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/set/">MongoDB Aggregation Framework:
 *      $set</a>
 */
public class SetOperation extends DocumentEnhancingOperation {

	/**
	 * Create new instance of {@link SetOperation} adding map keys as exposed fields.
	 *
	 * @param source must not be {@literal null}.
	 */
	private SetOperation(Map<Object, Object> source) {
		super(source);
	}

	/**
	 * Create new instance of {@link SetOperation}
	 *
	 * @param field must not be {@literal null}.
	 * @param value can be {@literal null}.
	 */
	public SetOperation(Object field, @Nullable Object value) {
		this(Collections.singletonMap(field, value));
	}

	/**
	 * Define the {@link SetOperation} via {@link FieldAppender}.
	 *
	 * @return new instance of {@link FieldAppender}.
	 */
	public static FieldAppender builder() {
		return new FieldAppender();
	}

	/**
	 * Concatenate another field to set.
	 *
	 * @param field must not be {@literal null}.
	 * @return new instance of {@link ValueAppender}.
	 */
	public static ValueAppender set(String field) {
		return new FieldAppender().set(field);
	}

	/**
	 * Append the value for a specific field to the operation.
	 *
	 * @param field the target field to set.
	 * @param value the value to assign.
	 * @return new instance of {@link SetOperation}.
	 */
	public SetOperation set(Object field, Object value) {

		LinkedHashMap<Object, Object> target = new LinkedHashMap<>(getValueMap());
		target.put(field, value);

		return new SetOperation(target);
	}

	/**
	 * Concatenate additional fields to set.
	 *
	 * @return new instance of {@link FieldAppender}.
	 */
	public FieldAppender and() {
		return new FieldAppender(getValueMap());
	}

	@Override
	protected String mongoOperator() {
		return "$set";
	}

	/**
	 * @author Christoph Strobl
	 * @since 3.0
	 */
	public static class FieldAppender {

		private final Map<Object, Object> valueMap;

		private FieldAppender() {
			this.valueMap = new LinkedHashMap<>();
		}

		private FieldAppender(Map<Object, Object> source) {
			this.valueMap = new LinkedHashMap<>(source);
		}

		/**
		 * Define the field to set.
		 *
		 * @param field must not be {@literal null}.
		 * @return new instance of {@link ValueAppender}.
		 */
		public ValueAppender set(String field) {

			return new ValueAppender() {

				@Override
				public SetOperation toValue(Object value) {

					valueMap.put(field, value);
					return FieldAppender.this.build();
				}

				@Override
				public SetOperation toValueOf(Object value) {

					valueMap.put(field, value instanceof String ? Fields.fields((String) value) : value);
					return FieldAppender.this.build();
				}

				@Override
				public SetOperation withValueOfExpression(String operation, Object... values) {

					valueMap.put(field, new ExpressionProjection(operation, values));
					return FieldAppender.this.build();
				}
			};
		}

		private SetOperation build() {
			return new SetOperation(valueMap);
		}

		/**
		 * @author Christoph Strobl
		 * @author Mark Paluch
		 * @since 3.0
		 */
		public interface ValueAppender {

			/**
			 * Define the value to assign as is.
			 *
			 * @param value can be {@literal null}.
			 * @return new instance of {@link SetOperation}.
			 */
			SetOperation toValue(@Nullable Object value);

			/**
			 * Define the value to assign. Plain {@link String} values are treated as {@link Field field references}.
			 *
			 * @param value must not be {@literal null}.
			 * @return new instance of {@link SetOperation}.
			 */
			SetOperation toValueOf(Object value);

			/**
			 * Adds a generic projection for the current field.
			 *
			 * @param operation the operation key, e.g. {@code $add}.
			 * @param values the values to be set for the projection operation.
			 * @return new instance of {@link SetOperation}.
			 */
			SetOperation withValueOfExpression(String operation, Object... values);
		}

	}
}
