/*
 * Copyright 2020-2021 the original author or authors.
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

import org.springframework.data.mongodb.core.aggregation.AddFieldsOperation.AddFieldsOperationBuilder.ValueAppender;
import org.springframework.lang.Nullable;

/**
 * Adds new fields to documents. {@code $addFields} outputs documents that contain all existing fields from the input
 * documents and newly added fields.
 *
 * <pre class="code">
 * AddFieldsOperation.addField("totalHomework").withValue("A+").and().addField("totalQuiz").withValue("B-")
 * </pre>
 *
 * @author Christoph Strobl
 * @since 3.0
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/">MongoDB Aggregation
 *      Framework: $addFields</a>
 */
public class AddFieldsOperation extends DocumentEnhancingOperation {

	/**
	 * Create new instance of {@link AddFieldsOperation} adding map keys as exposed fields.
	 *
	 * @param source must not be {@literal null}.
	 */
	private AddFieldsOperation(Map<Object, Object> source) {
		super(source);
	}

	/**
	 * Create new instance of {@link AddFieldsOperation}
	 *
	 * @param field must not be {@literal null}.
	 * @param value can be {@literal null}.
	 */
	public AddFieldsOperation(Object field, @Nullable Object value) {
		this(Collections.singletonMap(field, value));
	}

	/**
	 * Define the {@link AddFieldsOperation} via {@link AddFieldsOperationBuilder}.
	 *
	 * @return new instance of {@link AddFieldsOperationBuilder}.
	 */
	public static AddFieldsOperationBuilder builder() {
		return new AddFieldsOperationBuilder();
	}

	/**
	 * Concatenate another field to add.
	 *
	 * @param field must not be {@literal null}.
	 * @return new instance of {@link AddFieldsOperationBuilder}.
	 */
	public static ValueAppender addField(String field) {
		return new AddFieldsOperationBuilder().addField(field);
	}

	/**
	 * Append the value for a specific field to the operation.
	 *
	 * @param field the target field to add.
	 * @param value the value to assign.
	 * @return new instance of {@link AddFieldsOperation}.
	 */
	public AddFieldsOperation addField(Object field, Object value) {

		LinkedHashMap<Object, Object> target = new LinkedHashMap<>(getValueMap());
		target.put(field, value);

		return new AddFieldsOperation(target);
	}

	/**
	 * Concatenate additional fields to add.
	 *
	 * @return new instance of {@link AddFieldsOperationBuilder}.
	 */
	public AddFieldsOperationBuilder and() {
		return new AddFieldsOperationBuilder(getValueMap());
	}

	@Override
	protected String mongoOperator() {
		return "$addFields";
	}

	/**
	 * @author Christoph Strobl
	 * @since 3.0
	 */
	public static class AddFieldsOperationBuilder {

		private final Map<Object, Object> valueMap;

		private AddFieldsOperationBuilder() {
			this.valueMap = new LinkedHashMap<>();
		}

		private AddFieldsOperationBuilder(Map<Object, Object> source) {
			this.valueMap = new LinkedHashMap<>(source);
		}

		public AddFieldsOperationBuilder addFieldWithValue(String field, @Nullable Object value) {
			return addField(field).withValue(value);
		}

		public AddFieldsOperationBuilder addFieldWithValueOf(String field, Object value) {
			return addField(field).withValueOf(value);
		}

		/**
		 * Define the field to add.
		 *
		 * @param field must not be {@literal null}.
		 * @return new instance of {@link ValueAppender}.
		 */
		public ValueAppender addField(String field) {

			return new ValueAppender() {

				@Override
				public AddFieldsOperationBuilder withValue(Object value) {

					valueMap.put(field, value);
					return AddFieldsOperationBuilder.this;
				}

				@Override
				public AddFieldsOperationBuilder withValueOf(Object value) {

					valueMap.put(field, value instanceof String ? Fields.fields((String) value) : value);
					return AddFieldsOperationBuilder.this;
				}

				@Override
				public AddFieldsOperationBuilder withValueOfExpression(String operation, Object... values) {

					valueMap.put(field, new ExpressionProjection(operation, values));
					return AddFieldsOperationBuilder.this;
				}
			};
		}

		public AddFieldsOperation build() {
			return new AddFieldsOperation(valueMap);
		}

		/**
		 * @author Christoph Strobl
		 * @since 3.0
		 */
		public interface ValueAppender {

			/**
			 * Define the value to assign as is.
			 *
			 * @param value can be {@literal null}.
			 * @return new instance of {@link AddFieldsOperation}.
			 */
			AddFieldsOperationBuilder withValue(@Nullable Object value);

			/**
			 * Define the value to assign. Plain {@link String} values are treated as {@link Field field references}.
			 *
			 * @param value must not be {@literal null}.
			 * @return new instance of {@link AddFieldsOperation}.
			 */
			AddFieldsOperationBuilder withValueOf(Object value);

			/**
			 * Adds a generic projection for the current field.
			 *
			 * @param operation the operation key, e.g. {@code $add}.
			 * @param values the values to be set for the projection operation.
			 * @return new instance of {@link AddFieldsOperation}.
			 */
			AddFieldsOperationBuilder withValueOfExpression(String operation, Object... values);
		}
	}

}
