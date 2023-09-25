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
package org.springframework.data.mongodb.core.mapping;

import org.springframework.data.mongodb.core.mapping.FieldName.Type;

/**
 * Value Object for representing a field to read/write within a MongoDB {@link org.bson.Document}.
 * 
 * @author Christoph Strobl
 * @since 4.2
 */
public class MongoField {

	private final FieldName fieldName;
	private final FieldType fieldType;
	private final int fieldOrder;

	/**
	 * Create a new {@link MongoField} with given {@literal name}.
	 *
	 * @param name the name to be used as is (with all its potentially special characters).
	 * @return new instance of {@link MongoField}.
	 */
	public static MongoField just(String name) {
		return builder().fieldName(name).build();
	}

	/**
	 * @return new instance of {@link MongoFieldBuilder}.
	 */
	public static MongoFieldBuilder builder() {
		return new MongoFieldBuilder();
	}

	protected MongoField(FieldName fieldName, Class<?> targetFieldType, int fieldOrder) {
		this(fieldName, FieldType.valueOf(targetFieldType.getSimpleName()), fieldOrder);
	}

	protected MongoField(FieldName fieldName, FieldType fieldType, int fieldOrder) {

		this.fieldName = fieldName;
		this.fieldType = fieldType;
		this.fieldOrder = fieldOrder;
	}

	/**
	 * @return never {@literal null}.
	 */
	public FieldName getFieldName() {
		return fieldName;
	}

	/**
	 * Get the position of the field within the target document.
	 *
	 * @return {@link Integer#MAX_VALUE} if undefined.
	 */
	public int getFieldOrder() {
		return fieldOrder;
	}

	/**
	 * @param prefix a prefix to the current name.
	 * @return new instance of {@link MongoField} with prefix appended to current field name.
	 */
	MongoField withPrefix(String prefix) {
		return new MongoField(new FieldName(prefix + fieldName.name(), fieldName.type()), fieldType, fieldOrder);
	}

	/**
	 * Get the fields target type if defined.
	 * 
	 * @return never {@literal null}.
	 */
	public FieldType getFieldType() {
		return fieldType;
	}

	public static class MongoFieldBuilder {

		private String fieldName;
		private FieldType fieldType = FieldType.IMPLICIT;
		private int orderNumber = Integer.MAX_VALUE;
		private Type type = Type.PATH;

		public MongoFieldBuilder fieldType(FieldType fieldType) {

			this.fieldType = fieldType;
			return this;
		}

		public MongoFieldBuilder fieldName(String fieldName) {

			this.fieldName = fieldName;
			this.type = Type.KEY;
			return this;
		}

		public MongoFieldBuilder fieldOrderNumber(int orderNumber) {

			this.orderNumber = orderNumber;
			return this;
		}

		public MongoFieldBuilder fieldPath(String path) {

			this.fieldName = path;
			this.type = Type.PATH;
			return this;
		}

		public MongoField build() {
			return new MongoField(new FieldName(fieldName, type), fieldType, orderNumber);
		}
	}
}
