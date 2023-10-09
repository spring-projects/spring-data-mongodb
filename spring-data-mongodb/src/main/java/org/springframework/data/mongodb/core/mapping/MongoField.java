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
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Value Object for representing a field to read/write within a MongoDB {@link org.bson.Document}.
 *
 * @author Christoph Strobl
 * @since 4.2
 */
public class MongoField {

	private final FieldName name;
	private final FieldType fieldType;
	private final int order;

	protected MongoField(FieldName name, Class<?> targetFieldType, int fieldOrder) {
		this(name, FieldType.valueOf(targetFieldType.getSimpleName()), fieldOrder);
	}

	protected MongoField(FieldName name, FieldType fieldType, int fieldOrder) {

		this.name = name;
		this.fieldType = fieldType;
		this.order = fieldOrder;
	}

	/**
	 * Create a new {@link MongoField} with given {@literal name}.
	 *
	 * @param name the name to be used as is (with all its potentially special characters).
	 * @return new instance of {@link MongoField}.
	 */
	public static MongoField fromKey(String name) {
		return builder().name(name).build();
	}

	/**
	 * Create a new {@link MongoField} with given {@literal name}.
	 *
	 * @param name the name to be used path expression.
	 * @return new instance of {@link MongoField}.
	 */
	public static MongoField fromPath(String name) {
		return builder().path(name).build();
	}

	/**
	 * @return new instance of {@link MongoFieldBuilder}.
	 */
	public static MongoFieldBuilder builder() {
		return new MongoFieldBuilder();
	}

	/**
	 * @return never {@literal null}.
	 */
	public FieldName getName() {
		return name;
	}

	/**
	 * Get the position of the field within the target document.
	 *
	 * @return {@link Integer#MAX_VALUE} if undefined.
	 */
	public int getOrder() {
		return order;
	}

	/**
	 * @param prefix a prefix to the current name.
	 * @return new instance of {@link MongoField} with prefix appended to current field name.
	 */
	MongoField withPrefix(String prefix) {
		return new MongoField(new FieldName(prefix + name.name(), name.type()), fieldType, order);
	}

	/**
	 * Get the fields target type if defined.
	 *
	 * @return never {@literal null}.
	 */
	public FieldType getFieldType() {
		return fieldType;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		MongoField that = (MongoField) o;

		if (order != that.order)
			return false;
		if (!ObjectUtils.nullSafeEquals(name, that.name)) {
			return false;
		}
		return fieldType == that.fieldType;
	}

	@Override
	public int hashCode() {

		int result = ObjectUtils.nullSafeHashCode(name);
		result = 31 * result + ObjectUtils.nullSafeHashCode(fieldType);
		result = 31 * result + order;
		return result;
	}

	@Override
	public String toString() {
		return name.toString();
	}

	/**
	 * Builder for {@link MongoField}.
	 */
	public static class MongoFieldBuilder {

		private String name;
		private Type nameType = Type.PATH;
		private FieldType type = FieldType.IMPLICIT;
		private int order = Integer.MAX_VALUE;

		/**
		 * Configure the field type.
		 *
		 * @param fieldType
		 * @return
		 */
		public MongoFieldBuilder fieldType(FieldType fieldType) {

			this.type = fieldType;
			return this;
		}

		/**
		 * Configure the field name as key. Key field names are used as-is without applying path segmentation splitting
		 * rules.
		 *
		 * @param fieldName
		 * @return
		 */
		public MongoFieldBuilder name(String fieldName) {

			Assert.hasText(fieldName, "Field name must not be empty");

			this.name = fieldName;
			this.nameType = Type.KEY;
			return this;
		}

		/**
		 * Configure the field name as path. Path field names are applied as paths potentially pointing into subdocuments.
		 *
		 * @param path
		 * @return
		 */
		public MongoFieldBuilder path(String path) {

			Assert.hasText(path, "Field path (name) must not be empty");

			this.name = path;
			this.nameType = Type.PATH;
			return this;
		}

		/**
		 * Configure the field order, defaulting to {@link Integer#MAX_VALUE} (undefined).
		 *
		 * @param order
		 * @return
		 */
		public MongoFieldBuilder order(int order) {

			this.order = order;
			return this;
		}

		/**
		 * Build a new {@link MongoField}.
		 *
		 * @return a new {@link MongoField}.
		 */
		public MongoField build() {
			return new MongoField(new FieldName(name, nameType), type, order);
		}
	}
}
