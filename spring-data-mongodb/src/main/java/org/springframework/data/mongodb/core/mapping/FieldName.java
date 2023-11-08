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

import org.springframework.util.ObjectUtils;

/**
 * Value Object representing a field name that should be used to read/write fields within the MongoDB document.
 * {@link FieldName Field names} field names may contain special characters (such as dot ({@literal .})) but may be
 * treated differently depending on their {@link Type type}.
 *
 * @author Christoph Strobl
 * @since 4.2
 */
public record FieldName(String name, Type type) {

	private static final String ID_KEY = "_id";

	public static final FieldName ID = new FieldName(ID_KEY, Type.KEY);

	/**
	 * Create a new {@link FieldName} that treats the given {@literal value} as is.
	 *
	 * @param value must not be {@literal null}.
	 * @return new instance of {@link FieldName}.
	 */
	public static FieldName name(String value) {
		return new FieldName(value, Type.KEY);
	}

	/**
	 * Create a new {@link FieldName} that treats the given {@literal value} as a path. If the {@literal value} contains
	 * dot ({@literal .}) characters, they are considered deliminators in a path.
	 *
	 * @param value must not be {@literal null}.
	 * @return new instance of {@link FieldName}.
	 */
	public static FieldName path(String value) {
		return new FieldName(value, Type.PATH);
	}

	/**
	 * Get the parts the field name consists of. If the {@link FieldName} is a {@link Type#KEY} or a {@link Type#PATH}
	 * that does not contain dot ({@literal .}) characters an array containing a single element is returned. Otherwise the
	 * {@link #name()} is split into segments using dot ({@literal .}) as a separator.
	 *
	 * @return never {@literal null}.
	 */
	public String[] parts() {

		if (isKey()) {
			return new String[] { name };
		}

		return name.split("\\.");
	}

	/**
	 * @param type return true if the given {@link Type} is equal to {@link #type()}.
	 * @return {@literal true} if values are equal.
	 */
	public boolean isOfType(Type type) {
		return ObjectUtils.nullSafeEquals(type(), type);
	}

	/**
	 * @return whether the field name represents a key (i.e. as-is name).
	 */
	public boolean isKey() {
		return isOfType(Type.KEY);
	}

	/**
	 * @return whether the field name represents a path (i.e. dot-path).
	 */
	public boolean isPath() {
		return isOfType(Type.PATH);
	}

	@Override
	public String toString() {
		return "FieldName{%s=%s}".formatted(isKey() ? "key" : "path", name);
	}

	@Override
	public boolean equals(Object o) {

		if (o == this) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		FieldName fieldName = (FieldName) o;
		return ObjectUtils.nullSafeEquals(name, fieldName.name) && type == fieldName.type;
	}

	@Override
	public int hashCode() {

		int hashCode = ObjectUtils.nullSafeHashCode(name);
		return 31 * hashCode + ObjectUtils.nullSafeHashCode(type);
	}

	/**
	 * The {@link FieldName.Type type} defines how to treat a {@link FieldName} that contains special characters.
	 *
	 * @author Christoph Strobl
	 * @since 4.2
	 */
	public enum Type {

		/**
		 * Dot ({@literal .}) characters are treated as separators for segments in a path.
		 */
		PATH,

		/**
		 * Values are used as is.
		 */
		KEY
	}
}
