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
package org.springframework.data.mongodb.core.encryption;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.EvaluationContext;
import org.springframework.lang.Nullable;

/**
 * Context to encapsulate encryption for a specific {@link MongoPersistentProperty}.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
public interface EncryptionContext {

	/**
	 * Returns the {@link MongoPersistentProperty} to be handled.
	 *
	 * @return will never be {@literal null}.
	 */
	MongoPersistentProperty getProperty();

	/**
	 * Shortcut for converting a given {@literal value} into its store representation using the root
	 * {@code ValueConversionContext}.
	 *
	 * @param value
	 * @return
	 */
	Object convertToMongoType(Object value);

	/**
	 * Reads the value as an instance of the {@link PersistentProperty#getTypeInformation() property type}.
	 *
	 * @param value {@link Object value} to be read; can be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalStateException if value cannot be read as an instance of {@link Class type}.
	 */
	default <T> T read(@Nullable Object value) {
		return (T) read(value, getProperty().getTypeInformation());
	}

	/**
	 * Reads the value as an instance of {@link Class type}.
	 *
	 * @param value {@link Object value} to be read; can be {@literal null}.
	 * @param target {@link Class type} of value to be read; must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalStateException if value cannot be read as an instance of {@link Class type}.
	 */
	default <T> T read(@Nullable Object value, Class<T> target) {
		return read(value, TypeInformation.of(target));
	}

	/**
	 * Reads the value as an instance of {@link TypeInformation type}.
	 *
	 * @param value {@link Object value} to be read; can be {@literal null}.
	 * @param target {@link TypeInformation type} of value to be read; must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalStateException if value cannot be read as an instance of {@link Class type}.
	 */
	<T> T read(@Nullable Object value, TypeInformation<T> target);

	/**
	 * Write the value as an instance of the {@link PersistentProperty#getTypeInformation() property type}.
	 *
	 * @param value {@link Object value} to write; can be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalStateException if value cannot be written as an instance of the
	 *           {@link PersistentProperty#getTypeInformation() property type}.
	 * @see PersistentProperty#getTypeInformation()
	 * @see #write(Object, TypeInformation)
	 */
	@Nullable
	default <T> T write(@Nullable Object value) {
		return (T) write(value, getProperty().getTypeInformation());
	}

	/**
	 * Write the value as an instance of {@link Class type}.
	 *
	 * @param value {@link Object value} to write; can be {@literal null}.
	 * @param target {@link Class type} of value to be written; must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalStateException if value cannot be written as an instance of {@link Class type}.
	 */
	@Nullable
	default <T> T write(@Nullable Object value, Class<T> target) {
		return write(value, TypeInformation.of(target));
	}

	/**
	 * Write the value as an instance of given {@link TypeInformation type}.
	 *
	 * @param value {@link Object value} to write; can be {@literal null}.
	 * @param target {@link TypeInformation type} of value to be written; must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalStateException if value cannot be written as an instance of {@link Class type}.
	 */
	@Nullable
	<T> T write(@Nullable Object value, TypeInformation<T> target);

	/**
	 * Lookup the value for a given path within the current context.
	 *
	 * @param path the path/property name to resolve the current value for.
	 * @return can be {@literal null}.
	 */
	@Nullable
	Object lookupValue(String path);

	EvaluationContext getEvaluationContext(Object source);

}
