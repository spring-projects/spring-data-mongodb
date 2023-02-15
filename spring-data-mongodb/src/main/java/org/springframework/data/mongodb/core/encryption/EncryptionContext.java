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

import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.mongodb.core.convert.MongoConversionContext;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 */
public interface EncryptionContext extends ValueConversionContext<MongoPersistentProperty> {

	/**
	 * @return {@literal true} if the {@link ExplicitlyEncrypted} annotation is present.
	 */
	default boolean isExplicitlyEncrypted() {
		return getProperty().isAnnotationPresent(ExplicitlyEncrypted.class);
	}

	/**
	 * Lookup the value for a given path within the current context.
	 *
	 * @param path the path/property name to resolve the current value for.
	 * @return can be {@literal null}.
	 */
	@Nullable
	default Object lookupValue(String path) {
		return getValueConversionContext().getValue(path);
	}

	/**
	 * Shortcut for converting a given {@literal value} into its store representation using the root
	 * {@link ValueConversionContext}.
	 * 
	 * @param value
	 * @return
	 */
	default Object convertToMongoType(Object value) {
		return getValueConversionContext().write(value);
	}

	/**
	 * Search for the {@link Encrypted} annotation on both the {@link org.springframework.data.mapping.PersistentProperty
	 * property} as well as the {@link org.springframework.data.mapping.PersistentEntity entity} and return the first
	 * found
	 * 
	 * @return can be {@literal null}.
	 */
	@Nullable
	default Encrypted lookupEncryptedAnnotation() {

		// TODO: having the path present here would really be helpful to inherit the algorithm
		Encrypted annotation = getProperty().findAnnotation(Encrypted.class);
		return annotation != null ? annotation : getProperty().getOwner().findAnnotation(Encrypted.class);
	}

	/**
	 * @return the {@link ValueConversionContext}.
	 */
	MongoConversionContext getValueConversionContext();
}
