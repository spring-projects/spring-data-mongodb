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

	default boolean isExplicitlyEncrypted() {
		return getProperty().isAnnotationPresent(ExplicitlyEncrypted.class);
	}

	@Nullable
	default Object lookupValue(String path) {
		return getSourceContext().getValue(path);
	}

	default Object convertToMongoType(Object value) {
		return getSourceContext().write(value);
	}

	default Encrypted lookupEncryptedAnnotation() {

		// TODO: having the path present here would really be helpful to inherit the algorithm
		Encrypted annotation = getProperty().findAnnotation(Encrypted.class);
		return annotation != null ? annotation : getProperty().getOwner().findAnnotation(Encrypted.class);
	}

	MongoConversionContext getSourceContext();
}
