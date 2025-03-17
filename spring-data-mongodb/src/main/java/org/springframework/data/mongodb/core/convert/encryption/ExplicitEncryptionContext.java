/*
 * Copyright 2023-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.convert.encryption;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.convert.MongoConversionContext;
import org.springframework.data.mongodb.core.encryption.EncryptionContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.EvaluationContext;

/**
 * Default {@link EncryptionContext} implementation.
 * 
 * @author Christoph Strobl
 * @since 4.1
 */
class ExplicitEncryptionContext implements EncryptionContext {

	private final MongoConversionContext conversionContext;

	public ExplicitEncryptionContext(MongoConversionContext conversionContext) {
		this.conversionContext = conversionContext;
	}

	@Override
	public MongoPersistentProperty getProperty() {
		return conversionContext.getProperty();
	}

	@Override
	public @Nullable Object lookupValue(String path) {
		return conversionContext.getValue(path);
	}

	@Override
	public @Nullable Object convertToMongoType(Object value) {
		return conversionContext.write(value);
	}

	@Override
	public EvaluationContext getEvaluationContext(@Nullable Object source) {

		if(conversionContext.getSpELContext() != null) {
			return conversionContext.getSpELContext().getEvaluationContext(source);
		}

		throw new IllegalStateException("SpEL context not present");
	}

	@Override
	public <T> @Nullable T read(@Nullable Object value, TypeInformation<T> target) {
		return conversionContext.read(value, target);
	}

	@Override
	public <T> @Nullable T write(@Nullable Object value, TypeInformation<T> target) {
		return conversionContext.write(value, target);
	}
}
