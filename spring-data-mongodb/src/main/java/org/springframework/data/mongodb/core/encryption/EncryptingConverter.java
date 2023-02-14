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

import org.springframework.data.mongodb.core.convert.MongoConversionContext;
import org.springframework.data.mongodb.core.convert.MongoValueConverter;

/**
 * A specialized {@link MongoValueConverter} for {@literal en-/decrypting} properties.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
public interface EncryptingConverter<S, T> extends MongoValueConverter<S, T> {

	@Override
	default S read(Object value, MongoConversionContext context) {
		return decrypt(value, buildEncryptionContext(context));
	}

	@Override
	default T write(Object value, MongoConversionContext context) {
		return encrypt(value, buildEncryptionContext(context));
	}

	S decrypt(Object encryptedValue, EncryptionContext context);

	T encrypt(Object value, EncryptionContext context);

	EncryptionContext buildEncryptionContext(MongoConversionContext context);
}
