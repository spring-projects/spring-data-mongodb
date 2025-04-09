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
package org.springframework.data.mongodb.core.encryption;

import org.bson.BsonDocument;

/**
 * Component responsible for encrypting and decrypting values.
 *
 * @param <P> plaintext type.
 * @param <C> ciphertext type.
 * @author Christoph Strobl
 * @author Ross Lawley
 * @since 4.1
 */
public interface Encryption<P, C> {

	/**
	 * Encrypt the given value.
	 *
	 * @param value must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the encrypted value.
	 */
	C encrypt(P value, EncryptionOptions options);

	/**
	 * Decrypt the given value.
	 *
	 * @param value must not be {@literal null}.
	 * @return the decrypted value.
	 */
	P decrypt(C value);

	/**
	 * Encrypt the given expression.
	 *
	 * @param value must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the encrypted expression.
	 * @since 4.5.0
	 */
	default BsonDocument encryptExpression(BsonDocument value, EncryptionOptions options) {
		throw new UnsupportedOperationException("Unsupported encryption method");
	}

}
