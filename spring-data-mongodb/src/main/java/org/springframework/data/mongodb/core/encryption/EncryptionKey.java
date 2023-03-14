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

import org.bson.BsonBinary;
import org.springframework.util.Assert;

/**
 * The {@link EncryptionKey} represents a {@literal Data Encryption Key} reference that can be either direct via the
 * {@link KeyId key id} or its {@link KeyAltName Key Alternative Name}.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
public interface EncryptionKey {

	/**
	 * Create a new {@link EncryptionKey} that uses the keys id for reference.
	 *
	 * @param key must not be {@literal null}.
	 * @return new instance of {@link EncryptionKey KeyId}.
	 */
	static EncryptionKey keyId(BsonBinary key) {

		Assert.notNull(key, "KeyId must not be null");

		return new KeyId(key);
	}

	/**
	 * Create a new {@link EncryptionKey} that uses an {@literal Key Alternative Name} for reference.
	 *
	 * @param keyAltName must not be {@literal null} or empty.
	 * @return new instance of {@link EncryptionKey KeyAltName}.
	 */
	static EncryptionKey keyAltName(String keyAltName) {

		Assert.hasText(keyAltName, "Key Alternative Name must not be empty");

		return new KeyAltName(keyAltName);
	}

	/**
	 * @return the value that allows to reference a specific key.
	 */
	Object value();

	/**
	 * @return the {@link Type} of reference.
	 */
	Type type();

	/**
	 * The key reference type.
	 */
	enum Type {

		/**
		 * Key referenced via its {@literal id}.
		 */
		ID,

		/**
		 * Key referenced via an {@literal Key Alternative Name}.
		 */
		ALT
	}
}
