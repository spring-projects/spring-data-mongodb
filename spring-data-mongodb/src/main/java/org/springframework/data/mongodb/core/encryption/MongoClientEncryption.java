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

import java.util.function.Supplier;

import org.bson.BsonBinary;
import org.bson.BsonValue;
import org.springframework.data.mongodb.core.encryption.EncryptionKey.Type;
import org.springframework.util.Assert;

import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;

/**
 * {@link ClientEncryption} based {@link Encryption} implementation.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
public class MongoClientEncryption implements Encryption<BsonValue, BsonBinary> {

	private final Supplier<ClientEncryption> source;

	MongoClientEncryption(Supplier<ClientEncryption> source) {
		this.source = source;
	}

	/**
	 * Create a new {@link MongoClientEncryption} instance for the given {@link ClientEncryption}.
	 *
	 * @param clientEncryption must not be {@literal null}.
	 * @return new instance of {@link MongoClientEncryption}.
	 */
	public static MongoClientEncryption just(ClientEncryption clientEncryption) {

		Assert.notNull(clientEncryption, "ClientEncryption must not be null");

		return new MongoClientEncryption(() -> clientEncryption);
	}

	@Override
	public BsonValue decrypt(BsonBinary value) {
		return getClientEncryption().decrypt(value);
	}

	@Override
	public BsonBinary encrypt(BsonValue value, EncryptionOptions options) {

		EncryptOptions encryptOptions = new EncryptOptions(options.algorithm());

		if (Type.ALT.equals(options.key().type())) {
			encryptOptions = encryptOptions.keyAltName(options.key().value().toString());
		} else {
			encryptOptions = encryptOptions.keyId((BsonBinary) options.key().value());
		}

		return getClientEncryption().encrypt(value, encryptOptions);
	}

	public ClientEncryption getClientEncryption() {
		return source.get();
	}

}
