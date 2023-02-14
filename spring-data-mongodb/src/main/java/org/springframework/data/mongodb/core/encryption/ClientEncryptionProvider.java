/*
 * Copyright 2023. the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.encryption;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.bson.BsonBinary;
import org.bson.BsonValue;

import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;

/**
 * @author Christoph Strobl
 * @since 4.1
 */
public interface ClientEncryptionProvider {

	ClientEncryption getClientEncryption();

	boolean refresh();

	void shutdown();

	default BsonValue decrypt(BsonBinary value) {
		return getClientEncryption().decrypt(value);
	}

	default BsonBinary encrypt(BsonValue value, EncryptOptions options) {
		return getClientEncryption().encrypt(value, options);
	}

	static ClientEncryptionProvider from(Supplier<ClientEncryption> clientEncryption) {
		return new ClientEncryptionProvider() {
			@Override
			public ClientEncryption getClientEncryption() {
				return clientEncryption.get();
			}

			@Override
			public boolean refresh() {
				return true;
			}

			@Override
			public void shutdown() {
				clientEncryption.get().close();
			}
		};
	}

	static ClientEncryptionProvider caching(Supplier<ClientEncryption> source) {

		return new ClientEncryptionProvider() {

			final AtomicReference<ClientEncryption> enc = new AtomicReference<>(source.get());

			@Override
			public ClientEncryption getClientEncryption() {
				return enc.get();
			}

			@Override
			public boolean refresh() {
				enc.set(source.get());
				return true;
			}

			@Override
			public void shutdown() {
				source.get().close();
			}
		};
	}

	static ClientEncryptionProvider just(ClientEncryption clientEncryption) {

		return new ClientEncryptionProvider() {

			@Override
			public ClientEncryption getClientEncryption() {
				return clientEncryption;
			}

			@Override
			public boolean refresh() {
				return false;
			}

			@Override
			public void shutdown() {
				clientEncryption.close();
			}
		};
	}

}
