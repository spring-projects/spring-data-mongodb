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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.EncryptionAlgorithms.*;

import java.util.function.Supplier;

import org.bson.BsonBinary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;

/**
 * Unit tests for {@link MongoClientEncryption}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class MongoClientEncryptionUnitTests {

	@Mock //
	ClientEncryption clientEncryption;

	@Test // GH-4284
	void delegatesDecrypt() {

		MongoClientEncryption mce = MongoClientEncryption.just(clientEncryption);
		mce.decrypt(new BsonBinary(new byte[0]));

		verify(clientEncryption).decrypt(Mockito.any());
	}

	@Test // GH-4284
	void delegatesEncrypt() {

		MongoClientEncryption mce = MongoClientEncryption.just(clientEncryption);
		mce.encrypt(new BsonBinary(new byte[0]),
				new EncryptionOptions(AEAD_AES_256_CBC_HMAC_SHA_512_Random, EncryptionKey.keyAltName("sec-key-name")));

		ArgumentCaptor<EncryptOptions> options = ArgumentCaptor.forClass(EncryptOptions.class);
		verify(clientEncryption).encrypt(any(), options.capture());
		assertThat(options.getValue().getAlgorithm()).isEqualTo(AEAD_AES_256_CBC_HMAC_SHA_512_Random);
		assertThat(options.getValue().getKeyAltName()).isEqualTo("sec-key-name");
	}

	@Test // GH-4284
	void refreshObtainsNextInstanceFromSupplier() {

		ClientEncryption next = mock(ClientEncryption.class);

		MongoClientEncryption mce = new MongoClientEncryption(new Supplier<>() {

			int counter = 0;

			@Override
			public ClientEncryption get() {
				return counter++ % 2 == 0 ? clientEncryption : next;
			}
		});

		assertThat(mce.getClientEncryption()).isSameAs(clientEncryption);
		assertThat(mce.getClientEncryption()).isSameAs(next);
	}
}
