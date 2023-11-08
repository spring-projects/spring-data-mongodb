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

import java.util.UUID;

import org.bson.BsonBinary;
import org.bson.UuidRepresentation;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link EncryptionKey}.
 *
 * @author Christoph Strobl
 */
class EncryptionKeyUnitTests {

	@Test // GH-4284
	void keyIdToStringDoesNotRevealEntireKey() {

		UUID uuid = UUID.randomUUID();

		assertThat(EncryptionKey.keyId(new BsonBinary(uuid, UuidRepresentation.STANDARD)).toString())
				.contains(uuid.toString().substring(0, 6) + "***");
	}

	@Test // GH-4284
	void altKeyNameToStringDoesNotRevealEntireKey() {

		assertThat(EncryptionKey.keyAltName("s").toString()).contains("***");
		assertThat(EncryptionKey.keyAltName("su").toString()).contains("***");
		assertThat(EncryptionKey.keyAltName("sup").toString()).contains("***");
		assertThat(EncryptionKey.keyAltName("super-secret-key").toString()).contains("sup***");
	}
}
