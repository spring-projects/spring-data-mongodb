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
import org.bson.BsonValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class MongoEncryptionConverterUnitTests {

	@Mock //
	Encryption<BsonValue, BsonBinary> encryption;

	@Mock //
	EncryptionKeyResolver keyResolver;

	MongoEncryptionConverter converter;

	@BeforeEach
	void beforeEach() {
		converter = new MongoEncryptionConverter(encryption, keyResolver);
	}



	/*
	static class Person {

		String id;
		String name;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic) //
		String ssn;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, altKeyName = "mySuperSecretKey") //
		String wallet;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) // full document must be random
		Address address;

		AddressWithEncryptedZip encryptedZip;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) // lists must be random
		List<String> listOfString;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) // lists must be random
		List<Address> listOfComplex;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, altKeyName = "/name") //
		String viaAltKeyNameField;
	}
	 */

}
