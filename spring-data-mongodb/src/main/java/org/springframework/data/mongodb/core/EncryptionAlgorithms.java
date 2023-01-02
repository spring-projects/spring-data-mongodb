/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

/**
 * Encryption algorithms supported by MongoDB Client Side Field Level Encryption.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
public final class EncryptionAlgorithms {

	public static final String AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic";
	public static final String AEAD_AES_256_CBC_HMAC_SHA_512_Random = "AEAD_AES_256_CBC_HMAC_SHA_512-Random";

}
