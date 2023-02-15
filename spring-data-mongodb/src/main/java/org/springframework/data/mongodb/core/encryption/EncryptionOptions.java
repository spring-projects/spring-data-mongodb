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

import org.springframework.lang.Nullable;

/**
 * Options, like the {@link #algorithm()}, to apply when encrypting values.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
public class EncryptionOptions {

	private final String algorithm;
	private @Nullable EncryptionKey key;

	public EncryptionOptions(String algorithm) {
		this.algorithm = algorithm;
	}

	public void setKey(EncryptionKey key) {
		this.key = key;
	}

	public EncryptionKey key() {
		return key;
	}

	public String algorithm() {
		return algorithm;
	}
}
