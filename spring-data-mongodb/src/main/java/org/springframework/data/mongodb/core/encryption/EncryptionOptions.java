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

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Options, like the {@link #algorithm()}, to apply when encrypting values.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
public class EncryptionOptions {

	private final String algorithm;
	private final EncryptionKey key;

	public EncryptionOptions(String algorithm, EncryptionKey key) {

		Assert.hasText(algorithm, "Algorithm must not be empty");
		Assert.notNull(key, "EncryptionKey must not be empty");

		this.key = key;
		this.algorithm = algorithm;
	}

	public EncryptionKey key() {
		return key;
	}

	public String algorithm() {
		return algorithm;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		EncryptionOptions that = (EncryptionOptions) o;

		if (!ObjectUtils.nullSafeEquals(algorithm, that.algorithm)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(key, that.key);
	}

	@Override
	public int hashCode() {

		int result = ObjectUtils.nullSafeHashCode(algorithm);
		result = 31 * result + ObjectUtils.nullSafeHashCode(key);
		return result;
	}

	@Override
	public String toString() {
		return "EncryptionOptions{" + "algorithm='" + algorithm + '\'' + ", key=" + key + '}';
	}
}
