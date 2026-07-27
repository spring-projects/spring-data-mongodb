/*
 * Copyright 2026 the original author or authors.
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
package org.springframework.data.mongodb.cache;

import org.springframework.util.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * @author Anıl Şenocak
 */
final class MongoCacheKey {

	private static final byte SEPARATOR = 0;

	private MongoCacheKey() {}

	static String id(String cacheName, Object key) {

		Assert.hasText(cacheName, "Cache name must not be empty");
		Assert.notNull(key, "Cache key must not be null");

		MessageDigest digest = sha256();
		update(digest, cacheName);
		digest.update(SEPARATOR);
		update(digest, key.getClass().getName());
		digest.update(SEPARATOR);
		digest.update(keyBytes(key));

		return cacheName + "::" + HexFormat.of().formatHex(digest.digest());
	}

	static String description(Object key) {
		Assert.notNull(key, "Cache key must not be null");
		return key.getClass().getName() + ":" + key;
	}

	private static MessageDigest sha256() {

		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException ex) {
			throw new IllegalStateException("SHA-256 digest is not available", ex);
		}
	}

	private static void update(MessageDigest digest, String value) {
		digest.update(value.getBytes(StandardCharsets.UTF_8));
	}

	private static byte[] keyBytes(Object key) {

		if (key instanceof Serializable serializable) {
			try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
					ObjectOutputStream output = new ObjectOutputStream(bytes)) {

				output.writeObject(serializable);
				output.flush();
				return bytes.toByteArray();
			} catch (IOException ex) {
				// Fall back to a stable textual representation for non-serializable object graphs.
			}
		}

		return Objects.toString(key).getBytes(StandardCharsets.UTF_8);
	}
}
