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

import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public interface EncryptionKeyProvider {

	EncryptionKey getKey(EncryptionContext encryptionContext);

	static EncryptionKeyProvider annotationBasedKeyProvider() {
		return annotationBasedKeyProvider(() -> {
			throw new IllegalStateException("No Encryption key found");
		});
	}

	static EncryptionKeyProvider annotationBasedKeyProvider(Supplier<EncryptionKey> fallback) {

		return ((encryptionContext) -> {

			if (encryptionContext.isExplicitlyEncrypted()) {

				ExplicitlyEncrypted annotation = encryptionContext.getProperty().findAnnotation(ExplicitlyEncrypted.class);
				if (StringUtils.hasText(annotation.altKeyName())) {
					String altKeyName = annotation.altKeyName();
					if (altKeyName.startsWith("/")) {
						String fieldName = altKeyName.replace("/", "");
						return new EncryptionKey.AltKeyName(encryptionContext.lookupValue(fieldName).toString());
					} else {
						return new EncryptionKey.AltKeyName(altKeyName);
					}
				}
			}
			// if (encryptionContext.getKeyId() != null && !ObjectUtils.isEmpty(encryptionContext.getKeyId())) {
			// // TODO: resolve the hash
			// // return EncryptionKey.keyId()
			// throw new IllegalStateException();
			// }
			return fallback.get();
		});
	}
}
