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

import java.util.function.Function;

import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public interface EncryptionKeyResolver {

	EncryptionKey getKey(EncryptionContext encryptionContext);

	static EncryptionKeyResolver annotationBased() {
		return annotationBased((ctx) -> {
			throw new IllegalStateException("No Encryption key found");
		});
	}

	static EncryptionKeyResolver annotationBased(Function<EncryptionContext, EncryptionKey> fallback) {

		return ((encryptionContext) -> {

			ExplicitlyEncrypted annotation = encryptionContext.getProperty().findAnnotation(ExplicitlyEncrypted.class);
			if (annotation == null || !StringUtils.hasText(annotation.altKeyName())) {
				return fallback.apply(encryptionContext);
			}

			String altKeyName = annotation.altKeyName();
			if (altKeyName.startsWith("/")) {
				Object fieldValue = encryptionContext.lookupValue(altKeyName.replace("/", ""));
				if (fieldValue == null) {
					throw new IllegalStateException(String.format("Key Alternative Name for %s was null", altKeyName));
				}
				return new EncryptionKey.AltKeyName(fieldValue.toString());
			} else {
				return new EncryptionKey.AltKeyName(altKeyName);
			}
		});
	}
}
