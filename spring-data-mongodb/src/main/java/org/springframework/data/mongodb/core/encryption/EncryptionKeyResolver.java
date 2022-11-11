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
import org.bson.types.Binary;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.ExplicitlyEncrypted;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.mongodb.util.encryption.EncryptionUtils;
import org.springframework.util.StringUtils;

/**
 * Interface to obtain a {@link EncryptionKey Data Encryption Key} that is valid in a given {@link EncryptionContext
 * context}.
 * <p>
 * Use the {@link #annotationBased(EncryptionKeyResolver) based} variant which will first try to resolve a potential
 * {@link ExplicitlyEncrypted#altKeyName() Key Alternate Name} from annotations before calling the fallback resolver.
 * 
 * @author Christoph Strobl
 * @since 4.1
 * @see EncryptionKey
 */
@FunctionalInterface
public interface EncryptionKeyResolver {

	/**
	 * Get the {@link EncryptionKey Data Encryption Key}.
	 *
	 * @param encryptionContext the current {@link EncryptionContext context}.
	 * @return never {@literal null}.
	 */
	EncryptionKey getKey(EncryptionContext encryptionContext);

	/**
	 * Obtain an {@link EncryptionKeyResolver} that evaluates {@link ExplicitlyEncrypted#altKeyName()} and only calls the
	 * fallback {@link EncryptionKeyResolver resolver} if no {@literal Key Alternate Name} is present.
	 * 
	 * @param fallback must not be {@literal null}.
	 * @return new instance of {@link EncryptionKeyResolver}.
	 */
	static EncryptionKeyResolver annotationBased(EncryptionKeyResolver fallback) {

		return ((encryptionContext) -> {

			ExplicitlyEncrypted annotation = encryptionContext.getProperty().findAnnotation(ExplicitlyEncrypted.class);
			if (annotation == null || !StringUtils.hasText(annotation.altKeyName())) {

				Encrypted encrypted = encryptionContext.getProperty().getOwner().findAnnotation(Encrypted.class);
				if (encrypted == null) {
					return fallback.getKey(encryptionContext);
				}

				Object o = EncryptionUtils.resolveKeyId(encrypted.keyId()[0],
						() -> encryptionContext.getEvaluationContext(new Object()));
				if (o instanceof BsonBinary binary) {
					return EncryptionKey.keyId(binary);
				}
				if (o instanceof Binary binary) {
					return EncryptionKey.keyId((BsonBinary) BsonUtils.simpleToBsonValue(binary));
				}
				if (o instanceof String string) {
					return EncryptionKey.altKeyName(string);
				}
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
