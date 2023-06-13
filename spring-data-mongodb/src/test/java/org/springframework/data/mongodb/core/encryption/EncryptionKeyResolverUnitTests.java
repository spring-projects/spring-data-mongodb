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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Function;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.ExplicitEncrypted;
import org.springframework.data.mongodb.test.util.MongoTestMappingContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * Unit tests for {@link EncryptionKeyResolver}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EncryptionKeyResolverUnitTests {

	@Mock //
	EncryptionKeyResolver fallbackKeyResolver;

	MongoTestMappingContext mappingContext = MongoTestMappingContext.newTestContext().init();

	EncryptionKey defaultEncryptionKey = EncryptionKey
			.keyId(new BsonBinary("super-secret".getBytes(StandardCharsets.UTF_8)));

	@BeforeEach
	void beforeEach() {
		when(fallbackKeyResolver.getKey(any())).thenReturn(defaultEncryptionKey);
	}

	@Test // GH-4284
	void usesDefaultKeyIfNoAnnotationPresent() {

		EncryptionContext ctx = prepareEncryptionContext(AnnotatedWithExplicitlyEncrypted.class,
				AnnotatedWithExplicitlyEncrypted::getNotAnnotated);

		EncryptionKey key = EncryptionKeyResolver.annotated(fallbackKeyResolver).getKey(ctx);

		assertThat(key).isSameAs(defaultEncryptionKey);
	}

	@Test // GH-4284
	void usesDefaultKeyIfAnnotatedValueIsEmpty() {

		EncryptionContext ctx = prepareEncryptionContext(AnnotatedWithExplicitlyEncrypted.class,
				AnnotatedWithExplicitlyEncrypted::getAlgorithm);

		EncryptionKey key = EncryptionKeyResolver.annotated(fallbackKeyResolver).getKey(ctx);

		assertThat(key).isSameAs(defaultEncryptionKey);
	}

	@Test // GH-4284
	void usesDefaultAltKeyNameIfPresent() {

		EncryptionContext ctx = prepareEncryptionContext(AnnotatedWithExplicitlyEncrypted.class,
				AnnotatedWithExplicitlyEncrypted::getAlgorithmAndAltKeyName);

		EncryptionKey key = EncryptionKeyResolver.annotated(fallbackKeyResolver).getKey(ctx);

		assertThat(key).isEqualTo(EncryptionKey.keyAltName("sec-key-name"));
	}

	@Test // GH-4284
	void readsAltKeyNameFromContextIfReferencingPropertyValue() {

		EncryptionContext ctx = prepareEncryptionContext(AnnotatedWithExplicitlyEncrypted.class,
				AnnotatedWithExplicitlyEncrypted::getAlgorithmAndAltKeyNameFromPropertyValue);
		when(ctx.lookupValue(eq("notAnnotated"))).thenReturn("born-to-be-wild");

		EncryptionKey key = EncryptionKeyResolver.annotated(fallbackKeyResolver).getKey(ctx);

		assertThat(key).isEqualTo(EncryptionKey.keyAltName("born-to-be-wild"));
	}

	@Test // GH-4284
	void readsKeyIdFromEncryptedAnnotationIfNoBetterCandidateAvailable() {

		EncryptionContext ctx = prepareEncryptionContext(
				AnnotatedWithExplicitlyEncryptedHavingDefaultAlgorithmServedViaAnnotationOnType.class,
				AnnotatedWithExplicitlyEncryptedHavingDefaultAlgorithmServedViaAnnotationOnType::getKeyIdFromDomainType);

		EncryptionKey key = EncryptionKeyResolver.annotated(fallbackKeyResolver).getKey(ctx);

		assertThat(key).isEqualTo(EncryptionKey.keyId(
				new BsonBinary(BsonBinarySubType.UUID_STANDARD, Base64.getDecoder().decode("xKVup8B1Q+CkHaVRx+qa+g=="))));
	}

	@Test // GH-4284
	void ignoresKeyIdFromEncryptedAnnotationWhenBetterCandidateAvailable() {

		EncryptionContext ctx = prepareEncryptionContext(KeyIdFromSpel.class, KeyIdFromSpel::getKeyIdFromDomainType);

		StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
		evaluationContext.setVariable("myKeyId", "xKVup8B1Q+CkHaVRx+qa+g==");

		when(ctx.getEvaluationContext(any())).thenReturn(evaluationContext);

		EncryptionKey key = EncryptionKeyResolver.annotated(fallbackKeyResolver).getKey(ctx);

		assertThat(key).isEqualTo(EncryptionKey.keyId(
				new BsonBinary(BsonBinarySubType.UUID_STANDARD, Base64.getDecoder().decode("xKVup8B1Q+CkHaVRx+qa+g=="))));
	}

	private <T> EncryptionContext prepareEncryptionContext(Class<T> type, Function<T, ?> property) {

		EncryptionContext encryptionContext = mock(EncryptionContext.class);
		when(encryptionContext.getProperty()).thenReturn(mappingContext.getPersistentPropertyFor(type, property));
		return encryptionContext;
	}

	class AnnotatedWithExplicitlyEncrypted {

		String notAnnotated;

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) //
		String algorithm;

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, keyAltName = "sec-key-name") //
		String algorithmAndAltKeyName;

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, keyAltName = "/notAnnotated") //
		String algorithmAndAltKeyNameFromPropertyValue;

		public String getNotAnnotated() {
			return this.notAnnotated;
		}

		public String getAlgorithm() {
			return this.algorithm;
		}

		public String getAlgorithmAndAltKeyName() {
			return this.algorithmAndAltKeyName;
		}

		public String getAlgorithmAndAltKeyNameFromPropertyValue() {
			return this.algorithmAndAltKeyNameFromPropertyValue;
		}

		public void setNotAnnotated(String notAnnotated) {
			this.notAnnotated = notAnnotated;
		}

		public void setAlgorithm(String algorithm) {
			this.algorithm = algorithm;
		}

		public void setAlgorithmAndAltKeyName(String algorithmAndAltKeyName) {
			this.algorithmAndAltKeyName = algorithmAndAltKeyName;
		}

		public void setAlgorithmAndAltKeyNameFromPropertyValue(String algorithmAndAltKeyNameFromPropertyValue) {
			this.algorithmAndAltKeyNameFromPropertyValue = algorithmAndAltKeyNameFromPropertyValue;
		}

		public String toString() {
			return "EncryptionKeyResolverUnitTests.AnnotatedWithExplicitlyEncrypted(notAnnotated=" + this.getNotAnnotated()
					+ ", algorithm=" + this.getAlgorithm() + ", algorithmAndAltKeyName=" + this.getAlgorithmAndAltKeyName()
					+ ", algorithmAndAltKeyNameFromPropertyValue=" + this.getAlgorithmAndAltKeyNameFromPropertyValue() + ")";
		}
	}

	@Encrypted(keyId = "xKVup8B1Q+CkHaVRx+qa+g==")
	class AnnotatedWithExplicitlyEncryptedHavingDefaultAlgorithmServedViaAnnotationOnType {

		@ExplicitEncrypted //
		String keyIdFromDomainType;

		@ExplicitEncrypted(keyAltName = "sec-key-name") //
		String altKeyNameFromPropertyIgnoringKeyIdFromDomainType;

		public AnnotatedWithExplicitlyEncryptedHavingDefaultAlgorithmServedViaAnnotationOnType() {}

		public String getKeyIdFromDomainType() {
			return this.keyIdFromDomainType;
		}

		public String getAltKeyNameFromPropertyIgnoringKeyIdFromDomainType() {
			return this.altKeyNameFromPropertyIgnoringKeyIdFromDomainType;
		}

		public void setKeyIdFromDomainType(String keyIdFromDomainType) {
			this.keyIdFromDomainType = keyIdFromDomainType;
		}

		public void setAltKeyNameFromPropertyIgnoringKeyIdFromDomainType(
				String altKeyNameFromPropertyIgnoringKeyIdFromDomainType) {
			this.altKeyNameFromPropertyIgnoringKeyIdFromDomainType = altKeyNameFromPropertyIgnoringKeyIdFromDomainType;
		}

		public String toString() {
			return "EncryptionKeyResolverUnitTests.AnnotatedWithExplicitlyEncryptedHavingDefaultAlgorithmServedViaAnnotationOnType(keyIdFromDomainType="
					+ this.getKeyIdFromDomainType() + ", altKeyNameFromPropertyIgnoringKeyIdFromDomainType="
					+ this.getAltKeyNameFromPropertyIgnoringKeyIdFromDomainType() + ")";
		}
	}

	@Encrypted(keyId = "#{#myKeyId}")
	class KeyIdFromSpel {

		@ExplicitEncrypted //
		String keyIdFromDomainType;

		public String getKeyIdFromDomainType() {
			return this.keyIdFromDomainType;
		}

		public void setKeyIdFromDomainType(String keyIdFromDomainType) {
			this.keyIdFromDomainType = keyIdFromDomainType;
		}

		public String toString() {
			return "EncryptionKeyResolverUnitTests.KeyIdFromSpel(keyIdFromDomainType=" + this.getKeyIdFromDomainType() + ")";
		}
	}
}
