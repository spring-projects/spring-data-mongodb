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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.EncryptionAlgorithms.*;

import lombok.Data;

import java.util.List;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.mongodb.core.convert.MongoConversionContext;
import org.springframework.data.mongodb.core.convert.encryption.MongoEncryptionConverter;
import org.springframework.data.mongodb.core.mapping.ExplicitlyEncrypted;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.test.util.MongoTestMappingContext;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MongoEncryptionConverterUnitTests {

	@Mock //
	Encryption<BsonValue, BsonBinary> encryption;

	@Mock //
	EncryptionKeyResolver fallbackKeyResolver;

	@Mock //
	MongoConversionContext conversionContext;

	MongoTestMappingContext mappingContext = MongoTestMappingContext.newTestContext();
	EncryptionKeyResolver keyResolver;
	MongoEncryptionConverter converter;

	@Captor ArgumentCaptor<EncryptionOptions> encryptionOptions;

	@Captor ArgumentCaptor<BsonValue> valueToBeEncrypted;

	@BeforeEach
	void beforeEach() {

		when(fallbackKeyResolver.getKey(any())).thenReturn(EncryptionKey.altKeyName("default"));
		when(encryption.encrypt(valueToBeEncrypted.capture(), encryptionOptions.capture()))
				.thenReturn(new BsonBinary(new byte[0]));
		keyResolver = EncryptionKeyResolver.annotationBased(fallbackKeyResolver);
		converter = new MongoEncryptionConverter(encryption, keyResolver);
	}

	@Test // GH-4284
	void delegatesConversionOfSimpleValueWithDefaultEncryptionKeyFromKeyResolver() {

		when(conversionContext.getProperty())
				.thenReturn(mappingContext.getPersistentPropertyFor(Type.class, Type::getStringValueWithAlgorithmOnly));

		converter.write("foo", conversionContext);

		assertThat(valueToBeEncrypted.getValue()).isEqualTo(new BsonString("foo"));
		assertThat(encryptionOptions.getValue()).isEqualTo(
				new EncryptionOptions(AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic).setKey(EncryptionKey.altKeyName("default")));
	}

	@Test // GH-4284
	void favorsAltKeyNameIfPresent() {

		when(conversionContext.getProperty()).thenReturn(
				mappingContext.getPersistentPropertyFor(Type.class, Type::getStringValueWithAlgorithmAndAltKeyName));

		converter.write("foo", conversionContext);

		assertThat(encryptionOptions.getValue()).isEqualTo(
				new EncryptionOptions(AEAD_AES_256_CBC_HMAC_SHA_512_Random).setKey(EncryptionKey.altKeyName("sec-key-name")));
	}

	@Test // GH-4284
	void readsAltKeyNameFromProperty() {

		when(conversionContext.getProperty()).thenReturn(mappingContext.getPersistentPropertyFor(Type.class,
				Type::getStringValueWithAlgorithmAndAltKeyNameFromPropertyValue));

		ArgumentCaptor<String> path = ArgumentCaptor.forClass(String.class);
		when(conversionContext.getValue(path.capture())).thenReturn("(ツ)");

		converter.write("foo", conversionContext);
		assertThat(path.getValue()).isEqualTo("notAnnotated");

		assertThat(encryptionOptions.getValue())
				.isEqualTo(new EncryptionOptions(AEAD_AES_256_CBC_HMAC_SHA_512_Random).setKey(EncryptionKey.altKeyName("(ツ)")));
	}

	@Test // GH-4284
	void delegatesConversionOfEntityTypes() {

		Document convertedValue = new Document("unencryptedValue", "nested-unencrypted");
		MongoPersistentProperty property = mappingContext.getPersistentPropertyFor(Type.class,
				Type::getNestedFullyEncrypted);
		when(conversionContext.getProperty()).thenReturn(property);
		doReturn(convertedValue).when(conversionContext).write(any(), eq(property.getTypeInformation()));

		ArgumentCaptor<String> path = ArgumentCaptor.forClass(String.class);
		when(conversionContext.getValue(path.capture())).thenReturn("(ツ)");

		JustATypeWithAnUnencryptedField source = new JustATypeWithAnUnencryptedField();
		source.unencryptedValue = "nested-unencrypted";

		converter.write(source, conversionContext);

		assertThat(valueToBeEncrypted.getValue()).isEqualTo(convertedValue.toBsonDocument());
	}

	@Test // GH-4284
	void listsOfSimpleTypesAreConvertedEntirely() {

		MongoPersistentProperty property = mappingContext.getPersistentPropertyFor(Type.class, Type::getListOfString);
		when(conversionContext.getProperty()).thenReturn(property);

		converter.write(List.of("one", "two"), conversionContext);

		assertThat(valueToBeEncrypted.getValue())
				.isEqualTo(new BsonArray(List.of(new BsonString("one"), new BsonString("two"))));
	}

	@Test // GH-4284
	void listsOfComplexTypesAreConvertedEntirely() {

		Document convertedValue1 = new Document("unencryptedValue", "nested-unencrypted-1");
		Document convertedValue2 = new Document("unencryptedValue", "nested-unencrypted-2");

		MongoPersistentProperty property = mappingContext.getPersistentPropertyFor(Type.class, Type::getListOfComplex);
		when(conversionContext.getProperty()).thenReturn(property);
		doReturn(convertedValue1, convertedValue2).when(conversionContext).write(any(), eq(property.getTypeInformation()));

		JustATypeWithAnUnencryptedField source1 = new JustATypeWithAnUnencryptedField();
		source1.unencryptedValue = "nested-unencrypted-1";

		JustATypeWithAnUnencryptedField source2 = new JustATypeWithAnUnencryptedField();
		source2.unencryptedValue = "nested-unencrypted-1";

		converter.write(List.of(source1, source2), conversionContext);

		assertThat(valueToBeEncrypted.getValue())
				.isEqualTo(new BsonArray(List.of(convertedValue1.toBsonDocument(), convertedValue2.toBsonDocument())));
	}

	@Test // GH-4284
	void simpleMapsAreConvertedEntirely() {

		MongoPersistentProperty property = mappingContext.getPersistentPropertyFor(Type.class, Type::getMapOfString);
		when(conversionContext.getProperty()).thenReturn(property);
		doReturn(new Document("k1", "v1").append("k2", "v2")).when(conversionContext).write(any(),
				eq(property.getTypeInformation()));

		converter.write(Map.of("k1", "v1", "k2", "v2"), conversionContext);

		assertThat(valueToBeEncrypted.getValue())
				.isEqualTo(new Document("k1", new BsonString("v1")).append("k2", new BsonString("v2")).toBsonDocument());
	}

	@Test // GH-4284
	void complexMapsAreConvertedEntirely() {

		Document convertedValue1 = new Document("unencryptedValue", "nested-unencrypted-1");
		Document convertedValue2 = new Document("unencryptedValue", "nested-unencrypted-2");

		MongoPersistentProperty property = mappingContext.getPersistentPropertyFor(Type.class, Type::getMapOfComplex);
		when(conversionContext.getProperty()).thenReturn(property);
		doReturn(new Document("k1", convertedValue1).append("k2", convertedValue2)).when(conversionContext).write(any(),
				eq(property.getTypeInformation()));

		JustATypeWithAnUnencryptedField source1 = new JustATypeWithAnUnencryptedField();
		source1.unencryptedValue = "nested-unencrypted-1";

		JustATypeWithAnUnencryptedField source2 = new JustATypeWithAnUnencryptedField();
		source2.unencryptedValue = "nested-unencrypted-1";

		converter.write(Map.of("k1", source1, "k2", source2), conversionContext);

		assertThat(valueToBeEncrypted.getValue()).isEqualTo(new Document("k1", convertedValue1.toBsonDocument())
				.append("k2", convertedValue2.toBsonDocument()).toBsonDocument());
	}

	@Data
	static class Type {

		String notAnnotated;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic) //
		String stringValueWithAlgorithmOnly;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, altKeyName = "sec-key-name") //
		String stringValueWithAlgorithmAndAltKeyName;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, altKeyName = "/notAnnotated") //
		String stringValueWithAlgorithmAndAltKeyNameFromPropertyValue;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) // full document must be random
		JustATypeWithAnUnencryptedField nestedFullyEncrypted;

		NestedWithEncryptedField nestedWithEncryptedField;

		// Client-Side Field Level Encryption does not support encrypting individual array elements
		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) //
		List<String> listOfString;

		// Client-Side Field Level Encryption does not support encrypting individual array elements
		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) // lists must be random
		List<JustATypeWithAnUnencryptedField> listOfComplex;

		// just as it was a domain type encrypt the entire thing here
		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) //
		Map<String, String> mapOfString;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) //
		Map<String, JustATypeWithAnUnencryptedField> mapOfComplex;

		RecordWithEncryptedValue recordWithEncryptedValue;

		List<RecordWithEncryptedValue> listOfRecordWithEncryptedValue;
	}

	static class JustATypeWithAnUnencryptedField {

		String unencryptedValue;
	}

	static class NestedWithEncryptedField extends JustATypeWithAnUnencryptedField {

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic) //
		String encryptedValue;
	}

	record RecordWithEncryptedValue(@ExplicitlyEncrypted String value) {
	}
}
