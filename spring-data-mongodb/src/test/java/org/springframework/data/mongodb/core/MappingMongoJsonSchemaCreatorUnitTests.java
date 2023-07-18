/*
 * Copyright 2019-2023 the original author or authors.
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

import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.annotation.Transient;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type;
import org.springframework.data.mongodb.core.schema.JsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema.ConflictResolutionFunction.Resolution;
import org.springframework.data.spel.spi.EvaluationContextExtension;
import org.springframework.data.spel.spi.Function;

/**
 * Unit tests for {@link MappingMongoJsonSchemaCreator}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class MappingMongoJsonSchemaCreatorUnitTests {

	private MappingMongoConverter converter;
	private MongoMappingContext mappingContext;
	private MappingMongoJsonSchemaCreator schemaCreator;

	@BeforeEach
	void setUp() {

		mappingContext = new MongoMappingContext();
		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		schemaCreator = new MappingMongoJsonSchemaCreator(converter);
	}

	@Test // DATAMONGO-1849
	void simpleTypes() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(VariousFieldTypes.class);

		assertThat(schema.toDocument().get("$jsonSchema", Document.class)).isEqualTo(Document.parse(VARIOUS_FIELD_TYPES));
	}

	@Test // DATAMONGO-1849
	void withRemappedIdType() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(WithExplicitMongoIdTypeMapping.class);
		assertThat(schema.toDocument().get("$jsonSchema", Document.class)).isEqualTo(WITH_EXPLICIT_MONGO_ID_TYPE_MAPPING);
	}

	@Test // DATAMONGO-1849
	void cyclic() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(Cyclic.class);
		assertThat(schema.toDocument().get("$jsonSchema", Document.class)).isEqualTo(CYCLIC);
	}

	@Test // DATAMONGO-1849
	void converterRegistered() {

		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		MongoCustomConversions mcc = new MongoCustomConversions(
				Collections.singletonList(SimpleToDocumentConverter.INSTANCE));
		converter.setCustomConversions(mcc);
		converter.afterPropertiesSet();

		schemaCreator = new MappingMongoJsonSchemaCreator(converter);

		MongoJsonSchema schema = schemaCreator.createSchemaFor(WithNestedDomainType.class);
		assertThat(schema.toDocument().get("$jsonSchema", Document.class)).isEqualTo(
				"{ 'type' : 'object', 'properties' : { '_id' : { 'type' : 'object' }, 'nested' : { 'type' : 'object' } } }");
	}

	@Test // GH-3800
	void csfle/*encryptedFieldsOnly*/() {

		MongoJsonSchema schema = MongoJsonSchemaCreator.create() //
				.filter(MongoJsonSchemaCreator.encryptedOnly()) // filter non encrypted fields
				.createSchemaFor(Patient.class);

		Document targetSchema = schema.schemaDocument();
		assertThat(targetSchema.toBsonDocument()).isEqualTo(BsonDocument.parse(PATIENT));
	}

	@Test // GH-3800
	void csfleCyclic/*encryptedFieldsOnly*/() {

		MongoJsonSchema schema = MongoJsonSchemaCreator.create() //
				.filter(MongoJsonSchemaCreator.encryptedOnly()) // filter non encrypted fields
				.createSchemaFor(Cyclic.class);

		Document targetSchema = schema.schemaDocument();
		assertThat(targetSchema).isNotNull();
	}

	@Test // GH-3800
	void csfleWithKeyFromProperties() {

		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.registerBean("encryptionExtension", EncryptionExtension.class, () -> new EncryptionExtension());
		applicationContext.refresh();

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setApplicationContext(applicationContext);
		mappingContext.afterPropertiesSet();

		MongoJsonSchema schema = MongoJsonSchemaCreator.create(mappingContext) //
				.filter(MongoJsonSchemaCreator.encryptedOnly()) //
				.createSchemaFor(EncryptionMetadataFromProperty.class);

		assertThat(schema.schemaDocument().toBsonDocument()).isEqualTo(BsonDocument.parse(ENC_FROM_PROPERTY_SCHEMA));
	}

	@Test // GH-3800
	void csfleWithKeyFromMethod() {

		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.registerBean("encryptionExtension", EncryptionExtension.class, () -> new EncryptionExtension());
		applicationContext.refresh();

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setApplicationContext(applicationContext);
		mappingContext.afterPropertiesSet();

		MongoJsonSchema schema = MongoJsonSchemaCreator.create(mappingContext) //
				.filter(MongoJsonSchemaCreator.encryptedOnly()) //
				.createSchemaFor(EncryptionMetadataFromMethod.class);

		assertThat(schema.schemaDocument().toBsonDocument()).isEqualTo(BsonDocument.parse(ENC_FROM_METHOD_SCHEMA));
	}

	// --> Combining Schemas and Properties

	@Test // GH-3870
	void shouldAllowToSpecifyPolymorphicTypesForProperty() {

		MongoJsonSchema schema = MongoJsonSchemaCreator.create() //
				.property("objectValue").withTypes(A.class, B.class).createSchemaFor(SomeTestObject.class);

		Document targetSchema = schema.schemaDocument();
		assertThat(targetSchema) //
				.containsEntry("properties.objectValue.properties.aNonEncrypted", new Document("type", "string")) //
				.containsEntry("properties.objectValue.properties.aEncrypted", ENCRYPTED_BSON_STRING) //
				.containsEntry("properties.objectValue.properties.bEncrypted", ENCRYPTED_BSON_STRING);
	}

	@Test // GH-3870
	void shouldAllowToSpecifyNestedPolymorphicTypesForProperty() {

		MongoJsonSchema schema = MongoJsonSchemaCreator.create() //
				.property("value.objectValue").withTypes(A.class, B.class) //
				.createSchemaFor(WrapperAroundA.class);

		assertThat(schema.schemaDocument()) //
				.containsEntry("properties.value.properties.objectValue.properties.aNonEncrypted",
						new Document("type", "string")) //
				.containsEntry("properties.value.properties.objectValue.properties.aEncrypted", ENCRYPTED_BSON_STRING) //
				.containsEntry("properties.value.properties.objectValue.properties.bEncrypted", ENCRYPTED_BSON_STRING);

	}

	@Test // GH-3870
	void shouldAllowToSpecifyGenericTypesForProperty() {

		MongoJsonSchema schema = MongoJsonSchemaCreator.create() //
				.property("genericValue").withTypes(A.class, B.class).createSchemaFor(SomeTestObject.class);

		assertThat(schema.schemaDocument()) //
				.containsEntry("properties.genericValue.properties.aNonEncrypted", new Document("type", "string")) //
				.containsEntry("properties.genericValue.properties.aEncrypted", ENCRYPTED_BSON_STRING) //
				.containsEntry("properties.genericValue.properties.bEncrypted", ENCRYPTED_BSON_STRING);
	}

	@Test // GH-3870
	void encryptionFilterShouldCaptureSpecifiedPolymorphicTypesForProperty() {

		MongoJsonSchema schema = MongoJsonSchemaCreator.create() //
				.property("objectValue").withTypes(A.class, B.class) //
				.filter(MongoJsonSchemaCreator.encryptedOnly()) //
				.createSchemaFor(SomeTestObject.class);

		assertThat(schema.schemaDocument()) //
				.doesNotContainKey("properties.objectValue.properties.aNonEncrypted") //
				.containsEntry("properties.objectValue.properties.aEncrypted", ENCRYPTED_BSON_STRING) //
				.containsEntry("properties.objectValue.properties.bEncrypted", ENCRYPTED_BSON_STRING);
	}

	@Test // GH-3870
	void allowsToCreateCombinedSchemaWhenPropertiesDoNotOverlap() {

		MongoJsonSchema schema = MongoJsonSchemaCreator.create().mergedSchemaFor(A.class, B.class, C.class);

		assertThat(schema.schemaDocument()) //
				.containsEntry("properties.aNonEncrypted", new Document("type", "string")) //
				.containsEntry("properties.aEncrypted", ENCRYPTED_BSON_STRING) //
				.containsEntry("properties.bEncrypted", ENCRYPTED_BSON_STRING) //
				.containsEntry("properties.cEncrypted", ENCRYPTED_BSON_STRING);
	}

	@Test // GH-3870
	void combinedSchemaFailsOnPropertyClash() {

		MongoJsonSchema schemaA = MongoJsonSchemaCreator.create() //
				.createSchemaFor(A.class);
		MongoJsonSchema schemaAButDifferent = MongoJsonSchemaCreator.create() //
				.createSchemaFor(PropertyClashWithA.class);

		MongoJsonSchema targetSchema = schemaA.mergeWith(schemaAButDifferent);

		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(targetSchema::schemaDocument);
	}

	@Test // GH-3870
	void combinedSchemaAllowsToCompensateErrors() {

		MongoJsonSchema schemaA = MongoJsonSchemaCreator.create() //
				.createSchemaFor(A.class);
		MongoJsonSchema schemaAButDifferent = MongoJsonSchemaCreator.create() //
				.createSchemaFor(PropertyClashWithA.class);

		MongoJsonSchema schema = schemaA.mergeWith(Collections.singleton(schemaAButDifferent),
				(path, a, b) -> Resolution.ofValue(path, "object"));

		assertThat(schema.schemaDocument()) //
				.containsEntry("properties.aNonEncrypted", new Document("type", "object"));
	}

	@Test // GH-3870
	void bsonTypeVsJustTypeValueResolutionIsDoneByDefault() {

		MongoJsonSchema schemaUsingType = MongoJsonSchema.builder()
				.property(JsonSchemaProperty.named("value").ofType(Type.jsonTypeOf("string"))).build();
		MongoJsonSchema schemaUsingBsonType = MongoJsonSchema.builder()
				.property(JsonSchemaProperty.named("value").ofType(Type.bsonTypeOf("string"))).build();

		MongoJsonSchema targetSchema = MongoJsonSchema.merge(schemaUsingType, schemaUsingBsonType);

		assertThat(targetSchema.schemaDocument()) //
				.containsEntry("properties.value", new Document("type", "string"));
	}

	@Test // GH-4454
	void wrapEncryptedEntityTypeLikeProperty() {

		MongoJsonSchema schema = MongoJsonSchemaCreator.create() //
				.filter(MongoJsonSchemaCreator.encryptedOnly()) // filter non encrypted fields
				.createSchemaFor(WithEncryptedEntityLikeProperty.class);

		assertThat(schema.schemaDocument()) //
				.containsEntry("properties.domainTypeValue", Document.parse("{'encrypt': {'bsonType': 'object' } }"));
	}

	// --> TYPES AND JSON

	// --> ENUM

	private static final String JUST_SOME_ENUM = "{ 'type' : 'string', 'enum' : ['ONE', 'TWO'] }";

	enum JustSomeEnum {
		ONE, TWO
	}

	// --> VARIOUS FIELD TYPES

	static final String VARIOUS_FIELD_TYPES = "" + //
			"{" + //
			"    'type' : 'object'," + //
			"    'required' : ['primitiveInt']," + //
			"    'properties' : {" + //
			"        'id' : { 'type' : 'string' }," + //
			"        're-named-property' : { 'type' : 'string' }," + //
			"        'retypedProperty' : { 'bsonType' : 'javascript' }," + //
			"        'primitiveInt' : { 'bsonType' : 'int' }," + //
			"        'booleanProperty' : { 'type' : 'boolean' }," + //
			"        'longProperty' : { 'bsonType' : 'long' }," + //
			"        'intProperty' : { 'bsonType' : 'int' }," + //
			"        'dateProperty' : { 'bsonType' : 'date' }," + //
			"        'arrayProperty' : { 'type' : 'array' }," + //
			"        'binaryDataProperty' : { 'bsonType' : 'binData' }," + //
			"        'collectionProperty' : { 'type' : 'array' }," + //
			"        'simpleTypeCollectionProperty' : { 'type' : 'array', 'items' : { 'type' : 'string' } }," + //
			"        'complexTypeCollectionProperty' : { 'type' : 'array', 'items' : { 'type' : 'object', 'properties' : { 'field' : { 'type' : 'string'} } } }" + //
			"        'enumTypeCollectionProperty' : { 'type' : 'array', 'items' : " + JUST_SOME_ENUM + " }" + //
			"        'mapProperty' : { 'type' : 'object' }," + //
			"        'objectProperty' : { 'type' : 'object' }," + //
			"        'enumProperty' : " + JUST_SOME_ENUM + "     }" + //
			"}";

	static class VariousFieldTypes {

		@Field("id") String id;
		@Field("re-named-property") String renamedProperty;
		@Field(targetType = FieldType.SCRIPT) String retypedProperty;
		@Transient String transientProperty;
		int primitiveInt;
		Boolean booleanProperty;
		Long longProperty;
		Integer intProperty;
		Date dateProperty;
		Object[] arrayProperty;
		byte[] binaryDataProperty;
		List<Object> collectionProperty;
		List<String> simpleTypeCollectionProperty;
		List<SomeDomainType> complexTypeCollectionProperty;
		List<JustSomeEnum> enumTypeCollectionProperty;
		Map<String, String> mapProperty;
		Object objectProperty;
		JustSomeEnum enumProperty;
	}

	static class SomeDomainType {
		String field;
	}

	// --> NESTED DOMAIN TYPE

	static final String WITH_NESTED_DOMAIN_TYPE = "" + //
			"{" + //
			"    'type' : 'object'," + //
			"    'properties' : {" + //
			"        '_id' : { 'type' : 'object' }," + //
			"        'nested' : " + VARIOUS_FIELD_TYPES + //
			"     }" + //
			"}";

	static class WithNestedDomainType {

		String id;
		VariousFieldTypes nested;
	}

	// --> EXPLICIT MONGO_ID MAPPING

	final String WITH_EXPLICIT_MONGO_ID_TYPE_MAPPING = "" + //
			"{" + //
			"    'type' : 'object'," + //
			"    'properties' : {" + //
			"        '_id' : { 'bsonType' : 'objectId' }," + //
			"        'nested' : " + VARIOUS_FIELD_TYPES + //
			"     }" + //
			"}";

	static class WithExplicitMongoIdTypeMapping {

		@MongoId(targetType = FieldType.OBJECT_ID) String id;
		VariousFieldTypes nested;
	}

	// --> OH NO - A CYCLIC PROPERTY RELATIONSHIP 😱

	static final String CYCLIC_FIN = "" + //
			"{" + //
			"    'type' : 'object'," + //
			"    'properties' : {" + //
			"        'root' : { 'type' : 'string' }" + //
			"        'cyclic' : { 'type' : 'object' }" + //
			"     }" + //
			"}";

	static final String CYCLIC_2 = "" + //
			"{" + //
			"    'type' : 'object'," + //
			"    'properties' : {" + //
			"        'nested2' : { 'type' : 'string' }," + //
			"        'cyclic' : " + CYCLIC_FIN + //
			"     }" + //
			"}";

	class Cyclic2 {

		String nested2;
		Cyclic cyclic;
	}

	static final String CYCLIC_1 = "" + //
			"{" + //
			"    'type' : 'object'," + //
			"    'properties' : {" + //
			"        'nested1' : { 'type' : 'string' }," + //
			"        'cyclic2' : " + CYCLIC_2 + //
			"     }" + //
			"}";

	class Cyclic1 {

		String nested1;
		Cyclic2 cyclic2;
	}

	static final String CYCLIC = "" + //
			"{" + //
			"    'type' : 'object'," + //
			"    'properties' : {" + //
			"        'root' : { 'type' : 'string' }," + //
			"        'cyclic1' : " + CYCLIC_1 + //
			"     }" + //
			"}";

	class Cyclic {

		String root;
		Cyclic1 cyclic1;
	}

	@WritingConverter
	enum SimpleToDocumentConverter
			implements org.springframework.core.convert.converter.Converter<VariousFieldTypes, org.bson.Document> {
		INSTANCE;

		@Override
		public org.bson.Document convert(VariousFieldTypes source) {
			return null;
		}
	}

	static final String PATIENT = "{" + //
			"  'type': 'object'," + //
			"  'encryptMetadata': {" + //
			"    'keyId': [" + //
			"      {" + //
			"        '$binary': {" + //
			"          'base64': 'xKVup8B1Q+CkHaVRx+qa+g=='," + //
			"          'subType': '04'" + //
			"        }" + //
			"      }" + //
			"    ]" + //
			"  }," + //
			"  'properties': {" + //
			"    'ssn': {" + //
			"      'encrypt': {" + //
			"        'bsonType': 'int'," + //
			"        'algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'" + //
			"      }" + //
			"    }," + //
			"    'bloodType': {" + //
			"      'encrypt': {" + //
			"        'bsonType': 'string'," + //
			"        'algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Random'" + //
			"      }" + //
			"    }," + //
			"    'medicalRecords': {" + //
			"      'encrypt': {" + //
			"        'bsonType': 'array'," + //
			"        'algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Random'" + //
			"      }" + //
			"    }," + //
			"    'insurance': {" + //
			"      'type': 'object'," + //
			"      'properties': {" + //
			"        'policyNumber': {" + //
			"          'encrypt': {" + //
			"            'bsonType': 'int'," + //
			"            'algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'" + //
			"          }" + //
			"        }" + //
			"      }" + //
			"    }" + //
			"  }" + //
			"}";

	@Encrypted(keyId = "xKVup8B1Q+CkHaVRx+qa+g==")
	static class Patient {
		String name;

		@Encrypted(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic") //
		Integer ssn;

		@Encrypted(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Random") //
		String bloodType;

		String keyAltNameField;

		@Encrypted(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Random") //
		List<Map<String, String>> medicalRecords;

		Insurance insurance;
	}

	static class Insurance {

		@Encrypted(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic") //
		Integer policyNumber;

		String provider;
	}

	static final String ENC_FROM_PROPERTY_ENTITY_KEY = "C5a5aMB7Ttq4wSJTFeRn8g==";
	static final String ENC_FROM_PROPERTY_PROPOERTY_KEY = "Mw6mdTVPQfm4quqSCLVB3g==";
	static final String ENC_FROM_PROPERTY_SCHEMA = "{" + //
			"  'encryptMetadata': {" + //
			"    'keyId': [" + //
			"      {" + //
			"        '$binary': {" + //
			"          'base64': '" + ENC_FROM_PROPERTY_ENTITY_KEY + "'," + //
			"          'subType': '04'" + //
			"        }" + //
			"      }" + //
			"    ]" + //
			"  }," + //
			"  'type': 'object'," + //
			"  'properties': {" + //
			"    'policyNumber': {" + //
			"      'encrypt': {" + //
			"        'keyId': [" + //
			"          [" + //
			"            {" + //
			"              '$binary': {" + //
			"                'base64': '" + ENC_FROM_PROPERTY_PROPOERTY_KEY + "'," + //
			"                'subType': '04'" + //
			"              }" + //
			"            }" + //
			"          ]" + //
			"        ]," + //
			"        'bsonType': 'int'," + //
			"        'algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'" + //
			"      }" + //
			"    }" + //
			"  }" + //
			"}";

	@Encrypted(keyId = "#{entityKey}")
	static class EncryptionMetadataFromProperty {

		@Encrypted(keyId = "#{propertyKey}", algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic") //
		Integer policyNumber;

		String provider;
	}

	static final String ENC_FROM_METHOD_ENTITY_KEY = "4fPYFM9qSgyRAjgQ2u+IMQ==";
	static final String ENC_FROM_METHOD_PROPOERTY_KEY = "+idiseKwTVCJfSKC3iUeYQ==";
	static final String ENC_FROM_METHOD_SCHEMA = "{" + //
			"  'encryptMetadata': {" + //
			"    'keyId': [" + //
			"      {" + //
			"        '$binary': {" + //
			"          'base64': '" + ENC_FROM_METHOD_ENTITY_KEY + "'," + //
			"          'subType': '04'" + //
			"        }" + //
			"      }" + //
			"    ]" + //
			"  }," + //
			"  'type': 'object'," + //
			"  'properties': {" + //
			"    'policyNumber': {" + //
			"      'encrypt': {" + //
			"        'keyId': [" + //
			"          [" + //
			"            {" + //
			"              '$binary': {" + //
			"                'base64': '" + ENC_FROM_METHOD_PROPOERTY_KEY + "'," + //
			"                'subType': '04'" + //
			"              }" + //
			"            }" + //
			"          ]" + //
			"        ]," + //
			"        'bsonType': 'int'," + //
			"        'algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'" + //
			"      }" + //
			"    }" + //
			"  }" + //
			"}";

	@Encrypted(keyId = "#{mongocrypt.keyId(#target)}")
	static class EncryptionMetadataFromMethod {

		@Encrypted(keyId = "#{mongocrypt.keyId(#target)}", algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic") //
		Integer policyNumber;

		String provider;
	}

	public static class EncryptionExtension implements EvaluationContextExtension {

		@Override
		public String getExtensionId() {
			return "mongocrypt";
		}

		@Override
		public Map<String, Object> getProperties() {

			Map<String, Object> properties = new LinkedHashMap<>();
			properties.put("entityKey", ENC_FROM_PROPERTY_ENTITY_KEY);
			properties.put("propertyKey", ENC_FROM_PROPERTY_PROPOERTY_KEY);
			return properties;
		}

		@Override
		public Map<String, Function> getFunctions() {
			try {
				return Collections.singletonMap("keyId",
						new Function(EncryptionExtension.class.getMethod("keyId", String.class), this));
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			}
			return Collections.emptyMap();
		}

		public String keyId(String target) {

			if (target.equals("EncryptionMetadataFromMethod")) {
				return ENC_FROM_METHOD_ENTITY_KEY;
			}

			if (target.equals("EncryptionMetadataFromMethod.policyNumber")) {
				return ENC_FROM_METHOD_PROPOERTY_KEY;
			}

			return "xKVup8B1Q+CkHaVRx+qa+g==";
		}
	}

	private static final Document ENCRYPTED_BSON_STRING = Document
			.parse("{'encrypt': { 'bsonType': 'string','algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'} }");

	static class SomeTestObject<T> {
		T genericValue;
		Object objectValue;
	}

	static class RootWithGenerics<S, T> {
		S sValue;
		T tValue;
	}

	static class SubWithFixedGeneric<T> extends RootWithGenerics<A, T> {

	}

	static class Concrete extends SubWithFixedGeneric<B> {

	}

	static class WrapperAroundA {

		SomeTestObject value;
	}

	static class A {

		String aNonEncrypted;

		@Encrypted(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic") String aEncrypted;
	}

	static class B {
		@Encrypted(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic") String bEncrypted;
	}

	static class C extends A {
		@Encrypted(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic") String cEncrypted;
	}

	static class PropertyClashWithA {
		Integer aNonEncrypted;
	}

	@Encrypted(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
	static class WithEncryptedEntityLikeProperty {
		@Encrypted SomeDomainType domainTypeValue;
	}
}
