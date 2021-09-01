/*
 * Copyright 2019-2021 the original author or authors.
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

import ch.qos.logback.core.net.SyslogOutputStream;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.annotation.Transient;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.EncryptedField;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.spel.spi.EvaluationContextExtension;
import org.springframework.data.spel.spi.Function;

/**
 * Unit tests for {@link MappingMongoJsonSchemaCreator}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MappingMongoJsonSchemaCreatorUnitTests {

	MappingMongoConverter converter;
	MongoMappingContext mappingContext;
	MappingMongoJsonSchemaCreator schemaCreator;

	@BeforeEach
	public void setUp() {

		mappingContext = new MongoMappingContext();
		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		schemaCreator = new MappingMongoJsonSchemaCreator(converter);
	}

	@Test // DATAMONGO-1849
	public void simpleTypes() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(VariousFieldTypes.class);

		assertThat(schema.toDocument().get("$jsonSchema", Document.class)).isEqualTo(Document.parse(VARIOUS_FIELD_TYPES));
	}

	@Test // DATAMONGO-1849
	public void withRemappedIdType() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(WithExplicitMongoIdTypeMapping.class);
		assertThat(schema.toDocument().get("$jsonSchema", Document.class)).isEqualTo(WITH_EXPLICIT_MONGO_ID_TYPE_MAPPING);
	}

	@Test // DATAMONGO-1849
	public void cyclic() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(Cyclic.class);
		assertThat(schema.toDocument().get("$jsonSchema", Document.class)).isEqualTo(CYCLIC);
	}

	@Test // DATAMONGO-1849
	public void converterRegistered() {

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

	// --> TYPES AND JSON

	// --> ENUM

	static final String JUST_SOME_ENUM = "{ 'type' : 'string', 'enum' : ['ONE', 'TWO'] }";

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
		List<String> collectionProperty;
		Map<String, String> mapProperty;
		Object objectProperty;
		JustSomeEnum enumProperty;
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

	@Encrypted(keyId = "xKVup8B1Q+CkHaVRx+qa+g==")
	static class Patient {
		String name;

		@EncryptedField(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic") Integer ssn;

		@EncryptedField(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Random") String bloodType;

		String keyAltNameField;

		@EncryptedField(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Random") List<Map<String, String>> medicalRecords;
		Insurance insurance;
	}

	static class Insurance {

		@EncryptedField(algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic") Integer policyNumber;
		String provider;
	}

	@Test // GH-???
	public void xxx() {

		MongoJsonSchema schema = MongoJsonSchemaCreator.create().createSchemaFor(Patient.class);

		System.out.println(schema.toDocument().get("$jsonSchema", Document.class)
				.toJson(JsonWriterSettings.builder().indent(true).build()));

		// assertThat(schema.toDocument().get("$jsonSchema",
		// Document.class)).isEqualTo(Document.parse(VARIOUS_FIELD_TYPES));
	}

	@Test // GH-???
	public void csfle/*encryptedFieldsOnly*/() {

		MongoJsonSchema schema = MongoJsonSchemaCreator.create().filter(MongoJsonSchemaCreator.encryptedOnly())
				.wrapperName("db.patient").createSchemaFor(Patient.class);

		Document $jsonSchema = schema.toDocument().get("db.patient", Document.class);
		System.out.println($jsonSchema.toJson(JsonWriterSettings.builder().indent(true).build()));

		assertThat($jsonSchema).isEqualTo(Document.parse(patientSchema));

		// assertThat(schema.toDocument().get("$jsonSchema",
		// Document.class)).isEqualTo(Document.parse(VARIOUS_FIELD_TYPES));
	}

	@Test
	public void spelCheck() {

		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.registerBean("encryptionExtension", EncryptionExtension.class, () -> new EncryptionExtension());
		applicationContext.refresh();

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setApplicationContext(applicationContext);
		mappingContext.afterPropertiesSet();

		MongoJsonSchema schema = MongoJsonSchemaCreator.create(mappingContext).filter(MongoJsonSchemaCreator.encryptedOnly())
				.createSchemaFor(SpELPatient.class);

		Document $jsonSchema = schema.toDocument().get("$jsonSchema", Document.class);
		System.out.println($jsonSchema.toJson(JsonWriterSettings.builder().indent(true).build()));
	}

	@Test
	public void spelCheckMethod() {

		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.registerBean("encryptionExtension", EncryptionExtension.class, () -> new EncryptionExtension());
		applicationContext.refresh();

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setApplicationContext(applicationContext);
		mappingContext.afterPropertiesSet();

		MongoJsonSchema schema = MongoJsonSchemaCreator.create(mappingContext).filter(MongoJsonSchemaCreator.encryptedOnly())
				.createSchemaFor(MethodSpELPatient.class);

		Document $jsonSchema = schema.toDocument().get("$jsonSchema", Document.class);
		System.out.println($jsonSchema.toJson(JsonWriterSettings.builder().indent(true).build()));
	}

	@Encrypted(keyId = "#{patientKey}")
	static class SpELPatient {

	}

	@Encrypted(keyId = "#{mongocrypt.computeKeyId(#target)}")
	static class MethodSpELPatient {

		@EncryptedField(keyId = "#{mongocrypt.computeKeyId(#target)}", algorithm = EncryptionAlgorithms.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic)
		Integer policyNumber;

		String provider;
	}

	public static class EncryptionExtension implements EvaluationContextExtension {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.spel.spi.EvaluationContextExtension#getExtensionId()
		 */
		@Override
		public String getExtensionId() {
			return "mongocrypt";
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.spel.spi.EvaluationContextExtension#getProperties()
		 */
		@Override
		public Map<String, Object> getProperties() {

			Map<String, Object> properties = new LinkedHashMap<>();
			properties.put("patientKey", "xKVup8B1Q+CkHaVRx+qa+g==");
			return properties;
		}

		@Override
		public Map<String, Function> getFunctions() {
			try {
				return Collections.singletonMap("computeKeyId", new Function(EncryptionExtension.class.getMethod("computeKeyId", String.class), this));
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			}
			return Collections.emptyMap();
		}

		public String computeKeyId(String target) {

			System.out.println("target: " + target);
			return "xKVup8B1Q+CkHaVRx+qa+g==";
		}
		//		@Override
//		public Map<String, Function> getFunctions() {
//			return null;
//		}
	}

//	@org.springframework.data.mongodb.core.mapping.Document(collation = "#{myCollation}")
//	class WithCollationFromSpEL {}
//
//	@org.springframework.data.mongodb.core.mapping.Document(collection = "#{myProperty}", collation = "#{myCollation}")
//	class WithCollectionAndCollationFromSpEL {}

	/**
	 * @Test // DATAMONGO-2565
	 * 	void usesCorrectExpressionsForCollectionAndCollation() {
	 *
	 * 		BasicMongoPersistentEntity<WithCollectionAndCollationFromSpEL> entity = new BasicMongoPersistentEntity<>(
	 * 				ClassTypeInformation.from(WithCollectionAndCollationFromSpEL.class));
	 * 		entity.setEvaluationContextProvider(
	 * 				new ExtensionAwareEvaluationContextProvider(Collections.singletonList(new SampleExtension())));
	 *
	 * 		assertThat(entity.getCollection()).isEqualTo("collectionName");
	 * 		assertThat(entity.getCollation()).isEqualTo(Collation.of("en_US"));
	 *        }
	 */

	String patientSchema = "{\n" + "  \"type\": \"object\",\n" + "  \"encryptMetadata\": {\n" + "    \"keyId\": [\n"
			+ "      {\n" + "        \"$binary\": {\n" + "          \"base64\": \"xKVup8B1Q+CkHaVRx+qa+g==\",\n"
			+ "          \"subType\": \"04\"\n" + "        }\n" + "      }\n" + "    ]\n" + "  },\n" + "  \"properties\": {\n"
			+ "    \"ssn\": {\n" + "      \"encrypt\": {\n" + "        \"bsonType\": \"int\",\n"
			+ "        \"algorithm\": \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\"\n" + "      }\n" + "    },\n"
			+ "    \"bloodType\": {\n" + "      \"encrypt\": {\n" + "        \"bsonType\": \"string\",\n"
			+ "        \"algorithm\": \"AEAD_AES_256_CBC_HMAC_SHA_512-Random\"\n" + "      }\n" + "    },\n"
			+ "    \"medicalRecords\": {\n" + "      \"encrypt\": {\n" + "        \"bsonType\": \"array\",\n"
			+ "        \"algorithm\": \"AEAD_AES_256_CBC_HMAC_SHA_512-Random\"\n" + "      }\n" + "    },\n"
			+ "    \"insurance\": {\n" + "      \"type\": \"object\",\n" + "      \"properties\": {\n"
			+ "        \"policyNumber\": {\n" + "          \"encrypt\": {\n" + "            \"bsonType\": \"int\",\n"
			+ "            \"algorithm\": \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\"\n" + "          }\n" + "        }\n"
			+ "      }\n" + "    }\n" + "  }\n" + "}";

}
