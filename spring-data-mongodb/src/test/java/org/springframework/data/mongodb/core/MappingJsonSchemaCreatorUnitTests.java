/*
 * Copyright 2019 the original author or authors.
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

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Transient;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 */
public class MappingJsonSchemaCreatorUnitTests {

	MappingMongoConverter converter;
	MongoMappingContext mappingContext;
	MappingJsonSchemaCreator schemaCreator;

	@Before
	public void setUp() {

		mappingContext = new MongoMappingContext();
		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		schemaCreator = new MappingJsonSchemaCreator(converter);
	}

	@Test // DATAMONGO-1849
	public void simpleTypes() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(VariousFieldTypes.class);

		Assertions.assertThat(schema.toDocument().get("$jsonSchema", Document.class))
				.isEqualTo(Document.parse(VARIOUS_FIELD_TYPES));
	}

	@Test // DATAMONGO-1849
	public void requiredCtorArgs() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(RequiredArgsCtor.class);
		Assertions.assertThat(schema.toDocument().get("$jsonSchema", Document.class))
				.isEqualTo(Document.parse(REQUIRED_ARGS_CTOR));
	}

	@Test // DATAMONGO-1849
	public void withNestedObject() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(WithNestedDomainType.class);
		Assertions.assertThat(schema.toDocument().get("$jsonSchema", Document.class))
				.isEqualTo(Document.parse(WITH_NESTED_DOMAIN_TYPE));
	}

	@Test // DATAMONGO-1849
	public void withNestedThatHasRequiredFieldsObject() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(WithNestedDomainTypeHavingRequiredCtor.class);

		Assertions.assertThat(schema.toDocument().get("$jsonSchema", Document.class))
				.isEqualTo(Document.parse(WITH_NESTED_DOMAIN_TYPE_HAVING_REQUIRED_CTOR));
	}

	@Test // DATAMONGO-1849
	public void withRemappedIdType() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(WithExplicitMongoIdTypeMapping.class);
		Assertions.assertThat(schema.toDocument().get("$jsonSchema", Document.class))
				.isEqualTo(Document.parse(WITH_EXPLICIT_MONGO_ID_TYPE_MAPPING));
	}

	@Test // DATAMONGO-1849
	public void cyclic() {

		MongoJsonSchema schema = schemaCreator.createSchemaFor(Cyclic.class);
		Assertions.assertThat(schema.toDocument().get("$jsonSchema", Document.class)).isEqualTo(Document.parse(CYCLIC));
	}

	@Test // DATAMONGO-1849
	public void convterRegisterd() {

		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		MongoCustomConversions mcc = new MongoCustomConversions(
				Collections.singletonList(SimpleToDocumentConverter.INSTANCE));
		converter.setCustomConversions(mcc);
		converter.afterPropertiesSet();

		schemaCreator = new MappingJsonSchemaCreator(converter);

		MongoJsonSchema schema = schemaCreator.createSchemaFor(WithNestedDomainType.class);
		Assertions.assertThat(schema.toDocument().get("$jsonSchema", Document.class)).isEqualTo(Document.parse(
				"{ 'type' : 'object', 'properties' : { '_id' : { 'type' : 'object' }, 'nested' : { 'type' : 'object' } } }"));
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
			"        'enumProperty' : " + JUST_SOME_ENUM + //
			"     }" + //
			"}";

	static class VariousFieldTypes {

		@Field("id") String id;
		@Field("re-named-property") String renamedProperty;
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

	// --> REQUIRED ARGS (CTOR)

	static final String REQUIRED_ARGS_CTOR = "" + //
			"{" + //
			"    'type' : 'object'," + //
			"    'required' : ['requiredCtorArg']," + //
			"    'properties' : {" + //
			"        'requiredCtorArg' : { 'type' : 'string' }," + //
			"        'nullableCtorArg' : { 'type' : 'string' }," + //
			"        'optionalArg' : { 'type' : 'string' }" + //
			"     }" + //
			"}";

	static class RequiredArgsCtor {

		String requiredCtorArg;
		@Nullable String nullableCtorArg;
		String optionalArg;

		public RequiredArgsCtor(String requiredCtorArg, @Nullable String nullableCtorArg) {

			this.requiredCtorArg = requiredCtorArg;
			this.nullableCtorArg = nullableCtorArg;
		}
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

	static final String WITH_NESTED_DOMAIN_TYPE_HAVING_REQUIRED_CTOR = "" + //
			"{" + //
			"    'type' : 'object'," + //
			"    'properties' : {" + //
			"        '_id' : { 'type' : 'object' }," + //
			"        'nested' : " + REQUIRED_ARGS_CTOR + //
			"     }" + //
			"}";

	static class WithNestedDomainTypeHavingRequiredCtor {

		String id;
		RequiredArgsCtor nested;
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

}
