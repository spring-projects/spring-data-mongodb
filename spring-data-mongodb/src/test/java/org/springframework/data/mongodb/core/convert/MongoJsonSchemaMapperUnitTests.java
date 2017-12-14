/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.convert;

import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.schema.JsonSchemaProperty.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;

/**
 * Unit tests for {@link MongoJsonSchemaMapper}.
 *
 * @author Christoph Strobl
 */
public class MongoJsonSchemaMapperUnitTests {

	public @Rule ExpectedException exception = ExpectedException.none();

	MongoJsonSchemaMapper mapper;

	Document addressProperty = new Document("type", "object").append("required", Arrays.asList("street", "postCode"))
			.append("properties",
					new Document("street", new Document("type", "string")).append("postCode", new Document("type", "string")));

	Document mappedAddressProperty = new Document("type", "object")
			.append("required", Arrays.asList("street", "post_code")).append("properties",
					new Document("street", new Document("type", "string")).append("post_code", new Document("type", "string")));

	Document nameProperty = new Document("type", "string");
	Document gradePointAverageProperty = new Document("bsonType", "double");
	Document yearProperty = new Document("bsonType", "int").append("minimum", 2017).append("maximum", 3017)
			.append("exclusiveMaximum", true);

	Document properties = new Document("name", nameProperty) //
			.append("gradePointAverage", gradePointAverageProperty) //
			.append("year", yearProperty);

	Document mappedProperties = new Document("name", new Document(nameProperty)) //
			.append("gpa", new Document(gradePointAverageProperty)) //
			.append("year", new Document(yearProperty));

	List<String> requiredProperties = Arrays.asList("name", "gradePointAverage");
	List<String> mappedRequiredProperties = Arrays.asList("name", "gpa");

	Document $jsonSchema = new Document("type", "object") //
			.append("required", requiredProperties) //
			.append("properties", properties);

	Document mapped$jsonSchema = new Document("type", "object") //
			.append("required", mappedRequiredProperties) //
			.append("properties", mappedProperties);

	Document sourceSchemaDocument = new Document("$jsonSchema", $jsonSchema);
	Document mappedSchemaDocument = new Document("$jsonSchema", mapped$jsonSchema);

	String complexSchemaJsonString = "{ $jsonSchema: {" + //
			"         type: \"object\"," + //
			"         required: [ \"name\", \"year\", \"major\", \"gpa\" ]," + //
			"         properties: {" + //
			"            name: {" + //
			"               type: \"string\"," + //
			"               description: \"must be a string and is required\"" + //
			"            }," + //
			"            gender: {" + //
			"               type: \"string\"," + //
			"               description: \"must be a string and is not required\"" + //
			"            }," + //
			"            year: {" + //
			"               bsonType: \"int\"," + //
			"               minimum: 2017," + //
			"               maximum: 3017," + //
			"               exclusiveMaximum: true," + //
			"               description: \"must be an integer in [ 2017, 3017 ] and is required\"" + //
			"            }," + //
			"            major: {" + //
			"               type: \"string\"," + //
			"               enum: [ \"Math\", \"English\", \"Computer Science\", \"History\", null ]," + //
			"               description: \"can only be one of the enum values and is required\"" + //
			"            }," + //
			"            gpa: {" + //
			"               bsonType: \"double\"," + //
			"               description: \"must be a double and is required\"" + //
			"            }" + //
			"         }" + //
			"      } }";

	@Before
	public void setUp() {
		mapper = new MongoJsonSchemaMapper(new MappingMongoConverter(mock(DbRefResolver.class), new MongoMappingContext()));
	}

	@Test // DATAMONGO-1835
	public void noNullSchemaAllowed() {

		exception.expect(IllegalArgumentException.class);

		mapper.mapSchema(null, Object.class);
	}

	@Test // DATAMONGO-1835
	public void noNullDomainTypeAllowed() {

		exception.expect(IllegalArgumentException.class);

		mapper.mapSchema(new Document("$jsonSchema", new Document()), null);
	}

	@Test // DATAMONGO-1835
	public void schemaDocumentMustContain$jsonSchemaField() {

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("contain $jsonSchema");

		mapper.mapSchema(new Document("foo", new Document()), Object.class);
	}

	@Test // DATAMONGO-1835
	public void objectTypeSkipsFieldMapping() {
		assertThat(mapper.mapSchema(sourceSchemaDocument, Object.class)).isEqualTo(sourceSchemaDocument);
	}

	@Test // DATAMONGO-1835
	public void mapSchemaProducesNewDocument() {
		assertThat(mapper.mapSchema(sourceSchemaDocument, Object.class)).isNotSameAs(sourceSchemaDocument);
	}

	@Test // DATAMONGO-1835
	public void mapSchemaMapsPropertiesToFieldNames() {
		assertThat(mapper.mapSchema(sourceSchemaDocument, Student.class)).isEqualTo(mappedSchemaDocument);
	}

	@Test // DATAMONGO-1835
	public void mapSchemaLeavesSourceDocumentUntouched() {

		Document source = Document.parse(complexSchemaJsonString);
		mapper.mapSchema(source, Student.class);

		assertThat(source).isEqualTo(Document.parse(complexSchemaJsonString));
	}

	@Test // DATAMONGO-1835
	public void mapsNestedPropertiesCorrectly() {

		Document schema = new Document("$jsonSchema", new Document("type", "object") //
				.append("properties", new Document(properties).append("address", addressProperty)));

		Document expectedSchema = new Document("$jsonSchema", new Document("type", "object") //
				.append("properties", new Document(mappedProperties).append("address", mappedAddressProperty)));

		assertThat(mapper.mapSchema(schema, Student.class)).isEqualTo(expectedSchema);
	}

	@Test // DATAMONGO-1835
	public void constructReferenceSchemaCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder() //
				.required("name", "year", "major", "gradePointAverage") //
				.properties(string("name").description("must be a string and is required"), //
						string("gender").description("must be a string and is not required"), //
						int32("year").description("must be an integer in [ 2017, 3017 ] and is required").gte(2017).lt(3017), //
						string("major").description("can only be one of the enum values and is required").possibleValues("Math",
								"English", "Computer Science", "History", null), //
						float64("gradePointAverage").description("must be a double and is required") //
				).build();

		assertThat(mapper.mapSchema(schema.toDocument(), Student.class)).isEqualTo(Document.parse(complexSchemaJsonString));
	}

	// TODO: make sure to run enum fields through the converter so that values get mapped correctly !!!

	static class Student {

		String name;
		Gender gender;
		Integer year;
		String major;

		@Field("gpa") //
		Double gradePointAverage;
		Address address;
	}

	static class Address {

		String city;
		String street;

		@Field("post_code") //
		String postCode;
	}

	static enum Gender {
		M, F
	}

}
