/*
 * Copyright 2018-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.schema;

import static org.springframework.data.domain.Range.from;
import static org.springframework.data.domain.Range.Bound.*;
import static org.springframework.data.mongodb.core.schema.JsonSchemaObject.*;
import static org.springframework.data.mongodb.core.schema.JsonSchemaObject.array;
import static org.springframework.data.mongodb.core.schema.JsonSchemaObject.of;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.*;

/**
 * Tests verifying {@link org.bson.Document} representation of {@link JsonSchemaObject}s.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Michał Kurcius
 */
class JsonSchemaObjectUnitTests {

	// -----------------
	// type from class
	// -----------------

	@Test // DATAMONGO-1849
	void primitiveType() {

		assertThat(JsonSchemaObject.of(boolean.class).getTypes()).containsExactly(Type.booleanType());
		assertThat(JsonSchemaObject.of(int.class).getTypes()).containsExactly(Type.intType());
		assertThat(JsonSchemaObject.of(long.class).getTypes()).containsExactly(Type.longType());
		assertThat(JsonSchemaObject.of(float.class).getTypes()).containsExactly(Type.doubleType());
		assertThat(JsonSchemaObject.of(double.class).getTypes()).containsExactly(Type.doubleType());
		assertThat(JsonSchemaObject.of(short.class).getTypes()).containsExactly(Type.numberType());
	}

	@Test // DATAMONGO-1849
	void objectType() {

		assertThat(JsonSchemaObject.of(Object.class).getTypes()).containsExactly(Type.objectType());
		assertThat(JsonSchemaObject.of(Map.class).getTypes()).containsExactly(Type.objectType());
		assertThat(JsonSchemaObject.of(Document.class).getTypes()).containsExactly(Type.objectType());
	}

	@Test // DATAMONGO-1849
	void binaryData() {
		assertThat(JsonSchemaObject.of(byte[].class).getTypes()).containsExactly(Type.binaryType());
	}

	@Test // DATAMONGO-1849
	void collectionType() {

		assertThat(JsonSchemaObject.of(Object[].class).getTypes()).containsExactly(Type.arrayType());
		assertThat(JsonSchemaObject.of(Collection.class).getTypes()).containsExactly(Type.arrayType());
		assertThat(JsonSchemaObject.of(List.class).getTypes()).containsExactly(Type.arrayType());
		assertThat(JsonSchemaObject.of(Set.class).getTypes()).containsExactly(Type.arrayType());
	}

	@Test // DATAMONGO-1849
	void dateType() {
		assertThat(JsonSchemaObject.of(Date.class).getTypes()).containsExactly(Type.dateType());
	}

	// -----------------
	// type : 'object'
	// -----------------

	@Test // DATAMONGO-1835
	void objectObjectShouldRenderTypeCorrectly() {

		assertThat(object().generatedDescription().toDocument())
				.isEqualTo(new Document("type", "object").append("description", "Must be an object."));
	}

	@Test // DATAMONGO-1835
	void objectObjectShouldRenderNrPropertiesCorrectly() {

		assertThat(object().propertiesCount(from(inclusive(10)).to(inclusive(20))).generatedDescription().toDocument())
				.isEqualTo(new Document("type", "object").append("description", "Must be an object with [10-20] properties.")
						.append("minProperties", 10).append("maxProperties", 20));
	}

	@Test // DATAMONGO-1835
	void objectObjectShouldRenderRequiredPropertiesCorrectly() {

		assertThat(object().required("spring", "data", "mongodb").generatedDescription().toDocument())
				.isEqualTo(new Document("type", "object")
						.append("description", "Must be an object where spring, data, mongodb are mandatory.")
						.append("required", Arrays.asList("spring", "data", "mongodb")));
	}

	@Test // DATAMONGO-1835
	void objectObjectShouldRenderAdditionalPropertiesCorrectlyWhenBoolean() {

		assertThat(object().additionalProperties(true).generatedDescription().toDocument()).isEqualTo(
				new Document("type", "object").append("description", "Must be an object allowing additional properties.")
						.append("additionalProperties", true));

		assertThat(object().additionalProperties(false).generatedDescription().toDocument()).isEqualTo(
				new Document("type", "object").append("description", "Must be an object not allowing additional properties.")
						.append("additionalProperties", false));
	}

	@Test // DATAMONGO-1835
	void objectObjectShouldRenderPropertiesCorrectly() {

		Document expected = new Document("type", "object")
				.append("description", "Must be an object defining restrictions for name, active.").append("properties",
						new Document("name", new Document("type", "string")
								.append("description", "Must be a string with length unbounded-10].").append("maxLength", 10))
										.append("active", new Document("type", "boolean")));

		assertThat(object().generatedDescription()
				.properties(JsonSchemaProperty.string("name").maxLength(10).generatedDescription(),
						JsonSchemaProperty.bool("active"))
				.generatedDescription().toDocument()).isEqualTo(expected);
	}

	@Test // DATAMONGO-1835
	void objectObjectShouldRenderNestedObjectPropertiesCorrectly() {

		Document expected = new Document("type", "object")
				.append("description", "Must be an object defining restrictions for address.")
				.append("properties", new Document("address",
						new Document("type", "object").append("description", "Must be an object defining restrictions for city.")
								.append("properties", new Document("city", new Document("type", "string")
										.append("description", "Must be a string with length [3-unbounded.").append("minLength", 3)))));

		assertThat(object()
				.properties(JsonSchemaProperty.object("address")
						.properties(JsonSchemaProperty.string("city").minLength(3).generatedDescription()).generatedDescription())
				.generatedDescription().toDocument()).isEqualTo(expected);
	}

	@Test // DATAMONGO-1835
	void objectObjectShouldRenderPatternPropertiesCorrectly() {

		Document expected = new Document("type", "object")
				.append("description", "Must be an object defining restrictions for patterns na.*.")
				.append("patternProperties", new Document("na.*", new Document("type", "string")
						.append("description", "Must be a string with length unbounded-10].").append("maxLength", 10)));

		assertThat(object().patternProperties(JsonSchemaProperty.string("na.*").maxLength(10).generatedDescription())
				.generatedDescription().toDocument()).isEqualTo(expected);
	}

	@Test // DATAMONGO-1849
	void objectShouldIncludeRequiredNestedCorrectly() {

		assertThat(object() //
				.properties( //
						JsonSchemaProperty.required(JsonSchemaProperty.string("lastname")) //
				).toDocument())
						.isEqualTo(new Document("type", "object").append("required", Collections.singletonList("lastname"))
								.append("properties", new Document("lastname", new Document("type", "string"))));
	}

	// -----------------
	// type : 'string'
	// -----------------

	@Test // DATAMONGO-1835
	void stringObjectShouldRenderTypeCorrectly() {

		assertThat(string().generatedDescription().toDocument())
				.isEqualTo(new Document("type", "string").append("description", "Must be a string."));
	}

	@Test // DATAMONGO-1835
	void stringObjectShouldRenderDescriptionCorrectly() {

		assertThat(string().description("error msg").toDocument())
				.isEqualTo(new Document("type", "string").append("description", "error msg"));
	}

	@Test // DATAMONGO-1835
	void stringObjectShouldRenderRangeCorrectly() {

		assertThat(string().length(from(inclusive(10)).to(inclusive(20))).generatedDescription().toDocument())
				.isEqualTo(new Document("type", "string").append("description", "Must be a string with length [10-20].")
						.append("minLength", 10).append("maxLength", 20));
	}

	@Test // DATAMONGO-1835
	void stringObjectShouldRenderPatternCorrectly() {

		assertThat(string().matching("^spring$").generatedDescription().toDocument())
				.isEqualTo(new Document("type", "string").append("description", "Must be a string matching ^spring$.")
						.append("pattern", "^spring$"));
	}

	// -----------------
	// type : 'number'
	// -----------------

	@Test // DATAMONGO-1835
	void numberObjectShouldRenderMultipleOfCorrectly() {

		assertThat(number().multipleOf(3.141592F).generatedDescription().toDocument())
				.isEqualTo(new Document("type", "number").append("description", "Must be a numeric value multiple of 3.141592.")
						.append("multipleOf", 3.141592F));
	}

	@Test // DATAMONGO-1835
	void numberObjectShouldRenderMaximumCorrectly() {

		assertThat(
				number().within(Range.of(Bound.unbounded(), Bound.inclusive(3.141592F))).generatedDescription().toDocument())
						.isEqualTo(new Document("type", "number")
								.append("description", "Must be a numeric value within range unbounded-3.141592].")
								.append("maximum", 3.141592F));

		assertThat(
				number().within(Range.of(Bound.unbounded(), Bound.exclusive(3.141592F))).generatedDescription().toDocument())
						.isEqualTo(new Document("type", "number")
								.append("description", "Must be a numeric value within range unbounded-3.141592).")
								.append("maximum", 3.141592F).append("exclusiveMaximum", true));
	}

	@Test // DATAMONGO-1835
	void numberObjectShouldRenderMinimumCorrectly() {

		assertThat(
				number().within(Range.of(Bound.inclusive(3.141592F), Bound.unbounded())).generatedDescription().toDocument())
						.isEqualTo(new Document("type", "number")
								.append("description", "Must be a numeric value within range [3.141592-unbounded.")
								.append("minimum", 3.141592F));

		assertThat(
				number().within(Range.of(Bound.exclusive(3.141592F), Bound.unbounded())).generatedDescription().toDocument())
						.isEqualTo(new Document("type", "number")
								.append("description", "Must be a numeric value within range (3.141592-unbounded.")
								.append("minimum", 3.141592F).append("exclusiveMinimum", true));
	}

	// -----------------
	// type : 'arrays'
	// -----------------

	@Test // DATAMONGO-1835
	void arrayObjectShouldRenderItemsCorrectly() {

		assertThat(array().items(Arrays.asList(string(), bool())).toDocument()).isEqualTo(new Document("type", "array")
				.append("items", Arrays.asList(new Document("type", "string"), new Document("type", "boolean"))));
	}

	@Test // DATAMONGO-2613
	void arrayObjectShouldRenderItemsCorrectlyAsObjectIfContainsOnlyOneElement() {

		assertThat(array().items(Collections.singletonList(string())).toDocument())
				.isEqualTo(new Document("type", "array").append("items", new Document("type", "string")));
	}

	@Test // DATAMONGO-1835
	void arrayObjectShouldRenderMaxItemsCorrectly() {

		assertThat(array().maxItems(5).generatedDescription().toDocument()).isEqualTo(new Document("type", "array")
				.append("description", "Must be an array having size unbounded-5].").append("maxItems", 5));
	}

	@Test // DATAMONGO-1835
	void arrayObjectShouldRenderMinItemsCorrectly() {

		assertThat(array().minItems(5).generatedDescription().toDocument()).isEqualTo(new Document("type", "array")
				.append("description", "Must be an array having size [5-unbounded.").append("minItems", 5));
	}

	@Test // DATAMONGO-1835
	void arrayObjectShouldRenderUniqueItemsCorrectly() {

		assertThat(array().uniqueItems(true).generatedDescription().toDocument()).isEqualTo(new Document("type", "array")
				.append("description", "Must be an array of unique values.").append("uniqueItems", true));
	}

	@Test // DATAMONGO-1835
	void arrayObjectShouldRenderAdditionalItemsItemsCorrectly() {

		assertThat(array().additionalItems(true).generatedDescription().toDocument())
				.isEqualTo(new Document("type", "array").append("description", "Must be an array with additional items.")
						.append("additionalItems", true));
		assertThat(array().additionalItems(false).generatedDescription().toDocument())
				.isEqualTo(new Document("type", "array").append("description", "Must be an array with no additional items.")
						.append("additionalItems", false));
	}

	// -----------------
	// type : 'boolean'
	// -----------------

	@Test // DATAMONGO-1835
	void booleanShouldRenderCorrectly() {

		assertThat(bool().generatedDescription().toDocument())
				.isEqualTo(new Document("type", "boolean").append("description", "Must be a boolean"));
	}

	// -----------------
	// type : 'null'
	// -----------------

	@Test // DATAMONGO-1835
	void nullShouldRenderCorrectly() {

		assertThat(nil().generatedDescription().toDocument())
				.isEqualTo(new Document("type", "null").append("description", "Must be null"));
	}

	// -----------------
	// type : 'date'
	// -----------------

	@Test // DATAMONGO-1877
	void dateShouldRenderCorrectly() {

		assertThat(date().generatedDescription().toDocument())
				.isEqualTo(new Document("bsonType", "date").append("description", "Must be a date"));
	}

	// -----------------
	// type : 'timestamp'
	// -----------------

	@Test // DATAMONGO-1877
	void timestampShouldRenderCorrectly() {

		assertThat(timestamp().generatedDescription().toDocument())
				.isEqualTo(new Document("bsonType", "timestamp").append("description", "Must be a timestamp"));
	}

	// -----------------
	// type : 'any'
	// -----------------

	@Test // DATAMONGO-1835
	void typedObjectShouldRenderEnumCorrectly() {

		assertThat(of(String.class).possibleValues(Arrays.asList("one", "two")).toDocument())
				.isEqualTo(new Document("type", "string").append("enum", Arrays.asList("one", "two")));
	}

	@Test // DATAMONGO-1835
	void typedObjectShouldRenderAllOfCorrectly() {

		assertThat(of(Object.class).allOf(Arrays.asList(string())).toDocument())
				.isEqualTo(new Document("type", "object").append("allOf", Arrays.asList(new Document("type", "string"))));
	}

	@Test // DATAMONGO-1835
	void typedObjectShouldRenderAnyOfCorrectly() {

		assertThat(of(String.class).anyOf(Arrays.asList(string())).toDocument())
				.isEqualTo(new Document("type", "string").append("anyOf", Arrays.asList(new Document("type", "string"))));
	}

	@Test // DATAMONGO-1835
	void typedObjectShouldRenderOneOfCorrectly() {

		assertThat(of(String.class).oneOf(Arrays.asList(string())).toDocument())
				.isEqualTo(new Document("type", "string").append("oneOf", Arrays.asList(new Document("type", "string"))));
	}

	@Test // DATAMONGO-1835
	void typedObjectShouldRenderNotCorrectly() {

		assertThat(untyped().notMatch(string()).toDocument())
				.isEqualTo(new Document("not", new Document("type", "string")));
	}
}
