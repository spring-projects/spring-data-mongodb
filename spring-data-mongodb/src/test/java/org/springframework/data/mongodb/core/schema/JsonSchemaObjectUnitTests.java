/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.schema;

import static org.springframework.data.domain.Range.from;
import static org.springframework.data.domain.Range.Bound.*;
import static org.springframework.data.mongodb.core.schema.JsonSchemaObject.*;
import static org.springframework.data.mongodb.core.schema.JsonSchemaObject.of;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.*;

/**
 * Tests verifying {@link org.bson.Document} representation of {@link JsonSchemaObject}s.
 *
 * @author Christoph Strobl
 */
public class JsonSchemaObjectUnitTests {

	// -----------------
	// type : 'object'
	// -----------------

	@Test // DATAMONGO-1835
	public void objectObjectShouldRenderTypeCorrectly() {

		assertThat(object().generatedDescription().toDocument())
				.isEqualTo(new Document("type", "object").append("description", "Must be an object."));
	}

	@Test // DATAMONGO-1835
	public void objectObjectShouldRenderNrPropertiesCorrectly() {

		assertThat(object().nrProperties(from(inclusive(10)).to(inclusive(20))).generatedDescription().toDocument())
				.isEqualTo(new Document("type", "object").append("description", "Must be an object with [10-20] properties.")
						.append("minProperties", 10).append("maxProperties", 20));
	}

	@Test // DATAMONGO-1835
	public void objectObjectShouldRenderRequiredPropertiesCorrectly() {

		assertThat(object().required("spring", "data", "mongodb").generatedDescription().toDocument()).isEqualTo(
				new Document("type", "object")
						.append("description", "Must be an object where spring, data, mongodb are mandatory.").append("required",
								Arrays.asList("spring", "data", "mongodb")));
	}

	@Test // DATAMONGO-1835
	public void objectObjectShouldRenderAdditionalPropertiesCorrectlyWhenBoolean() {

		assertThat(object().additionalProperties(true).generatedDescription().toDocument()).isEqualTo(
				new Document("type", "object").append("description", "Must be an object allowing additional properties.")
						.append("additionalProperties", true));

		assertThat(object().additionalProperties(false).generatedDescription().toDocument()).isEqualTo(
				new Document("type", "object").append("description", "Must be an object not allowing additional properties.")
						.append("additionalProperties", false));
	}

	@Test // DATAMONGO-1835
	public void objectObjectShouldRenderPropertiesCorrectly() {

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
	public void objectObjectShouldRenderNestedObjectPropertiesCorrectly() {

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
	public void objectObjectShouldRenderPatternPropertiesCorrectly() {

		Document expected = new Document("type", "object")
				.append("description", "Must be an object defining restrictions for patterns na.*.")
				.append("patternProperties", new Document("na.*", new Document("type", "string")
						.append("description", "Must be a string with length unbounded-10].").append("maxLength", 10)));

		assertThat(object().patternProperties(JsonSchemaProperty.string("na.*").maxLength(10).generatedDescription())
				.generatedDescription().toDocument()).isEqualTo(expected);
	}

	// -----------------
	// type : 'string'
	// -----------------

	@Test // DATAMONGO-1835
	public void stringObjectShouldRenderTypeCorrectly() {

		assertThat(string().generatedDescription().toDocument())
				.isEqualTo(new Document("type", "string").append("description", "Must be a string."));
	}

	@Test // DATAMONGO-1835
	public void stringObjectShouldRenderDescriptionCorrectly() {

		assertThat(string().description("error msg").toDocument())
				.isEqualTo(new Document("type", "string").append("description", "error msg"));
	}

	@Test // DATAMONGO-1835
	public void stringObjectShouldRenderRangeCorrectly() {

		assertThat(string().length(from(inclusive(10)).to(inclusive(20))).generatedDescription().toDocument())
				.isEqualTo(new Document("type", "string").append("description", "Must be a string with length [10-20].")
						.append("minLength", 10).append("maxLength", 20));
	}

	@Test // DATAMONGO-1835
	public void stringObjectShouldRenderPatternCorrectly() {

		assertThat(string().matching("^spring$").generatedDescription().toDocument())
				.isEqualTo(new Document("type", "string").append("description", "Must be a string matching ^spring$.")
						.append("pattern", "^spring$"));
	}

	// -----------------
	// type : 'number'
	// -----------------

	@Test // DATAMONGO-1835
	public void numberObjectShouldRenderMultipleOfCorrectly() {

		assertThat(number().multipleOf(3.141592F).generatedDescription().toDocument())
				.isEqualTo(new Document("type", "number").append("description", "Must be a numeric value multiple of 3.141592.")
						.append("multipleOf", 3.141592F));
	}

	@Test // DATAMONGO-1835
	public void numberObjectShouldRenderMaximumCorrectly() {

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
	public void numberObjectShouldRenderMinimumCorrectly() {

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
	public void arrayObjectShouldRenderItemsCorrectly() {

		assertThat(array().items(Arrays.asList(string(), bool())).toDocument()).isEqualTo(new Document("type", "array")
				.append("items", Arrays.asList(new Document("type", "string"), new Document("type", "boolean"))));
	}

	@Test // DATAMONGO-1835
	public void arrayObjectShouldRenderMaxItemsCorrectly() {

		assertThat(array().maxItems(5).generatedDescription().toDocument()).isEqualTo(new Document("type", "array")
				.append("description", "Must be an array having size unbounded-5].").append("maxItems", 5));
	}

	@Test // DATAMONGO-1835
	public void arrayObjectShouldRenderMinItemsCorrectly() {

		assertThat(array().minItems(5).generatedDescription().toDocument()).isEqualTo(new Document("type", "array")
				.append("description", "Must be an array having size [5-unbounded.").append("minItems", 5));
	}

	@Test // DATAMONGO-1835
	public void arrayObjectShouldRenderUniqueItemsCorrectly() {

		assertThat(array().uniqueItems(true).generatedDescription().toDocument()).isEqualTo(new Document("type", "array")
				.append("description", "Must be an array of unique values.").append("uniqueItems", true));
	}

	// -----------------
	// type : 'any'
	// -----------------

	@Test // DATAMONGO-1835
	public void typedObjectShouldRenderEnumCorrectly() {

		assertThat(of(String.class).possibleValues(Arrays.asList("one", "two")).toDocument())
				.isEqualTo(new Document("type", "string").append("enum", Arrays.asList("one", "two")));
	}

	@Test // DATAMONGO-1835
	public void typedObjectShouldRenderAllOfCorrectly() {

		assertThat(of(Object.class).allOf(Arrays.asList(string())).toDocument())
				.isEqualTo(new Document("type", "object").append("allOf", Arrays.asList(new Document("type", "string"))));
	}

	@Test // DATAMONGO-1835
	public void typedObjectShouldRenderAnyOfCorrectly() {

		assertThat(of(String.class).anyOf(Arrays.asList(string())).toDocument())
				.isEqualTo(new Document("type", "string").append("anyOf", Arrays.asList(new Document("type", "string"))));
	}

	@Test // DATAMONGO-1835
	public void typedObjectShouldRenderOneOfCorrectly() {

		assertThat(of(String.class).oneOf(Arrays.asList(string())).toDocument())
				.isEqualTo(new Document("type", "string").append("oneOf", Arrays.asList(new Document("type", "string"))));
	}

	@Test // DATAMONGO-1835
	public void typedObjectShouldRenderNotCorrectly() {

		assertThat(untyped().notMatch(string()).toDocument())
				.isEqualTo(new Document("not", new Document("type", "string")));
	}
}
