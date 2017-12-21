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
package org.springframework.data.mongodb.core.schema;

import static org.springframework.data.domain.Range.from;
import static org.springframework.data.domain.Range.Bound.*;
import static org.springframework.data.mongodb.core.schema.JsonSchemaObject.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.Test;

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

		assertThat(object().toDocument())
				.isEqualTo(new Document("type", "object").append("description", "Must be an object."));
	}

	@Test // DATAMONGO-1835
	public void objectObjectShouldRenderNrPropertiesCorrectly() {

		assertThat(object().nrProperties(from(inclusive(10)).to(inclusive(20))).toDocument())
				.isEqualTo(new Document("type", "object").append("description", "Must be an object with [10-20] properties.")
						.append("minProperties", 10).append("maxProperties", 20));
	}

	@Test // DATAMONGO-1835
	public void objectObjectShouldRenderRequiredPropertiesCorrectly() {

		assertThat(object().required("spring", "data", "mongodb").toDocument()).isEqualTo(new Document("type", "object")
				.append("description", "Must be an object where spring, data, mongodb are mandatory.")
				.append("required", Arrays.asList("spring", "data", "mongodb")));
	}

	@Test // DATAMONGO-1835
	public void objectObjectShouldRenderAdditionalPropertiesCorrectlyWhenBoolean() {

		assertThat(object().additionalProperties(true).toDocument()).isEqualTo(
				new Document("type", "object").append("description", "Must be an object allowing additional properties.")
						.append("additionalProperties", true));

		assertThat(object().additionalProperties(false).toDocument()).isEqualTo(
				new Document("type", "object").append("description", "Must be an object not allowing additional properties.")
						.append("additionalProperties", false));
	}

	// TODO: go on here with properties and pattern properties

	// -----------------
	// type : 'string'
	// -----------------

	@Test // DATAMONGO-1835
	public void stringObjectShouldRenderTypeCorrectly() {

		assertThat(string().toDocument())
				.isEqualTo(new Document("type", "string").append("description", "Must be a string."));
	}

	@Test // DATAMONGO-1835
	public void stringObjectShouldRenderDescriptionCorrectly() {

		assertThat(string().description("error msg").toDocument())
				.isEqualTo(new Document("type", "string").append("description", "error msg"));
	}

	@Test // DATAMONGO-1835
	public void stringObjectShouldRenderRangeCorrectly() {

		assertThat(string().length(from(inclusive(10)).to(inclusive(20))).toDocument())
				.isEqualTo(new Document("type", "string").append("description", "Must be a string with length [10-20].")
						.append("minLength", 10).append("maxLength", 20));
	}

	@Test // DATAMONGO-1835
	public void stringObjectShouldRenderPatternCorrectly() {

		assertThat(string().matching("^spring$").toDocument()).isEqualTo(new Document("type", "string")
				.append("description", "Must be a string matching ^spring$.").append("pattern", "^spring$"));
	}

}
