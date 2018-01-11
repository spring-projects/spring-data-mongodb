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

import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type;

/**
 * Unit tests for {@link JsonSchemaProperty}.
 *
 * @author Mark Paluch
 */
public class JsonSchemaPropertyUnitTests {

	@Test // DATAMONGO-1835
	public void shouldRenderInt32Correctly() {
		assertThat(JsonSchemaProperty.int32("foo").toDocument()).containsEntry("foo.bsonType", "int");
	}

	@Test // DATAMONGO-1835
	public void shouldRenderInt64Correctly() {
		assertThat(JsonSchemaProperty.int64("foo").toDocument()).containsEntry("foo.bsonType", "long");
	}

	@Test // DATAMONGO-1835
	public void shouldRenderDecimal128Correctly() {
		assertThat(JsonSchemaProperty.decimal128("foo").toDocument()).containsEntry("foo.bsonType", "decimal");
	}

	@Test // DATAMONGO-1835
	public void shouldRenderNullCorrectly() {
		assertThat(JsonSchemaProperty.nil("foo").toDocument()).containsEntry("foo.type", "null");
	}

	@Test // DATAMONGO-1835
	public void shouldRenderUntypedCorrectly() {
		assertThat(JsonSchemaProperty.named("foo").ofType(Type.binaryType()).toDocument()).containsEntry("foo.bsonType",
				"binData");
	}
}
