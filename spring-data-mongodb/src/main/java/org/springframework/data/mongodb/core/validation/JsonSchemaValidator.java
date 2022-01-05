/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.mongodb.core.validation;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * {@link Validator} implementation based on {@link MongoJsonSchema JSON Schema}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 * @see <a href="https://docs.mongodb.com/manual/core/schema-validation/#json-schema">Schema Validation</a>
 */
class JsonSchemaValidator implements Validator {

	private final MongoJsonSchema schema;

	private JsonSchemaValidator(MongoJsonSchema schema) {
		this.schema = schema;
	}

	/**
	 * Create new {@link JsonSchemaValidator} defining validation rules via {@link MongoJsonSchema}.
	 *
	 * @param schema must not be {@literal null}.
	 * @throws IllegalArgumentException if schema is {@literal null}.
	 */
	static JsonSchemaValidator of(MongoJsonSchema schema) {

		Assert.notNull(schema, "Schema must not be null!");

		return new JsonSchemaValidator(schema);
	}

	@Override
	public Document toDocument() {
		return schema.toDocument();
	}

	@Override
	public String toString() {
		return SerializationUtils.serializeToJsonSafely(toDocument());
	}

	@Override
	public boolean equals(Object o) {

		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		JsonSchemaValidator that = (JsonSchemaValidator) o;

		return ObjectUtils.nullSafeEquals(schema, that.schema);
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(schema);
	}
}
