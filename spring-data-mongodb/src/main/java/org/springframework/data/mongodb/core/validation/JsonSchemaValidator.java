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
package org.springframework.data.mongodb.core.validation;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.util.Assert;

/**
 * {@link Validator} implementation based on {@link MongoJsonSchema JSON Schema}.
 *
 * @author Christoph Strobl
 * @since 2.1
 * @see <a href="https://docs.mongodb.com/manual/core/schema-validation/#json-schema">Schema Validation</a>
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
public class JsonSchemaValidator implements Validator {

	private final MongoJsonSchema schema;

	public static JsonSchemaValidator of(MongoJsonSchema schema) {

		Assert.notNull(schema, "Schema must not be null!");
		return new JsonSchemaValidator(schema);
	}

	public MongoJsonSchema getSchema() {
		return schema;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.validation.Validator#toDocument()
	 */
	@Override
	public Document toDocument() {
		return schema.toDocument();
	}

	/*
	 * (non-Javadoc) 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return SerializationUtils.serializeToJsonSafely(toDocument());
	}
}
