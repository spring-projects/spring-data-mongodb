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

import java.util.Set;

import org.bson.Document;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.ObjectJsonSchemaObject;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @since 2017/12
 */
public class MongoJsonSchema {

	private final JsonSchemaObject root;

	private MongoJsonSchema(JsonSchemaObject root) {

		Assert.notNull(root, "Root must not be null!");
		this.root = root;
	}

	public Document toDocument() {  // TODO: consider rename to getSchemaObject() aligning to Query#getQueryObject()
		return new Document("$jsonSchema", root.toDocument());
	}

	public static MongoJsonSchema of(JsonSchemaObject type) {
		return new MongoJsonSchema(type);
	}

	public static MongoJsonSchemaBuilder builder() {
		return new MongoJsonSchemaBuilder();
	}

	public static class MongoJsonSchemaBuilder {

		private ObjectJsonSchemaObject root;

		MongoJsonSchemaBuilder() {
			root = new ObjectJsonSchemaObject();
		}

		public MongoJsonSchemaBuilder minNrProperties(int nrProperties) {
			root = root.minNrProperties(nrProperties);
			return this;
		}

		public MongoJsonSchemaBuilder maxNrProperties(int nrProperties) {
			root = root.maxNrProperties(nrProperties);
			return this;
		}

		public MongoJsonSchemaBuilder required(String... properties) {
			root = root.required(properties);
			return this;
		}

		public MongoJsonSchemaBuilder additionalProperties(boolean additionalPropertiesAllowed) {
			root = root.additionalProperties(additionalPropertiesAllowed);
			return this;
		}

		public MongoJsonSchemaBuilder additionalProperties(ObjectJsonSchemaObject schema) {
			root = root.additionalProperties(schema);
			return this;
		}

		public MongoJsonSchemaBuilder properties(JsonSchemaProperty... properties) {
			root = root.properties(properties);
			return this;
		}

		public MongoJsonSchemaBuilder patternProperties(JsonSchemaProperty... properties) {
			root = root.patternProperties(properties);
			return this;
		}

		public MongoJsonSchemaBuilder property(JsonSchemaProperty property) {
			root = root.property(property);
			return this;
		}

		public MongoJsonSchemaBuilder possibleValues(Set<Object> possibleValues) {
			root = root.possibleValues(possibleValues);
			return this;
		}

		public MongoJsonSchemaBuilder allOf(Set<JsonSchemaObject> allOf) {
			root = root.allOf(allOf);
			return this;
		}

		public MongoJsonSchemaBuilder anyOf(Set<JsonSchemaObject> anyOf) {
			root = root.anyOf(anyOf);
			return this;
		}

		public MongoJsonSchemaBuilder oneOf(Set<JsonSchemaObject> oneOf) {
			root = root.oneOf(oneOf);
			return this;
		}

		public MongoJsonSchemaBuilder notMatch(JsonSchemaObject notMatch) {
			root = root.notMatch(notMatch);
			return this;
		}

		public MongoJsonSchema build() {
			return MongoJsonSchema.of(root);
		}
	}

}
