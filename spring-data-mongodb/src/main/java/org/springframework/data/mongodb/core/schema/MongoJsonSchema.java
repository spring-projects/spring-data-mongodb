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

import java.util.Collection;
import java.util.Set;

import org.bson.Document;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.ObjectJsonSchemaObject;
import org.springframework.util.Assert;

/**
 * Interface defining MongoDB-specific JSON schema object. New objects can be built with {@link #builder()}, for
 * example:
 *
 * <pre class="code">
 * MongoJsonSchema schema = MongoJsonSchema.builder().required("firstname", "lastname")
 * 		.properties(string("firstname").possibleValues("luke", "han"),
 * 				object("address").properties(string("postCode").minLength(4).maxLength(5))
 *
 * 		).build();
 * </pre>
 *
 * resulting in the following schema:
 *
 * <pre>
 *  {
  "type": "object",
  "required": [ "firstname", "lastname" ],
  "properties": {
    "firstname": {
      "type": "string", "enum": [ "luke", "han" ],
    },
    "address": {
      "type": "object",
      "properties": {
        "postCode": { "type": "string", "minLength": 4, "maxLength": 5 }
      }
    }
  }
}
 * </pre>
 *
 * @author Christoph Strobl
 * @since 2.1
 **/
public class MongoJsonSchema {

	private final JsonSchemaObject root;

	private MongoJsonSchema(JsonSchemaObject root) {

		Assert.notNull(root, "Root must not be null!");
		this.root = root;
	}

	/**
	 * Create the {@link Document} containing the specified {@code $jsonSchema}. <br />
	 * Property and field names still need to be mapped to the domain type ones by running the {@link Document} through a
	 * {@link org.springframework.data.mongodb.core.convert.JsonSchemaMapper}.
	 *
	 * @return never {@literal null}.
	 */
	public Document toDocument() {
		return new Document("$jsonSchema", root.toDocument());
	}

	/**
	 * Create a new {@link MongoJsonSchema} for a given root object.
	 *
	 * @param root must not be {@literal null}.
	 * @return
	 */
	public static MongoJsonSchema of(JsonSchemaObject root) {
		return new MongoJsonSchema(root);
	}

	/**
	 * Obtain a new {@link MongoJsonSchemaBuilder} to fluently define the schema.
	 *
	 * @return new instance of {@link MongoJsonSchemaBuilder}.
	 */
	public static MongoJsonSchemaBuilder builder() {
		return new MongoJsonSchemaBuilder();
	}

	/**
	 * {@link MongoJsonSchemaBuilder} provides a fluent API for defining a {@link MongoJsonSchema}.
	 *
	 * @since 2.1
	 */
	public static class MongoJsonSchemaBuilder {

		private ObjectJsonSchemaObject root;

		MongoJsonSchemaBuilder() {
			root = new ObjectJsonSchemaObject();
		}

		/**
		 * @param nrProperties
		 * @return this
		 * @see ObjectJsonSchemaObject#minNrProperties(int)
		 */
		public MongoJsonSchemaBuilder minNrProperties(int nrProperties) {
			root = root.minNrProperties(nrProperties);
			return this;
		}

		/**
		 * @param nrProperties
		 * @return this
		 * @see ObjectJsonSchemaObject#maxNrProperties(int)
		 */
		public MongoJsonSchemaBuilder maxNrProperties(int nrProperties) {
			root = root.maxNrProperties(nrProperties);
			return this;
		}

		/**
		 * @param properties
		 * @return
		 * @see ObjectJsonSchemaObject#required(String...)
		 */
		public MongoJsonSchemaBuilder required(String... properties) {
			root = root.required(properties);
			return this;
		}

		/**
		 * @param additionalPropertiesAllowed
		 * @return this
		 * @see ObjectJsonSchemaObject#additionalProperties(boolean)
		 */
		public MongoJsonSchemaBuilder additionalProperties(boolean additionalPropertiesAllowed) {
			root = root.additionalProperties(additionalPropertiesAllowed);
			return this;
		}

		/**
		 * @param schema
		 * @return this
		 * @see ObjectJsonSchemaObject#additionalProperties(ObjectJsonSchemaObject)
		 */
		public MongoJsonSchemaBuilder additionalProperties(ObjectJsonSchemaObject schema) {
			root = root.additionalProperties(schema);
			return this;
		}

		/**
		 * @param properties
		 * @return this
		 * @see ObjectJsonSchemaObject#properties(JsonSchemaProperty...)
		 */
		public MongoJsonSchemaBuilder properties(JsonSchemaProperty... properties) {
			root = root.properties(properties);
			return this;
		}

		/**
		 * @param properties
		 * @return this
		 * @see ObjectJsonSchemaObject#patternProperties(JsonSchemaProperty...)
		 */
		public MongoJsonSchemaBuilder patternProperties(JsonSchemaProperty... properties) {
			root = root.patternProperties(properties);
			return this;
		}

		/**
		 * @param property
		 * @return this
		 * @see ObjectJsonSchemaObject#property(JsonSchemaProperty)
		 */
		public MongoJsonSchemaBuilder property(JsonSchemaProperty property) {
			root = root.property(property);
			return this;
		}

		/**
		 * @param possibleValues
		 * @return this
		 * @see ObjectJsonSchemaObject#possibleValues(Collection)
		 */
		public MongoJsonSchemaBuilder possibleValues(Set<Object> possibleValues) {
			root = root.possibleValues(possibleValues);
			return this;
		}

		/**
		 * @param allOf
		 * @return this
		 * @see UntypedJsonSchemaObject#allOf(Collection)
		 */
		public MongoJsonSchemaBuilder allOf(Set<JsonSchemaObject> allOf) {
			root = root.allOf(allOf);
			return this;
		}

		/**
		 * @param anyOf
		 * @return this
		 * @see UntypedJsonSchemaObject#anyOf(Collection)
		 */
		public MongoJsonSchemaBuilder anyOf(Set<JsonSchemaObject> anyOf) {
			root = root.anyOf(anyOf);
			return this;
		}

		/**
		 * @param oneOf
		 * @return this
		 * @see UntypedJsonSchemaObject#oneOf(Collection)
		 */
		public MongoJsonSchemaBuilder oneOf(Set<JsonSchemaObject> oneOf) {
			root = root.oneOf(oneOf);
			return this;
		}

		/**
		 * @param notMatch
		 * @return this
		 * @see UntypedJsonSchemaObject#notMatch(JsonSchemaObject)
		 */
		public MongoJsonSchemaBuilder notMatch(JsonSchemaObject notMatch) {
			root = root.notMatch(notMatch);
			return this;
		}

		/**
		 * @param description
		 * @return this
		 * @see UntypedJsonSchemaObject#description(String)
		 */
		public MongoJsonSchemaBuilder description(String description) {
			root = root.description(description);
			return this;
		}

		/**
		 * Obtain the {@link MongoJsonSchema}.
		 *
		 * @return new instance of {@link MongoJsonSchema}.
		 */
		public MongoJsonSchema build() {
			return MongoJsonSchema.of(root);
		}
	}

}
