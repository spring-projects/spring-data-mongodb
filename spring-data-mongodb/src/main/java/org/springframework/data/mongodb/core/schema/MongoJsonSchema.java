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
package org.springframework.data.mongodb.core.schema;

import java.util.Collection;
import java.util.Set;

import org.bson.Document;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.ObjectJsonSchemaObject;
import org.springframework.lang.Nullable;

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
 * @author Mark Paluch
 * @since 2.1
 * @see UntypedJsonSchemaObject
 * @see TypedJsonSchemaObject
 */
public interface MongoJsonSchema {

	/**
	 * Create the {@code $jsonSchema} {@link Document} containing the specified {@link #schemaDocument()}. <br />
	 * Property and field names need to be mapped to the domain type ones by running the {@link Document} through a
	 * {@link org.springframework.data.mongodb.core.convert.JsonSchemaMapper} to apply field name customization.
	 *
	 * @return never {@literal null}.
	 */
	default Document toDocument() {
		return new Document("$jsonSchema", schemaDocument());
	}

	/**
	 * Create the {@link Document} defining the schema. <br />
	 * Property and field names need to be mapped to the domain type ones by running the {@link Document} through a
	 * {@link org.springframework.data.mongodb.core.convert.JsonSchemaMapper} to apply field name customization.
	 *
	 * @return never {@literal null}.
	 * @since 3.3
	 */
	Document schemaDocument();

	/**
	 * Create a new {@link MongoJsonSchema} for a given root object.
	 *
	 * @param root must not be {@literal null}.
	 * @return new instance of {@link MongoJsonSchema}.
	 */
	static MongoJsonSchema of(JsonSchemaObject root) {
		return new DefaultMongoJsonSchema(root);
	}

	/**
	 * Create a new {@link MongoJsonSchema} for a given root {@link Document} containing the schema definition.
	 *
	 * @param document must not be {@literal null}.
	 * @return new instance of {@link MongoJsonSchema}.
	 */
	static MongoJsonSchema of(Document document) {
		return new DocumentJsonSchema(document);
	}

	/**
	 * Obtain a new {@link MongoJsonSchemaBuilder} to fluently define the schema.
	 *
	 * @return new instance of {@link MongoJsonSchemaBuilder}.
	 */
	static MongoJsonSchemaBuilder builder() {
		return new MongoJsonSchemaBuilder();
	}

	/**
	 * {@link MongoJsonSchemaBuilder} provides a fluent API for defining a {@link MongoJsonSchema}.
	 *
	 * @author Christoph Strobl
	 */
	class MongoJsonSchemaBuilder {

		private ObjectJsonSchemaObject root;

		@Nullable //
		private Document encryptionMetadata;

		MongoJsonSchemaBuilder() {
			root = new ObjectJsonSchemaObject();
		}

		/**
		 * @param count
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see ObjectJsonSchemaObject#minProperties(int)
		 */
		public MongoJsonSchemaBuilder minProperties(int count) {

			root = root.minProperties(count);
			return this;
		}

		/**
		 * @param count
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see ObjectJsonSchemaObject#maxProperties(int)
		 */
		public MongoJsonSchemaBuilder maxProperties(int count) {

			root = root.maxProperties(count);
			return this;
		}

		/**
		 * @param properties must not be {@literal null}.
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see ObjectJsonSchemaObject#required(String...)
		 */
		public MongoJsonSchemaBuilder required(String... properties) {

			root = root.required(properties);
			return this;
		}

		/**
		 * @param additionalPropertiesAllowed
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see ObjectJsonSchemaObject#additionalProperties(boolean)
		 */
		public MongoJsonSchemaBuilder additionalProperties(boolean additionalPropertiesAllowed) {

			root = root.additionalProperties(additionalPropertiesAllowed);
			return this;
		}

		/**
		 * @param schema must not be {@literal null}.
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see ObjectJsonSchemaObject#additionalProperties(ObjectJsonSchemaObject)
		 */
		public MongoJsonSchemaBuilder additionalProperties(ObjectJsonSchemaObject schema) {

			root = root.additionalProperties(schema);
			return this;
		}

		/**
		 * @param properties must not be {@literal null}.
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see ObjectJsonSchemaObject#properties(JsonSchemaProperty...)
		 */
		public MongoJsonSchemaBuilder properties(JsonSchemaProperty... properties) {

			root = root.properties(properties);
			return this;
		}

		/**
		 * @param properties must not be {@literal null}.
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see ObjectJsonSchemaObject#patternProperties(JsonSchemaProperty...)
		 */
		public MongoJsonSchemaBuilder patternProperties(JsonSchemaProperty... properties) {

			root = root.patternProperties(properties);
			return this;
		}

		/**
		 * @param property must not be {@literal null}.
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see ObjectJsonSchemaObject#property(JsonSchemaProperty)
		 */
		public MongoJsonSchemaBuilder property(JsonSchemaProperty property) {

			root = root.property(property);
			return this;
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see ObjectJsonSchemaObject#possibleValues(Collection)
		 */
		public MongoJsonSchemaBuilder possibleValues(Set<Object> possibleValues) {

			root = root.possibleValues(possibleValues);
			return this;
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see UntypedJsonSchemaObject#allOf(Collection)
		 */
		public MongoJsonSchemaBuilder allOf(Set<JsonSchemaObject> allOf) {

			root = root.allOf(allOf);
			return this;
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see UntypedJsonSchemaObject#anyOf(Collection)
		 */
		public MongoJsonSchemaBuilder anyOf(Set<JsonSchemaObject> anyOf) {

			root = root.anyOf(anyOf);
			return this;
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see UntypedJsonSchemaObject#oneOf(Collection)
		 */
		public MongoJsonSchemaBuilder oneOf(Set<JsonSchemaObject> oneOf) {

			root = root.oneOf(oneOf);
			return this;
		}

		/**
		 * @param notMatch must not be {@literal null}.
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see UntypedJsonSchemaObject#notMatch(JsonSchemaObject)
		 */
		public MongoJsonSchemaBuilder notMatch(JsonSchemaObject notMatch) {

			root = root.notMatch(notMatch);
			return this;
		}

		/**
		 * @param description must not be {@literal null}.
		 * @return {@code this} {@link MongoJsonSchemaBuilder}.
		 * @see UntypedJsonSchemaObject#description(String)
		 */
		public MongoJsonSchemaBuilder description(String description) {

			root = root.description(description);
			return this;
		}

		/**
		 * Define the {@literal encryptMetadata} element of the schema.
		 *
		 * @param encryptionMetadata can be {@literal null}.
		 * @since 3.3
		 */
		public void encryptionMetadata(@Nullable Document encryptionMetadata) {
			this.encryptionMetadata = encryptionMetadata;
		}

		/**
		 * Obtain the {@link MongoJsonSchema}.
		 *
		 * @return new instance of {@link MongoJsonSchema}.
		 */
		public MongoJsonSchema build() {
			return new DefaultMongoJsonSchema(root, encryptionMetadata);
		}
	}
}
