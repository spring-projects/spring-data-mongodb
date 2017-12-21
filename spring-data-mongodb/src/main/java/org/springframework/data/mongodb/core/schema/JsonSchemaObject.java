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

import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.bson.Document;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.ArrayJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.BooleanJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.NullJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.NumericJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.ObjectJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.StringJsonSchemaObject;

/**
 * @author Christoph Strobl
 * @since 2.1
 */
public interface JsonSchemaObject {

	/**
	 * Get the set of types defined for this schema element.<br />
	 * The {@link Set} is likely to contain only one element in most cases.
	 *
	 * @return never {@literal null}.
	 */
	Set<Type> getTypes();

	/**
	 * Get the MongoDB specific representation.<br />
	 * The Document may contain fields (eg. like {@literal bsonType}) not contained in the JsonSchema specification. It
	 * may also contain types not directly processable by the MongoDB java driver. Make sure to run the produced
	 * {@link Document} through the mapping infrastructure.
	 *
	 * @return never {@literal null}.
	 */
	Document toDocument();

	/**
	 * Create a new {@link JsonSchemaObject} of {@code type : 'object'}.
	 *
	 * @return never {@literal null}.
	 */
	static ObjectJsonSchemaObject object() {
		return new ObjectJsonSchemaObject();
	}

	/**
	 * Create a new {@link JsonSchemaObject} of {@code type : 'string'}.
	 *
	 * @return never {@literal null}.
	 */
	static StringJsonSchemaObject string() {
		return new StringJsonSchemaObject();
	}

	/**
	 * Create a new {@link JsonSchemaObject} of {@code type : 'number'}.
	 *
	 * @return never {@literal null}.
	 */
	static NumericJsonSchemaObject number() {
		return new NumericJsonSchemaObject();
	}

	/**
	 * Create a new {@link JsonSchemaObject} of {@code type : 'array'}.
	 *
	 * @return never {@literal null}.
	 */
	static ArrayJsonSchemaObject array() {
		return new ArrayJsonSchemaObject();
	}

	/**
	 * Create a new {@link JsonSchemaObject} of {@code type : 'boolean'}.
	 *
	 * @return never {@literal null}.
	 */
	static BooleanJsonSchemaObject bool() {
		return new BooleanJsonSchemaObject();
	}

	/**
	 * Create a new {@link JsonSchemaObject} of {@code type : 'null'}.
	 *
	 * @return never {@literal null}.
	 */
	static NullJsonSchemaObject nil() {
		return new NullJsonSchemaObject();
	}

	/**
	 * Create a new {@link JsonSchemaObject} of given {@link Type}.
	 *
	 * @return never {@literal null}.
	 */
	static TypedJsonSchemaObject of(Type type) {
		return TypedJsonSchemaObject.of(type);
	}

	/**
	 * Type represents either a json schema {@literal type} or a MongoDB specific {@literal bsonType}.
	 */
	interface Type {

		// BSON TYPES
		final Type OBJECT_ID = bsonTypeOf("objectId");
		final Type REGULAR_EXPRESSION = bsonTypeOf("regex");
		final Type DOUBLE = bsonTypeOf("double");
		final Type BINARY_DATA = bsonTypeOf("binData");
		final Type DATE = bsonTypeOf("date");
		final Type JAVA_SCRIPT = bsonTypeOf("javascript");
		final Type INT_32 = bsonTypeOf("int");
		final Type INT_64 = bsonTypeOf("long");
		final Type DECIMAL_128 = bsonTypeOf("decimal");
		final Type TIMESTAMP = bsonTypeOf("timestamp");

		final Set<Type> BSON_TYPES = new HashSet<>(Arrays.asList(OBJECT_ID, REGULAR_EXPRESSION, DOUBLE, BINARY_DATA, DATE,
				JAVA_SCRIPT, INT_32, INT_64, DECIMAL_128, TIMESTAMP));

		// JSON SCHEMA TYPES
		final Type OBJECT = jsonTypeOf("object");
		final Type ARRAY = jsonTypeOf("array");
		final Type NUMBER = jsonTypeOf("number");
		final Type BOOLEAN = jsonTypeOf("boolean");
		final Type STRING = jsonTypeOf("string");
		final Type NULL = jsonTypeOf("null");

		final Set<Type> JSON_TYPES = new HashSet<>(Arrays.asList(OBJECT, ARRAY, NUMBER, BOOLEAN, STRING, NULL));

		/**
		 * @return a constant {@link Type} representing {@code bsonType : 'objectId' }.
		 */
		static Type objectIdType() {
			return OBJECT_ID;
		}

		/**
		 * @return a constant {@link Type} representing {@code bsonType : 'regex' }.
		 */
		static Type regexType() {
			return REGULAR_EXPRESSION;
		}

		/**
		 * @return a constant {@link Type} representing {@code bsonType : 'double' }.
		 */
		static Type doubleType() {
			return DOUBLE;
		}

		/**
		 * @return a constant {@link Type} representing {@code bsonType : 'binData' }.
		 */
		static Type binaryType() {
			return BINARY_DATA;
		}

		/**
		 * @return a constant {@link Type} representing {@code bsonType : 'date' }.
		 */
		static Type dateType() {
			return DATE;
		}

		/**
		 * @return a constant {@link Type} representing {@code bsonType : 'javascript' }.
		 */
		static Type javascriptType() {
			return JAVA_SCRIPT;
		}

		/**
		 * @return a constant {@link Type} representing {@code bsonType : 'int' }.
		 */
		static Type intType() {
			return INT_32;
		}

		/**
		 * @return a constant {@link Type} representing {@code bsonType : 'long' }.
		 */
		static Type longType() {
			return INT_64;
		}

		/**
		 * @return a constant {@link Type} representing {@code bsonType : 'decimal128' }.
		 */
		static Type bigintType() {
			return DECIMAL_128;
		}

		/**
		 * @return a constant {@link Type} representing {@code bsonType : 'timestamp' }.
		 */
		static Type timestampType() {
			return TIMESTAMP;
		}

		/**
		 * @return a constant {@link Type} representing {@code type : 'object' }.
		 */
		static Type objectType() {
			return OBJECT;
		}

		/**
		 * @return a constant {@link Type} representing {@code type : 'array' }.
		 */
		static Type arrayType() {
			return ARRAY;
		}

		/**
		 * @return a constant {@link Type} representing {@code type : 'number' }.
		 */
		static Type numberType() {
			return NUMBER;
		}

		/**
		 * @return a constant {@link Type} representing {@code type : 'boolean' }.
		 */
		static Type booleanType() {
			return BOOLEAN;
		}

		/**
		 * @return a constant {@link Type} representing {@code type : 'string' }.
		 */
		static Type stringType() {
			return STRING;
		}

		/**
		 * @return a constant {@link Type} representing {@code type : 'null' }.
		 */
		static Type nullType() {
			return NULL;
		}

		/**
		 * @return new {@link Type} representing the given {@code bsonType}.
		 */
		static Type bsonTypeOf(String name) {
			return new BsonType(name);
		}

		/**
		 * @return new {@link Type} representing the given {@code type}.
		 */
		static Type jsonTypeOf(String name) {
			return new JsonType(name);
		}

		static Set<Type> jsonTypes() {
			return JSON_TYPES;
		}

		static Set<Type> bsonTypes() {
			return BSON_TYPES;
		}

		/**
		 * Get the {@link Type} representation. Either {@code type} or {@code bsonType}.
		 *
		 * @return never {@literal null}.
		 */
		String representation();

		/**
		 * Get the {@link Type} value. Like {@literal string}, {@literal number},...
		 *
		 * @return never {@literal null}.
		 */
		Object value();

		@RequiredArgsConstructor
		static class JsonType implements Type {

			private final String name;

			@Override
			public String representation() {
				return "type";
			}

			@Override
			public String value() {
				return name;
			}
		}

		@RequiredArgsConstructor
		static class BsonType implements Type {

			private final String name;

			@Override
			public String representation() {
				return "bsonType";
			}

			@Override
			public String value() {
				return name;
			}
		}
	}
}
