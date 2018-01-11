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

import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.ArrayJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.BooleanJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.NullJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.NumericJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.ObjectJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.StringJsonSchemaObject;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

/**
 * Interface that can be implemented by objects that know how to serialize themselves to JSON schema using
 * {@link #toDocument()}.
 * <p/>
 * This class also declares factory methods for type-specific {@link JsonSchemaObject schema objects} such as
 * {@link #string()} or {@link #object()}. For example:
 *
 * <pre class="code">
 * JsonSchemaProperty.object("address").properties(JsonSchemaProperty.string("city").minLength(3));
 * </pre>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
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
	 * Create a new {@link UntypedJsonSchemaObject}.
	 *
	 * @return never {@literal null}.
	 */
	static UntypedJsonSchemaObject untyped() {
		return new UntypedJsonSchemaObject(null, null, false);
	}

	/**
	 * Create a new {@link JsonSchemaObject} matching the given {@code type}.
	 *
	 * @param type Java class to create a {@link JsonSchemaObject} for. May be {@literal null} or {@link Void#getClass()}
	 *          to create {@link Type#nullType() null} type.
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException if {@code type} is not supported.
	 */
	static TypedJsonSchemaObject of(@Nullable Class<?> type) {

		if (type == null || type.equals(Void.class)) {
			return of(Type.nullType());
		}

		if (type.isArray()) {

			if (type.equals(byte[].class)) {
				return of(Type.binaryType());
			}

			return of(Type.arrayType());
		}

		if (type.equals(Object.class)) {
			return of(Type.objectType());
		}

		if (type.equals(ObjectId.class)) {
			return of(Type.objectIdType());
		}

		if (ClassUtils.isAssignable(String.class, type)) {
			return of(Type.stringType());
		}

		if (ClassUtils.isAssignable(Date.class, type)) {
			return of(Type.dateType());
		}

		if (ClassUtils.isAssignable(BsonTimestamp.class, type)) {
			return of(Type.timestampType());
		}

		if (ClassUtils.isAssignable(Pattern.class, type)) {
			return of(Type.regexType());
		}

		if (ClassUtils.isAssignable(Boolean.class, type)) {
			return of(Type.booleanType());
		}

		if (ClassUtils.isAssignable(Number.class, type)) {

			if (type.equals(Long.class)) {
				return of(Type.longType());
			}

			if (type.equals(Float.class)) {
				return of(Type.doubleType());
			}

			if (type.equals(Double.class)) {
				return of(Type.doubleType());
			}

			if (type.equals(Integer.class)) {
				return of(Type.intType());
			}

			if (type.equals(BigDecimal.class)) {
				return of(Type.bigDecimalType());
			}

			return of(Type.numberType());
		}

		throw new IllegalArgumentException(String.format("No JSON schema type found for %s.", type));
	}

	/**
	 * Type represents either a JSON schema {@literal type} or a MongoDB specific {@literal bsonType}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface Type {

		// BSON TYPES
		Type OBJECT_ID = bsonTypeOf("objectId");
		Type REGULAR_EXPRESSION = bsonTypeOf("regex");
		Type DOUBLE = bsonTypeOf("double");
		Type BINARY_DATA = bsonTypeOf("binData");
		Type DATE = bsonTypeOf("date");
		Type JAVA_SCRIPT = bsonTypeOf("javascript");
		Type INT_32 = bsonTypeOf("int");
		Type INT_64 = bsonTypeOf("long");
		Type DECIMAL_128 = bsonTypeOf("decimal");
		Type TIMESTAMP = bsonTypeOf("timestamp");

		Set<Type> BSON_TYPES = new HashSet<>(Arrays.asList(OBJECT_ID, REGULAR_EXPRESSION, DOUBLE, BINARY_DATA, DATE,
				JAVA_SCRIPT, INT_32, INT_64, DECIMAL_128, TIMESTAMP));

		// JSON SCHEMA TYPES
		Type OBJECT = jsonTypeOf("object");
		Type ARRAY = jsonTypeOf("array");
		Type NUMBER = jsonTypeOf("number");
		Type BOOLEAN = jsonTypeOf("boolean");
		Type STRING = jsonTypeOf("string");
		Type NULL = jsonTypeOf("null");

		Set<Type> JSON_TYPES = new HashSet<>(Arrays.asList(OBJECT, ARRAY, NUMBER, BOOLEAN, STRING, NULL));

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
		static Type bigDecimalType() {
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

		/**
		 * @return all known JSON types.
		 */
		static Set<Type> jsonTypes() {
			return JSON_TYPES;
		}

		/**
		 * @return all known BSON types.
		 */
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

		/**
		 * @author Christpoh Strobl
		 * @since 2.1
		 */
		@RequiredArgsConstructor
		class JsonType implements Type {

			private final String name;

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type#representation()
			 */
			@Override
			public String representation() {
				return "type";
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type#value()
			 */
			@Override
			public String value() {
				return name;
			}
		}

		/**
		 * @author Christpoh Strobl
		 * @since 2.1
		 */
		@RequiredArgsConstructor
		class BsonType implements Type {

			private final String name;

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type#representation()
			 */
			@Override
			public String representation() {
				return "bsonType";
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type#value()
			 */
			@Override
			public String value() {
				return name;
			}
		}
	}
}
