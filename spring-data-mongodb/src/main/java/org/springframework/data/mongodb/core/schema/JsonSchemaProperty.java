/*
 * Copyright 2018-2024 the original author or authors.
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

import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.NumericJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.ObjectJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.IdentifiableJsonSchemaProperty.*;
import org.springframework.lang.Nullable;

/**
 * A {@literal property} or {@literal patternProperty} within a {@link JsonSchemaObject} of {@code type : 'object'}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
public interface JsonSchemaProperty extends JsonSchemaObject {

	/**
	 * The identifier can be either the property name or the regex expression properties have to match when used along
	 * with {@link ObjectJsonSchemaObject#patternProperties(JsonSchemaProperty...)}.
	 *
	 * @return never {@literal null}.
	 */
	String getIdentifier();

	/**
	 * @return {@literal false} by default.
	 * @since 2.2
	 */
	default boolean isRequired() {
		return false;
	}

	/**
	 * Creates a new {@link UntypedJsonSchemaProperty} with given {@literal identifier} without {@code type}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link UntypedJsonSchemaProperty}.
	 */
	static UntypedJsonSchemaProperty untyped(String identifier) {
		return new UntypedJsonSchemaProperty(identifier, JsonSchemaObject.untyped());
	}

	/**
	 * Turns the given target property into an {@link EncryptedJsonSchemaProperty ecrypted} one.
	 *
	 * @param property must not be {@literal null}.
	 * @return new instance of {@link EncryptedJsonSchemaProperty}.
	 * @since 2.2
	 */
	static EncryptedJsonSchemaProperty encrypted(JsonSchemaProperty property) {
		return EncryptedJsonSchemaProperty.encrypted(property);
	}

	/**
	 * Creates a new {@link StringJsonSchemaProperty} with given {@literal identifier} of {@code type : 'string'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link StringJsonSchemaProperty}.
	 */
	static StringJsonSchemaProperty string(String identifier) {
		return new StringJsonSchemaProperty(identifier, JsonSchemaObject.string());
	}

	/**
	 * Creates a new {@link ObjectJsonSchemaProperty} with given {@literal identifier} of {@code type : 'object'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link ObjectJsonSchemaProperty}.
	 */
	static ObjectJsonSchemaProperty object(String identifier) {
		return new ObjectJsonSchemaProperty(identifier, JsonSchemaObject.object());
	}

	/**
	 * Creates a new {@link JsonSchemaProperty} with given {@literal identifier} of {@code bsonType : 'objectId'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link JsonSchemaProperty}.
	 * @since 2.2
	 */
	static JsonSchemaProperty objectId(String identifier) {
		return JsonSchemaProperty.named(identifier).ofType(Type.objectIdType());
	}

	/**
	 * Creates a new {@link NumericJsonSchemaProperty} with given {@literal identifier} of {@code type : 'number'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link NumericJsonSchemaProperty}.
	 */
	static NumericJsonSchemaProperty number(String identifier) {
		return new NumericJsonSchemaProperty(identifier, JsonSchemaObject.number());
	}

	/**
	 * Creates a new {@link NumericJsonSchemaProperty} with given {@literal identifier} of {@code bsonType : 'int'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link NumericJsonSchemaProperty}.
	 */
	static NumericJsonSchemaProperty int32(String identifier) {
		return new NumericJsonSchemaProperty(identifier, new NumericJsonSchemaObject(Type.intType()));
	}

	/**
	 * Creates a new {@link NumericJsonSchemaProperty} with given {@literal identifier} of {@code bsonType : 'long'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link NumericJsonSchemaProperty}.
	 */
	static NumericJsonSchemaProperty int64(String identifier) {
		return new NumericJsonSchemaProperty(identifier, new NumericJsonSchemaObject(Type.longType()));
	}

	/**
	 * Creates a new {@link NumericJsonSchemaProperty} with given {@literal identifier} of {@code bsonType : 'double'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link NumericJsonSchemaProperty}.
	 */
	static NumericJsonSchemaProperty float64(String identifier) {
		return new NumericJsonSchemaProperty(identifier, new NumericJsonSchemaObject(Type.doubleType()));
	}

	/**
	 * Creates a new {@link NumericJsonSchemaProperty} with given {@literal identifier} of
	 * {@code bsonType : 'decimal128'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link NumericJsonSchemaProperty}.
	 */
	static NumericJsonSchemaProperty decimal128(String identifier) {
		return new NumericJsonSchemaProperty(identifier, new NumericJsonSchemaObject(Type.bigDecimalType()));
	}

	/**
	 * Creates a new {@link ArrayJsonSchemaProperty} with given {@literal identifier} of {@code type : 'array'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link ArrayJsonSchemaProperty}.
	 */
	static ArrayJsonSchemaProperty array(String identifier) {
		return new ArrayJsonSchemaProperty(identifier, JsonSchemaObject.array());
	}

	/**
	 * Creates a new {@link BooleanJsonSchemaProperty} with given {@literal identifier} of {@code type : 'boolean'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link BooleanJsonSchemaProperty}.
	 */
	static BooleanJsonSchemaProperty bool(String identifier) {
		return new BooleanJsonSchemaProperty(identifier, JsonSchemaObject.bool());
	}

	/**
	 * Creates a new {@link BooleanJsonSchemaProperty} with given {@literal identifier} of {@code type : 'null'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link NullJsonSchemaProperty}.
	 */
	static NullJsonSchemaProperty nil(String identifier) {
		return new NullJsonSchemaProperty(identifier, JsonSchemaObject.nil());
	}

	/**
	 * Creates a new {@link DateJsonSchemaProperty} with given {@literal identifier} of {@code type : 'date'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link DateJsonSchemaProperty}.
	 */
	static DateJsonSchemaProperty date(String identifier) {
		return new DateJsonSchemaProperty(identifier, JsonSchemaObject.date());
	}

	/**
	 * Creates a new {@link TimestampJsonSchemaProperty} with given {@literal identifier} of {@code type : 'timestamp'}.
	 *
	 * @param identifier the {@literal property} name or {@literal patternProperty} regex. Must not be {@literal null} nor
	 *          {@literal empty}.
	 * @return new instance of {@link TimestampJsonSchemaProperty}.
	 */
	static TimestampJsonSchemaProperty timestamp(String identifier) {
		return new TimestampJsonSchemaProperty(identifier, JsonSchemaObject.timestamp());
	}

	/**
	 * Obtain a builder to create a {@link JsonSchemaProperty}.
	 *
	 * @param identifier must not be {@literal null}.
	 * @return new instance of {@link JsonSchemaPropertyBuilder}.
	 */
	static JsonSchemaPropertyBuilder named(String identifier) {
		return new JsonSchemaPropertyBuilder(identifier);
	}

	/**
	 * Turns the given {@link JsonSchemaProperty} into a required on.
	 *
	 * @param property must not be {@literal null}.
	 * @return new instance of {@link JsonSchemaProperty}.
	 * @since 2.2
	 */
	static JsonSchemaProperty required(JsonSchemaProperty property) {
		return new RequiredJsonSchemaProperty(property, true);
	}

	/**
	 * Merges multiple {@link JsonSchemaProperty} with potentially different attributes into one.
	 *
	 * @param properties must not be {@literal null}.
	 * @return new instance of {@link JsonSchemaProperty}.
	 * @since 3.4
	 */
	static JsonSchemaProperty merged(Collection<JsonSchemaProperty> properties) {
		return new MergedJsonSchemaProperty(properties);
	}

	/**
	 * Builder for {@link IdentifiableJsonSchemaProperty}.
	 */
	class JsonSchemaPropertyBuilder {

		private final String identifier;

		JsonSchemaPropertyBuilder(String identifier) {
			this.identifier = identifier;
		}

		/**
		 * Configure a {@link Type} for the property.
		 *
		 * @param type must not be {@literal null}.
		 * @return new instance of {@link IdentifiableJsonSchemaProperty}.
		 */
		public IdentifiableJsonSchemaProperty<TypedJsonSchemaObject> ofType(Type type) {
			return new IdentifiableJsonSchemaProperty<>(identifier, TypedJsonSchemaObject.of(type));
		}

		/**
		 * Configure the {@link Type} for the property by deriving it from the given {@link Class type}.
		 *
		 * @param type must not be {@literal null}.
		 * @return new instance of {@link IdentifiableJsonSchemaProperty}.
		 * @since 2.2
		 */
		public IdentifiableJsonSchemaProperty<TypedJsonSchemaObject> ofType(@Nullable Class<?> type) {
			return new IdentifiableJsonSchemaProperty<>(identifier, JsonSchemaObject.of(type));
		}

		/**
		 * Configure a {@link TypedJsonSchemaObject} for the property.
		 *
		 * @param schemaObject must not be {@literal null}.
		 * @return new instance of {@link IdentifiableJsonSchemaProperty}.
		 */
		public IdentifiableJsonSchemaProperty<TypedJsonSchemaObject> with(TypedJsonSchemaObject schemaObject) {
			return new IdentifiableJsonSchemaProperty<>(identifier, schemaObject);
		}

		/**
		 * @return an untyped {@link IdentifiableJsonSchemaProperty}.
		 */
		public IdentifiableJsonSchemaProperty<UntypedJsonSchemaObject> withoutType() {
			return new IdentifiableJsonSchemaProperty<>(identifier, UntypedJsonSchemaObject.newInstance());
		}
	}

}
