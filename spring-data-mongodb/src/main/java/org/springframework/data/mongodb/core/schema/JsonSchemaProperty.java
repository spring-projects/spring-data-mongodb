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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;

import org.bson.Document;
import org.springframework.data.domain.Range;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.ArrayJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.NumericJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.ObjectJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.StringJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type;

/**
 * @author Christoph Strobl
 * @since 2017/12
 */
public interface JsonSchemaProperty {

	Document toDocument();

	String getIdentifier();

	static StringJsonSchemaProperty string(String identifier) {
		return new StringJsonSchemaProperty(identifier, new StringJsonSchemaObject());
	}

	static ObjectJsonSchemaProperty object(String identifier) {
		return new ObjectJsonSchemaProperty(identifier, new ObjectJsonSchemaObject());
	}

	static NumericJsonSchemaProperty number(String identifier) {
		return new NumericJsonSchemaProperty(identifier, new NumericJsonSchemaObject());
	}

	static NumericJsonSchemaProperty int32(String identifier) {
		return new NumericJsonSchemaProperty(identifier, new NumericJsonSchemaObject(Type.intType(), null));
	}

	static NumericJsonSchemaProperty int64(String identifier) {
		return new NumericJsonSchemaProperty(identifier, new NumericJsonSchemaObject(Type.longType(), null));
	}

	static NumericJsonSchemaProperty float64(String identifier) {
		return new NumericJsonSchemaProperty(identifier, new NumericJsonSchemaObject(Type.doubleType(), null));
	}

	static NumericJsonSchemaProperty decimal128(String identifier) {
		return new NumericJsonSchemaProperty(identifier, new NumericJsonSchemaObject(Type.bigintType(), null));
	}

	static ArrayJsonSchemaProperty array(String identifier) {
		return new ArrayJsonSchemaProperty(identifier, new ArrayJsonSchemaObject(null, null));
	}

	static class StringJsonSchemaProperty extends IdentifiableJsonSchemaProperty<StringJsonSchemaObject> {

		public StringJsonSchemaProperty(String identifier, StringJsonSchemaObject type) {
			super(identifier, type);
		}

		public StringJsonSchemaProperty minLength(int length) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaType.minLength(length));
		}

		public StringJsonSchemaProperty maxLength(int length) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaType.maxLength(length));
		}

		public StringJsonSchemaProperty matching(String pattern) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaType.matching(pattern));
		}

		public StringJsonSchemaProperty possibleValues(String... possibleValues) {
			return possibleValues(Arrays.asList(possibleValues));
		}

		public StringJsonSchemaProperty allOf(JsonSchemaObject... allOf) {
			return allOf(new LinkedHashSet<>(Arrays.asList(allOf)));
		}

		public StringJsonSchemaProperty anyOf(JsonSchemaObject... anyOf) {
			return anyOf(new LinkedHashSet<>(Arrays.asList(anyOf)));
		}

		public StringJsonSchemaProperty oneOf(JsonSchemaObject... oneOf) {
			return oneOf(new LinkedHashSet<>(Arrays.asList(oneOf)));
		}

		public StringJsonSchemaProperty possibleValues(Collection<String> possibleValues) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaType.possibleValues((Collection) possibleValues));
		}

		public StringJsonSchemaProperty allOf(Collection<JsonSchemaObject> allOf) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaType.allOf(allOf));
		}

		public StringJsonSchemaProperty anyOf(Collection<JsonSchemaObject> anyOf) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaType.anyOf(anyOf));
		}

		public StringJsonSchemaProperty oneOf(Collection<JsonSchemaObject> oneOf) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaType.oneOf(oneOf));
		}

		public StringJsonSchemaProperty notMatch(JsonSchemaObject notMatch) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaType.notMatch(notMatch));
		}

		public StringJsonSchemaProperty description(String description) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaType.description(description));
		}

	}

	static class ObjectJsonSchemaProperty extends IdentifiableJsonSchemaProperty<ObjectJsonSchemaObject> {

		public ObjectJsonSchemaProperty(String identifier, ObjectJsonSchemaObject jsonSchemaType) {
			super(identifier, jsonSchemaType);
		}

		public ObjectJsonSchemaProperty nrProperties(Range<Integer> range) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.nrProperties(range));
		}

		public ObjectJsonSchemaProperty minNrProperties(int nrProperties) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.minNrProperties(nrProperties));
		}

		public ObjectJsonSchemaProperty maxNrProperties(int nrProperties) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.maxNrProperties(nrProperties));
		}

		public ObjectJsonSchemaProperty required(String... properties) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.required(properties));
		}

		public ObjectJsonSchemaProperty additionalProperties(boolean additionalPropertiesAllowed) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.additionalProperties(additionalPropertiesAllowed));
		}

		public ObjectJsonSchemaProperty additionalProperties(ObjectJsonSchemaObject additionalProperties) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.additionalProperties(additionalProperties));
		}

		public ObjectJsonSchemaProperty properties(JsonSchemaProperty... properties) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.properties(properties));
		}

		public ObjectJsonSchemaProperty possibleValues(Object... possibleValues) {
			return possibleValues(Arrays.asList(possibleValues));
		}

		public ObjectJsonSchemaProperty allOf(JsonSchemaObject... allOf) {
			return allOf(new LinkedHashSet<>(Arrays.asList(allOf)));
		}

		public ObjectJsonSchemaProperty anyOf(JsonSchemaObject... anyOf) {
			return anyOf(new LinkedHashSet<>(Arrays.asList(anyOf)));
		}

		public ObjectJsonSchemaProperty oneOf(JsonSchemaObject... oneOf) {
			return oneOf(new LinkedHashSet<>(Arrays.asList(oneOf)));
		}

		public ObjectJsonSchemaProperty possibleValues(Collection<Object> possibleValues) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.possibleValues(possibleValues));
		}

		public ObjectJsonSchemaProperty allOf(Collection<JsonSchemaObject> allOf) {

			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.allOf(allOf));
		}

		public ObjectJsonSchemaProperty anyOf(Collection<JsonSchemaObject> anyOf) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.anyOf(anyOf));
		}

		public ObjectJsonSchemaProperty oneOf(Collection<JsonSchemaObject> oneOf) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.oneOf(oneOf));
		}

		public ObjectJsonSchemaProperty notMatch(JsonSchemaObject notMatch) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.notMatch(notMatch));
		}

		public ObjectJsonSchemaProperty description(String description) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaType.description(description));
		}
	}

	static class NumericJsonSchemaProperty extends IdentifiableJsonSchemaProperty<NumericJsonSchemaObject> {

		public NumericJsonSchemaProperty(String identifier, NumericJsonSchemaObject jsonSchemaType) {
			super(identifier, jsonSchemaType);
		}

		public NumericJsonSchemaProperty multipleOf(Number value) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.multipleOf(value));
		}

		public NumericJsonSchemaProperty within(Range<? extends Number> range) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.within(range));
		}

		public NumericJsonSchemaProperty gt(Number min) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.gt(min));
		}

		public NumericJsonSchemaProperty gte(Number min) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.gte(min));
		}

		public NumericJsonSchemaProperty lt(Number max) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.lt(max));
		}

		public NumericJsonSchemaProperty lte(Number max) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.lte(max));
		}

		public NumericJsonSchemaProperty possibleValues(Number... possibleValues) {
			return possibleValues(new LinkedHashSet<>(Arrays.asList(possibleValues)));
		}

		public NumericJsonSchemaProperty allOf(JsonSchemaObject... allOf) {
			return allOf(Arrays.asList(allOf));
		}

		public NumericJsonSchemaProperty anyOf(JsonSchemaObject... anyOf) {
			return anyOf(new LinkedHashSet<>(Arrays.asList(anyOf)));
		}

		public NumericJsonSchemaProperty oneOf(JsonSchemaObject... oneOf) {
			return oneOf(new LinkedHashSet<>(Arrays.asList(oneOf)));
		}

		public NumericJsonSchemaProperty possibleValues(Collection<Number> possibleValues) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.possibleValues((Collection) possibleValues));
		}

		public NumericJsonSchemaProperty allOf(Collection<JsonSchemaObject> allOf) {

			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.allOf(allOf));
		}

		public NumericJsonSchemaProperty anyOf(Collection<JsonSchemaObject> anyOf) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.anyOf(anyOf));
		}

		public NumericJsonSchemaProperty oneOf(Collection<JsonSchemaObject> oneOf) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.oneOf(oneOf));
		}

		public NumericJsonSchemaProperty notMatch(JsonSchemaObject notMatch) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.notMatch(notMatch));
		}

		public NumericJsonSchemaProperty description(String description) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaType.description(description));
		}
	}

	static class ArrayJsonSchemaProperty extends IdentifiableJsonSchemaProperty<ArrayJsonSchemaObject> {

		public ArrayJsonSchemaProperty(String identifier, ArrayJsonSchemaObject jsonSchemaType) {
			super(identifier, jsonSchemaType);
		}

		public ArrayJsonSchemaProperty uniqueItems(boolean uniqueItems) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaType.uniqueItems(uniqueItems));
		}

		public ArrayJsonSchemaProperty range(Range<Integer> range) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaType.range(range));
		}

		public ArrayJsonSchemaProperty items(Collection<JsonSchemaObject> items) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaType.items(items));
		}

		public ArrayJsonSchemaProperty possibleValues(Object... possibleValues) {
			return possibleValues(new LinkedHashSet<>(Arrays.asList(possibleValues)));
		}

		public ArrayJsonSchemaProperty allOf(JsonSchemaObject... allOf) {
			return allOf(new LinkedHashSet<>(Arrays.asList(allOf)));
		}

		public ArrayJsonSchemaProperty anyOf(JsonSchemaObject... anyOf) {
			return anyOf(new LinkedHashSet<>(Arrays.asList(anyOf)));
		}

		public ArrayJsonSchemaProperty oneOf(JsonSchemaObject... oneOf) {
			return oneOf(new LinkedHashSet<>(Arrays.asList(oneOf)));
		}

		public ArrayJsonSchemaProperty possibleValues(Collection<Object> possibleValues) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaType.possibleValues(possibleValues));
		}

		public ArrayJsonSchemaProperty allOf(Collection<JsonSchemaObject> allOf) {

			return new ArrayJsonSchemaProperty(identifier, jsonSchemaType.allOf(allOf));
		}

		public ArrayJsonSchemaProperty anyOf(Collection<JsonSchemaObject> anyOf) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaType.anyOf(anyOf));
		}

		public ArrayJsonSchemaProperty oneOf(Collection<JsonSchemaObject> oneOf) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaType.oneOf(oneOf));
		}

		public ArrayJsonSchemaProperty notMatch(JsonSchemaObject notMatch) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaType.notMatch(notMatch));
		}

		public ArrayJsonSchemaProperty description(String description) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaType.description(description));
		}

	}
}
