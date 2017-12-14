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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @since 2017/12
 */
public interface JsonSchemaObject {

	@Nullable
	Set<Type> getTypes();

	Document toDocument();

	class ObjectJsonSchemaObject extends AbstractJsonSchemaObject {

		private @Nullable Range<Integer> nrProperties;
		private @Nullable Object additionalProperties;
		private List<String> requiredProperties = Collections.emptyList();
		private List<JsonSchemaProperty> properties = Collections.emptyList();
		private List<JsonSchemaProperty> patternProperties = Collections.emptyList();

		public ObjectJsonSchemaObject() {
			this(null, null);
		}

		public ObjectJsonSchemaObject(@Nullable String description, @Nullable Restrictions restrictions) {
			super(Type.objectType(), description, restrictions);
		}

		/*
		 * minProperties	objects	integer	Indicates the field’s minimum number of properties
		 * maxProperties	strings	integer	Indicates the field’s maximum number of properties
		 */
		public ObjectJsonSchemaObject nrProperties(@Nullable Range<Integer> range) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.nrProperties = range;
			return newInstance;
		}

		/*
		 * inProperties	objects	integer	Indicates the field’s minimum number of properties
		 */
		public ObjectJsonSchemaObject minNrProperties(int nrProperties) {

			Bound upper = this.nrProperties != null ? this.nrProperties.getUpperBound() : Bound.unbounded();
			return nrProperties(Range.of(Bound.inclusive(nrProperties), upper));
		}

		/*
		 * maxProperties	strings	integer	Indicates the field’s maximum number of properties
		 */
		public ObjectJsonSchemaObject maxNrProperties(int nrProperties) {

			Bound lower = this.nrProperties != null ? this.nrProperties.getLowerBound() : Bound.unbounded();
			return nrProperties(Range.of(lower, Bound.inclusive(nrProperties)));
		}

		/*
		 * required	objects	array of unique strings	Object’s property set must contain all the specified elements in the array
		 */
		public ObjectJsonSchemaObject required(String... properties) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.requiredProperties = new ArrayList<>(this.requiredProperties.size() + properties.length);
			newInstance.requiredProperties.addAll(this.requiredProperties);
			newInstance.requiredProperties.addAll(Arrays.asList(properties));
			return newInstance;
		}

		/*
		 * required	objects	array of unique strings	Object’s property set must contain all the specified elements in the array
		 */
		public ObjectJsonSchemaObject additionalProperties(boolean additionalPropertiesAllowed) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.additionalProperties = additionalPropertiesAllowed;
			return newInstance;
		}

		public ObjectJsonSchemaObject additionalProperties(ObjectJsonSchemaObject schema) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.additionalProperties = schema;
			return newInstance;
		}

		public ObjectJsonSchemaObject properties(JsonSchemaProperty... properties) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.properties = new ArrayList<>(this.properties.size() + properties.length);
			newInstance.properties.addAll(this.properties);
			newInstance.properties.addAll(Arrays.asList(properties));
			return newInstance;
		}

		public ObjectJsonSchemaObject patternProperties(JsonSchemaProperty... properties) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.patternProperties = new ArrayList<>(this.patternProperties.size() + properties.length);
			newInstance.patternProperties.addAll(this.patternProperties);
			newInstance.patternProperties.addAll(Arrays.asList(properties));
			return newInstance;
		}

		public ObjectJsonSchemaObject property(JsonSchemaProperty property) {
			return properties(property);
		}

		/*
		 * minProperties	objects	integer	Indicates the field’s minimum number of properties
		 * required	objects	array of unique strings	Object’s property set must contain all the specified elements in the array
		 * additionalProperties	objects	boolean or object If true, additional fields are allowed. If false, they are not. If a valid JSON Schema object is specified, additional fields must validate against the schema. Defaults to true.
		
		 * properties	objects	object	A valid JSON Schema where each value is also a valid JSON Schema object
		 * patternProperties	objects	object	In addition to properties requirements, each property name of this object must be a valid regular expression
		 * dependencies	objects	object
		 */

		@Override
		public ObjectJsonSchemaObject possibleValues(Collection<Object> possibleValues) {
			return newInstance(description, restrictions.possibleValues(possibleValues));
		}

		@Override
		public ObjectJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return newInstance(description, restrictions.allOf(allOf));
		}

		@Override
		public ObjectJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return newInstance(description, restrictions.anyOf(anyOf));
		}

		@Override
		public ObjectJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return newInstance(description, restrictions.oneOf(oneOf));
		}

		@Override
		public ObjectJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return newInstance(description, restrictions.notMatch(notMatch));
		}

		@Override
		public ObjectJsonSchemaObject description(String description) {
			return newInstance(description, restrictions);
		}

		@Override
		public Document toDocument() {

			Document doc = new Document(super.toDocument());
			if (!CollectionUtils.isEmpty(requiredProperties)) {
				doc.append("required", requiredProperties);
			}

			if (nrProperties != null) {

				if (nrProperties.getLowerBound().isBounded()) {
					doc.append("minProperties", nrProperties.getLowerBound().getValue().get());

				}

				if (nrProperties.getUpperBound().isBounded()) {
					doc.append("maxProperties", nrProperties.getUpperBound().getValue().get());
				}
			}
			if (!CollectionUtils.isEmpty(properties)) {
				doc.append("properties", reduceToDocument(properties));
			}

			if (!CollectionUtils.isEmpty(patternProperties)) {
				doc.append("patternProperties", reduceToDocument(patternProperties));
			}

			if (additionalProperties != null) {

				doc.append("additionalProperties", additionalProperties instanceof JsonSchemaObject
						? ((JsonSchemaObject) additionalProperties).toDocument() : additionalProperties);
			}
			return doc;
		}

		private ObjectJsonSchemaObject newInstance(String description, Restrictions restrictions) {

			ObjectJsonSchemaObject newInstance = new ObjectJsonSchemaObject(description, restrictions);
			newInstance.properties = this.properties;
			newInstance.requiredProperties = this.requiredProperties;
			newInstance.additionalProperties = this.additionalProperties;
			newInstance.nrProperties = this.nrProperties;
			newInstance.patternProperties = this.patternProperties;
			return newInstance;
		}

		private Document reduceToDocument(Collection<JsonSchemaProperty> source) {

			return source.stream() //
					.map(JsonSchemaProperty::toDocument) //
					.collect(Document::new, (target, propertyDocument) -> target.putAll(propertyDocument),
							(target, propertyDocument) -> {});
		}
	}

	static class NumericJsonSchemaObject extends AbstractJsonSchemaObject {

		private static final Set<Type> NUMERIC_TYPES = new HashSet<>(
				Arrays.asList(Type.doubleType(), Type.intType(), Type.longType(), Type.numberType(), Type.bigintType()));

		@Nullable Number multipleOf;
		@Nullable Range<? extends Number> range;

		public NumericJsonSchemaObject() {
			this(null);
		}

		public NumericJsonSchemaObject(@Nullable String description) {
			this(Type.numberType(), description);
		}

		public NumericJsonSchemaObject(Type type, @Nullable String description) {
			this(Collections.singleton(type), description, null);
		}

		public NumericJsonSchemaObject(Set<Type> types, @Nullable String description, @Nullable Restrictions restrictions) {
			super(validateTypes(types), description, restrictions);
		}

		private static Set<Type> validateTypes(Set<Type> types) {

			types.forEach(type -> {
				Assert.isTrue(NUMERIC_TYPES.contains(type),
						() -> String.format("%s is not a valid numeric type. Expected one of %s.", type, NUMERIC_TYPES));
			});

			return types;
		}

		/* multipleOf	numbers	number	Field must be a multiple of this value */
		NumericJsonSchemaObject multipleOf(Number value) {

			NumericJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.multipleOf = value;
			return newInstance;
		}

		/*
		
		maximum				numbers	number	Indicates the maximum value of the field
		exclusiveMaximum	numbers	boolean	If true and field is a number, maximum is an exclusive maximum. Otherwise, it is an inclusive maximum.
		minimum				numbers	number	Indicates the minimum value of the field
		exclusiveMinimum	numbers	boolean	If true, minimum is an exclusive minimum. Otherwise, it is an inclusive minimum.
		 */
		NumericJsonSchemaObject within(Range<? extends Number> range) {

			NumericJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.range = range;
			return newInstance;
		}

		NumericJsonSchemaObject gt(Number min) {

			Bound upper = this.range != null ? this.range.getUpperBound() : Bound.unbounded();

			if (min instanceof Long) {
				return within(Range.of(Bound.exclusive((Long) min), upper));
			}
			if (min instanceof Double) {
				return within(Range.of(Bound.exclusive((Double) min), upper));
			}
			if (min instanceof Float) {
				return within(Range.of(Bound.exclusive((Float) min), upper));
			}
			if (min instanceof Integer) {
				return within(Range.of(Bound.exclusive((Integer) min), upper));
			}
			if (min instanceof BigDecimal) {
				return within(Range.of(Bound.exclusive((BigDecimal) min), upper));
			}

			throw new IllegalArgumentException("Unsupported numeric value.");
		}

		NumericJsonSchemaObject gte(Number min) {

			Bound upper = this.range != null ? this.range.getUpperBound() : Bound.unbounded();

			if (min instanceof Long) {
				return within(Range.of(Bound.inclusive((Long) min), upper));
			}
			if (min instanceof Double) {
				return within(Range.of(Bound.inclusive((Double) min), upper));
			}
			if (min instanceof Float) {
				return within(Range.of(Bound.inclusive((Float) min), upper));
			}
			if (min instanceof Integer) {
				return within(Range.of(Bound.inclusive((Integer) min), upper));
			}
			if (min instanceof BigDecimal) {
				return within(Range.of(Bound.inclusive((BigDecimal) min), upper));
			}

			throw new IllegalArgumentException("Unsupported numeric value.");
		}

		NumericJsonSchemaObject lt(Number max) {

			Bound lower = this.range != null ? this.range.getLowerBound() : Bound.unbounded();

			if (max instanceof Long) {
				return within(Range.of(lower, Bound.exclusive((Long) max)));
			}
			if (max instanceof Double) {
				return within(Range.of(lower, Bound.exclusive((Double) max)));
			}
			if (max instanceof Float) {
				return within(Range.of(lower, Bound.exclusive((Float) max)));
			}
			if (max instanceof Integer) {
				return within(Range.of(lower, Bound.exclusive((Integer) max)));
			}
			if (max instanceof BigDecimal) {
				return within(Range.of(lower, Bound.exclusive((BigDecimal) max)));
			}

			throw new IllegalArgumentException("Unsupported numeric value.");
		}

		NumericJsonSchemaObject lte(Number max) {

			Bound lower = this.range != null ? this.range.getLowerBound() : Bound.unbounded();

			if (max instanceof Long) {
				return within(Range.of(lower, Bound.inclusive((Long) max)));
			}
			if (max instanceof Double) {
				return within(Range.of(lower, Bound.inclusive((Double) max)));
			}
			if (max instanceof Float) {
				return within(Range.of(lower, Bound.inclusive((Float) max)));
			}
			if (max instanceof Integer) {
				return within(Range.of(lower, Bound.inclusive((Integer) max)));
			}
			if (max instanceof BigDecimal) {
				return within(Range.of(lower, Bound.inclusive((BigDecimal) max)));
			}

			throw new IllegalArgumentException("Unsupported numeric value.");
		}

		@Override
		public NumericJsonSchemaObject possibleValues(Collection<Object> possibleValues) {
			return newInstance(description, restrictions.possibleValues(possibleValues));
		}

		@Override
		public NumericJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return newInstance(description, restrictions.allOf(allOf));
		}

		@Override
		public NumericJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return newInstance(description, restrictions.anyOf(anyOf));
		}

		@Override
		public NumericJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return newInstance(description, restrictions.oneOf(oneOf));
		}

		@Override
		public NumericJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return newInstance(description, restrictions.notMatch(notMatch));
		}

		@Override
		public NumericJsonSchemaObject description(String description) {
			return newInstance(description, restrictions);
		}

		@Override
		public Document toDocument() {

			Document doc = new Document(super.toDocument());

			if (multipleOf != null) {
				doc.append("multipleOf", multipleOf);
			}

			if (range != null) {

				if (range.getLowerBound().isBounded()) {
					doc.append("minimum", range.getLowerBound().getValue().get());
					if (!range.getLowerBound().isInclusive()) {
						doc.append("exclusiveMinimum", true);
					}
				}

				if (range.getUpperBound().isBounded()) {
					doc.append("maximum", range.getUpperBound().getValue().get());
					if (!range.getUpperBound().isInclusive()) {
						doc.append("exclusiveMaximum", true);
					}
				}
			}

			return doc;
		}

		private NumericJsonSchemaObject newInstance(String description, Restrictions restrictions) {

			NumericJsonSchemaObject newInstance = new NumericJsonSchemaObject(types, description, restrictions);

			newInstance.multipleOf = this.multipleOf;
			newInstance.range = this.range;
			return newInstance;

		}
	}

	static class StringJsonSchemaObject extends AbstractJsonSchemaObject {

		@Nullable Range<Integer> length;
		@Nullable String pattern;

		StringJsonSchemaObject() {
			this(null, null);
		}

		public StringJsonSchemaObject(@Nullable String description, @Nullable Restrictions restrictions) {
			super(Type.stringType(), description, restrictions);
		}

		/*
		maxLength	strings	integer	Indicates the maximum length of the field
		minLength	strings	integer	Indicates the minimum length of the field
		pattern	strings	string containing a regex	Field must match the regular expression
		 */

		/*
		 * maxLength	strings	integer	Indicates the maximum length of the field
		 * minLength	strings	integer	Indicates the minimum length of the field
		 */
		StringJsonSchemaObject length(Range<Integer> range) {

			StringJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.length = range;
			return newInstance;
		}

		/*
		 * minLength	strings	integer	Indicates the minimum length of the field
		 */
		StringJsonSchemaObject minLength(int length) {

			Bound upper = this.length != null ? this.length.getUpperBound() : Bound.unbounded();
			return length(Range.of(Bound.inclusive(length), upper));
		}

		/*
		 * maxLength	strings	integer	Indicates the maximum length of the field
		 */
		StringJsonSchemaObject maxLength(int length) {

			Bound lower = this.length != null ? this.length.getLowerBound() : Bound.unbounded();
			return length(Range.of(lower, Bound.inclusive(length)));
		}

		/*
		 * pattern	strings	string containing a regex	Field must match the regular expression
		 */
		StringJsonSchemaObject matching(String pattern) {

			StringJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.pattern = pattern;
			return newInstance;
		}

		@Override
		public Document toDocument() {

			Document doc = new Document(super.toDocument());

			if (length != null) {

				if (length.getLowerBound().isBounded()) {
					doc.append("minLength", length.getLowerBound().getValue().get());
				}

				if (length.getUpperBound().isBounded()) {
					doc.append("maxLength", length.getUpperBound().getValue().get());
				}
			}

			if (!StringUtils.isEmpty(pattern)) {
				doc.append("pattern", pattern);
			}

			return doc;
		}

		@Override
		public StringJsonSchemaObject possibleValues(Collection<Object> possibleValues) {
			return newInstance(description, restrictions.possibleValues(possibleValues));
		}

		@Override
		public StringJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return newInstance(description, restrictions.allOf(allOf));
		}

		@Override
		public StringJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return newInstance(description, restrictions.anyOf(anyOf));
		}

		@Override
		public StringJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return newInstance(description, restrictions.oneOf(oneOf));
		}

		@Override
		public StringJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return newInstance(description, restrictions.notMatch(notMatch));
		}

		@Override
		public StringJsonSchemaObject description(String description) {
			return newInstance(description, restrictions);
		}

		private StringJsonSchemaObject newInstance(String description, Restrictions restrictions) {

			StringJsonSchemaObject newInstance = new StringJsonSchemaObject(description, restrictions);

			newInstance.length = this.length;
			newInstance.pattern = this.pattern;
			return newInstance;
		}
	}

	static class ArrayJsonSchemaObject extends AbstractJsonSchemaObject {

		private @Nullable Boolean uniqueItems;
		private @Nullable Range<Integer> range;
		private Collection<JsonSchemaObject> items;

		public ArrayJsonSchemaObject(@Nullable String description, @Nullable Restrictions restrictions) {
			super(Collections.singleton(Type.arrayType()), description, restrictions);
		}

		ArrayJsonSchemaObject uniqueItems(boolean uniqueItems) {

			ArrayJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.uniqueItems = uniqueItems;
			return newInstance;
		}

		ArrayJsonSchemaObject range(Range<Integer> range) {

			ArrayJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.range = range;
			return newInstance;
		}

		ArrayJsonSchemaObject items(Collection<JsonSchemaObject> items) {

			ArrayJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.items = new ArrayList<>(items);
			return newInstance;
		}

		@Override
		public ArrayJsonSchemaObject possibleValues(Collection<Object> possibleValues) {
			return newInstance(description, restrictions.possibleValues(possibleValues));
		}

		@Override
		public ArrayJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return newInstance(description, restrictions.allOf(allOf));
		}

		@Override
		public ArrayJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return newInstance(description, restrictions.anyOf(anyOf));
		}

		@Override
		public ArrayJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return newInstance(description, restrictions.oneOf(oneOf));
		}

		@Override
		public ArrayJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return newInstance(description, restrictions.notMatch(notMatch));
		}

		@Override
		public ArrayJsonSchemaObject description(String description) {
			return newInstance(description, restrictions);
		}

		@Override
		public Document toDocument() {
			Document doc = new Document(super.toDocument());

			if (!CollectionUtils.isEmpty(items)) {
				doc.append("items", items.size() == 1 ? items.iterator().next()
						: items.stream().map(JsonSchemaObject::toDocument).collect(Collectors.toList()));
			}

			if (range != null) {

				if (range.getLowerBound().isBounded()) {
					doc.append("minItems", range.getLowerBound().getValue().get());
				}

				if (range.getUpperBound().isBounded()) {
					doc.append("maxItems", range.getUpperBound().getValue().get());
				}
			}

			if (ObjectUtils.nullSafeEquals(uniqueItems, Boolean.TRUE)) {
				doc.append("uniqueItems", true);
			}

			return doc;
		}

		private ArrayJsonSchemaObject newInstance(String description, Restrictions restrictions) {

			ArrayJsonSchemaObject newInstance = new ArrayJsonSchemaObject(description, restrictions);
			newInstance.uniqueItems = this.uniqueItems;
			newInstance.range = this.range;
			newInstance.items = this.items;
			return newInstance;
		}
	}

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

		static Type objectIdType() {
			return OBJECT_ID;
		}

		static Type regexType() {
			return REGULAR_EXPRESSION;
		}

		static Type doubleType() {
			return DOUBLE;
		}

		static Type binaryType() {
			return BINARY_DATA;
		}

		static Type dateType() {
			return DATE;
		}

		static Type javascriptType() {
			return JAVA_SCRIPT;
		}

		static Type intType() {
			return INT_32;
		}

		static Type longType() {
			return INT_64;
		}

		static Type bigintType() {
			return DECIMAL_128;
		}

		static Type timestampType() {
			return TIMESTAMP;
		}

		static Type objectType() {
			return OBJECT;
		}

		static Type arrayType() {
			return ARRAY;
		}

		static Type numberType() {
			return NUMBER;
		}

		static Type booleanType() {
			return BOOLEAN;
		}

		static Type stringType() {
			return STRING;
		}

		static Type nullType() {
			return NULL;
		}

		static Type enumOf(Object... values) {
			return new EnumType(values);
		}

		static Type bsonTypeOf(String name) {
			return new BsonType(name);
		}

		static Type jsonTypeOf(String name) {
			return new JsonType(name);
		}

		static Set<Type> jsonTypes() {
			return JSON_TYPES;
		}

		static Set<Type> bsonTypes() {
			return BSON_TYPES;
		}

		String representation();

		Object value();

		@RequiredArgsConstructor
		class JsonType implements Type {

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
		class BsonType implements Type {

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

		class EnumType implements Type {

			private final Object[] values;

			private EnumType(Object[] values) {
				this.values = values;
			}

			@Override
			public String representation() {
				return "enum";
			}

			@Override
			public Object[] value() {
				return values;
			}
		}

	}
}
