/*
 * Copyright 2018-2023 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.bson.Document;

import org.springframework.data.domain.Range;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.ArrayJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.BooleanJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.DateJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.NullJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.NumericJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.ObjectJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.StringJsonSchemaObject;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject.TimestampJsonSchemaObject;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * {@link JsonSchemaProperty} implementation.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
public class IdentifiableJsonSchemaProperty<T extends JsonSchemaObject> implements JsonSchemaProperty {

	protected final String identifier;
	protected final T jsonSchemaObjectDelegate;

	/**
	 * Creates a new {@link IdentifiableJsonSchemaProperty} for {@code identifier} and {@code jsonSchemaObject}.
	 *
	 * @param identifier must not be {@literal null}.
	 * @param jsonSchemaObject must not be {@literal null}.
	 */
	IdentifiableJsonSchemaProperty(String identifier, T jsonSchemaObject) {

		Assert.notNull(identifier, "Identifier must not be null");
		Assert.notNull(jsonSchemaObject, "JsonSchemaObject must not be null");

		this.identifier = identifier;
		this.jsonSchemaObjectDelegate = jsonSchemaObject;
	}

	@Override
	public String getIdentifier() {
		return identifier;
	}

	@Override
	public Document toDocument() {
		return new Document(identifier, jsonSchemaObjectDelegate.toDocument());
	}

	@Override
	public Set<Type> getTypes() {
		return jsonSchemaObjectDelegate.getTypes();
	}

	/**
	 * Convenience {@link JsonSchemaProperty} implementation without a {@code type} property.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class UntypedJsonSchemaProperty extends IdentifiableJsonSchemaProperty<UntypedJsonSchemaObject> {

		UntypedJsonSchemaProperty(String identifier, UntypedJsonSchemaObject jsonSchemaObject) {
			super(identifier, jsonSchemaObject);
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#possibleValues(Collection)
		 */
		public UntypedJsonSchemaProperty possibleValues(Object... possibleValues) {
			return possibleValues(Arrays.asList(possibleValues));
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#allOf(Collection)
		 */
		public UntypedJsonSchemaProperty allOf(JsonSchemaObject... allOf) {
			return allOf(new LinkedHashSet<>(Arrays.asList(allOf)));
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#anyOf(Collection)
		 */
		public UntypedJsonSchemaProperty anyOf(JsonSchemaObject... anyOf) {
			return anyOf(new LinkedHashSet<>(Arrays.asList(anyOf)));
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#oneOf(Collection)
		 */
		public UntypedJsonSchemaProperty oneOf(JsonSchemaObject... oneOf) {
			return oneOf(new LinkedHashSet<>(Arrays.asList(oneOf)));
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#possibleValues(Collection)
		 */
		public UntypedJsonSchemaProperty possibleValues(Collection<Object> possibleValues) {
			return new UntypedJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.possibleValues(possibleValues));
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#allOf(Collection)
		 */
		public UntypedJsonSchemaProperty allOf(Collection<JsonSchemaObject> allOf) {
			return new UntypedJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.allOf(allOf));
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#anyOf(Collection)
		 */
		public UntypedJsonSchemaProperty anyOf(Collection<JsonSchemaObject> anyOf) {
			return new UntypedJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.anyOf(anyOf));
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#oneOf(Collection)
		 */
		public UntypedJsonSchemaProperty oneOf(Collection<JsonSchemaObject> oneOf) {
			return new UntypedJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.oneOf(oneOf));
		}

		/**
		 * @param notMatch must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#notMatch(JsonSchemaObject)
		 */
		public UntypedJsonSchemaProperty notMatch(JsonSchemaObject notMatch) {
			return new UntypedJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.notMatch(notMatch));
		}

		/**
		 * @param description must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#description(String)
		 */
		public UntypedJsonSchemaProperty description(String description) {
			return new UntypedJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.description(description));
		}

		/**
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#generateDescription()
		 */
		public UntypedJsonSchemaProperty generatedDescription() {
			return new UntypedJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.generatedDescription());
		}
	}

	/**
	 * Convenience {@link JsonSchemaProperty} implementation for a {@code type : 'string'} property.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class StringJsonSchemaProperty extends IdentifiableJsonSchemaProperty<StringJsonSchemaObject> {

		/**
		 * @param identifier identifier the {@literal property} name or {@literal patternProperty} regex. Must not be
		 *          {@literal null} nor {@literal empty}.
		 * @param schemaObject must not be {@literal null}.
		 */
		StringJsonSchemaProperty(String identifier, StringJsonSchemaObject schemaObject) {
			super(identifier, schemaObject);
		}

		/**
		 * @param length
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#minLength(int)
		 */
		public StringJsonSchemaProperty minLength(int length) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.minLength(length));
		}

		/**
		 * @param length
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#maxLength(int)
		 */
		public StringJsonSchemaProperty maxLength(int length) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.maxLength(length));
		}

		/**
		 * @param pattern must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#matching(String)
		 */
		public StringJsonSchemaProperty matching(String pattern) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.matching(pattern));
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#possibleValues(Collection)
		 */
		public StringJsonSchemaProperty possibleValues(String... possibleValues) {
			return possibleValues(Arrays.asList(possibleValues));
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#allOf(Collection)
		 */
		public StringJsonSchemaProperty allOf(JsonSchemaObject... allOf) {
			return allOf(new LinkedHashSet<>(Arrays.asList(allOf)));
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#anyOf(Collection)
		 */
		public StringJsonSchemaProperty anyOf(JsonSchemaObject... anyOf) {
			return anyOf(new LinkedHashSet<>(Arrays.asList(anyOf)));
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#oneOf(Collection)
		 */
		public StringJsonSchemaProperty oneOf(JsonSchemaObject... oneOf) {
			return oneOf(new LinkedHashSet<>(Arrays.asList(oneOf)));
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#possibleValues(Collection)
		 */
		public StringJsonSchemaProperty possibleValues(Collection<String> possibleValues) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.possibleValues(possibleValues));
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#allOf(Collection)
		 */
		public StringJsonSchemaProperty allOf(Collection<JsonSchemaObject> allOf) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.allOf(allOf));
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#anyOf(Collection)
		 */
		public StringJsonSchemaProperty anyOf(Collection<JsonSchemaObject> anyOf) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.anyOf(anyOf));
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#oneOf(Collection)
		 */
		public StringJsonSchemaProperty oneOf(Collection<JsonSchemaObject> oneOf) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.oneOf(oneOf));
		}

		/**
		 * @param notMatch must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#notMatch(JsonSchemaObject)
		 */
		public StringJsonSchemaProperty notMatch(JsonSchemaObject notMatch) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.notMatch(notMatch));
		}

		/**
		 * @param description must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#description(String)
		 */
		public StringJsonSchemaProperty description(String description) {
			return new StringJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.description(description));
		}

		/**
		 * @return new instance of {@link StringJsonSchemaProperty}.
		 * @see StringJsonSchemaObject#generateDescription()
		 */
		public StringJsonSchemaProperty generatedDescription() {
			return new StringJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.generatedDescription());
		}
	}

	/**
	 * Convenience {@link JsonSchemaProperty} implementation for a {@code type : 'object'} property.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class ObjectJsonSchemaProperty extends IdentifiableJsonSchemaProperty<ObjectJsonSchemaObject> {

		/**
		 * @param identifier identifier the {@literal property} name or {@literal patternProperty} regex. Must not be
		 *          {@literal null} nor {@literal empty}.
		 * @param schemaObject must not be {@literal null}.
		 */
		ObjectJsonSchemaProperty(String identifier, ObjectJsonSchemaObject schemaObject) {
			super(identifier, schemaObject);
		}

		/**
		 * @param range must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 */
		public ObjectJsonSchemaProperty propertiesCount(Range<Integer> range) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.propertiesCount(range));
		}

		/**
		 * @param count must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#minProperties(int)
		 */
		public ObjectJsonSchemaProperty minProperties(int count) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.minProperties(count));
		}

		/**
		 * @param count must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#maxProperties(int)
		 */
		public ObjectJsonSchemaProperty maxProperties(int count) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.maxProperties(count));
		}

		/**
		 * @param properties must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#required(String...)
		 */
		public ObjectJsonSchemaProperty required(String... properties) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.required(properties));
		}

		/**
		 * @param additionalPropertiesAllowed
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#additionalProperties(boolean)
		 */
		public ObjectJsonSchemaProperty additionalProperties(boolean additionalPropertiesAllowed) {
			return new ObjectJsonSchemaProperty(identifier,
					jsonSchemaObjectDelegate.additionalProperties(additionalPropertiesAllowed));
		}

		/**
		 * @param additionalProperties must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#additionalProperties(ObjectJsonSchemaObject)
		 */
		public ObjectJsonSchemaProperty additionalProperties(ObjectJsonSchemaObject additionalProperties) {
			return new ObjectJsonSchemaProperty(identifier,
					jsonSchemaObjectDelegate.additionalProperties(additionalProperties));
		}

		/**
		 * @param properties must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#properties(JsonSchemaProperty...)
		 */
		public ObjectJsonSchemaProperty properties(JsonSchemaProperty... properties) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.properties(properties));
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#possibleValues(Collection)
		 */
		public ObjectJsonSchemaProperty possibleValues(Object... possibleValues) {
			return possibleValues(Arrays.asList(possibleValues));
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#allOf(Collection)
		 */
		public ObjectJsonSchemaProperty allOf(JsonSchemaObject... allOf) {
			return allOf(new LinkedHashSet<>(Arrays.asList(allOf)));
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#anyOf(Collection)
		 */
		public ObjectJsonSchemaProperty anyOf(JsonSchemaObject... anyOf) {
			return anyOf(new LinkedHashSet<>(Arrays.asList(anyOf)));
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#oneOf(Collection)
		 */
		public ObjectJsonSchemaProperty oneOf(JsonSchemaObject... oneOf) {
			return oneOf(new LinkedHashSet<>(Arrays.asList(oneOf)));
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#possibleValues(Collection)
		 */
		public ObjectJsonSchemaProperty possibleValues(Collection<Object> possibleValues) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.possibleValues(possibleValues));
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#allOf(Collection)
		 */
		public ObjectJsonSchemaProperty allOf(Collection<JsonSchemaObject> allOf) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.allOf(allOf));
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#anyOf(Collection)
		 */
		public ObjectJsonSchemaProperty anyOf(Collection<JsonSchemaObject> anyOf) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.anyOf(anyOf));
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#oneOf(Collection)
		 */
		public ObjectJsonSchemaProperty oneOf(Collection<JsonSchemaObject> oneOf) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.oneOf(oneOf));
		}

		/**
		 * @param notMatch must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#notMatch(JsonSchemaObject)
		 */
		public ObjectJsonSchemaProperty notMatch(JsonSchemaObject notMatch) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.notMatch(notMatch));
		}

		/**
		 * @param description must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#description(String)
		 */
		public ObjectJsonSchemaProperty description(String description) {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.description(description));
		}

		/**
		 * @return new instance of {@link ObjectJsonSchemaProperty}.
		 * @see ObjectJsonSchemaObject#generateDescription()
		 */
		public ObjectJsonSchemaProperty generatedDescription() {
			return new ObjectJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.generatedDescription());
		}

		public List<JsonSchemaProperty> getProperties() {
			return jsonSchemaObjectDelegate.getProperties();
		}
	}

	/**
	 * Convenience {@link JsonSchemaProperty} implementation for a {@code type : 'number'} property.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class NumericJsonSchemaProperty extends IdentifiableJsonSchemaProperty<NumericJsonSchemaObject> {

		/**
		 * @param identifier identifier the {@literal property} name or {@literal patternProperty} regex. Must not be
		 *          {@literal null} nor {@literal empty}.
		 * @param schemaObject must not be {@literal null}.
		 */
		public NumericJsonSchemaProperty(String identifier, NumericJsonSchemaObject schemaObject) {
			super(identifier, schemaObject);
		}

		/**
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#multipleOf
		 */
		public NumericJsonSchemaProperty multipleOf(Number value) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.multipleOf(value));
		}

		/**
		 * @param range must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#within(Range)
		 */
		public NumericJsonSchemaProperty within(Range<? extends Number> range) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.within(range));
		}

		/**
		 * @param min must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#gt(Number)
		 */
		public NumericJsonSchemaProperty gt(Number min) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.gt(min));
		}

		/**
		 * @param min must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#gte(Number)
		 */
		public NumericJsonSchemaProperty gte(Number min) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.gte(min));
		}

		/**
		 * @param max must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#lt(Number)
		 */
		public NumericJsonSchemaProperty lt(Number max) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.lt(max));
		}

		/**
		 * @param max must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#lte(Number)
		 */
		public NumericJsonSchemaProperty lte(Number max) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.lte(max));
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#possibleValues(Collection)
		 */
		public NumericJsonSchemaProperty possibleValues(Number... possibleValues) {
			return possibleValues(new LinkedHashSet<>(Arrays.asList(possibleValues)));
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#allOf(Collection)
		 */
		public NumericJsonSchemaProperty allOf(JsonSchemaObject... allOf) {
			return allOf(Arrays.asList(allOf));
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#anyOf(Collection)
		 */
		public NumericJsonSchemaProperty anyOf(JsonSchemaObject... anyOf) {
			return anyOf(new LinkedHashSet<>(Arrays.asList(anyOf)));
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#oneOf(Collection)
		 */
		public NumericJsonSchemaProperty oneOf(JsonSchemaObject... oneOf) {
			return oneOf(new LinkedHashSet<>(Arrays.asList(oneOf)));
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#possibleValues(Collection)
		 */
		public NumericJsonSchemaProperty possibleValues(Collection<Number> possibleValues) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.possibleValues(possibleValues));
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#allOf(Collection)
		 */
		public NumericJsonSchemaProperty allOf(Collection<JsonSchemaObject> allOf) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.allOf(allOf));
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#anyOf(Collection)
		 */
		public NumericJsonSchemaProperty anyOf(Collection<JsonSchemaObject> anyOf) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.anyOf(anyOf));
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#oneOf(Collection)
		 */
		public NumericJsonSchemaProperty oneOf(Collection<JsonSchemaObject> oneOf) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.oneOf(oneOf));
		}

		/**
		 * @param notMatch must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#notMatch(JsonSchemaObject)
		 */
		public NumericJsonSchemaProperty notMatch(JsonSchemaObject notMatch) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.notMatch(notMatch));
		}

		/**
		 * @param description must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#description(String)
		 */
		public NumericJsonSchemaProperty description(String description) {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.description(description));
		}

		/**
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see NumericJsonSchemaObject#generateDescription()
		 */
		public NumericJsonSchemaProperty generatedDescription() {
			return new NumericJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.generatedDescription());
		}
	}

	/**
	 * Convenience {@link JsonSchemaProperty} implementation for a {@code type : 'array'} property.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class ArrayJsonSchemaProperty extends IdentifiableJsonSchemaProperty<ArrayJsonSchemaObject> {

		/**
		 * @param identifier identifier the {@literal property} name or {@literal patternProperty} regex. Must not be
		 *          {@literal null} nor {@literal empty}.
		 * @param schemaObject must not be {@literal null}.
		 */
		public ArrayJsonSchemaProperty(String identifier, ArrayJsonSchemaObject schemaObject) {
			super(identifier, schemaObject);
		}

		/**
		 * @param uniqueItems
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#uniqueItems(boolean)
		 */
		public ArrayJsonSchemaProperty uniqueItems(boolean uniqueItems) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.uniqueItems(uniqueItems));
		}

		/**
		 * @param range must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#range(Range)
		 */
		public ArrayJsonSchemaProperty range(Range<Integer> range) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.range(range));
		}

		/**
		 * @param count
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#minItems(int)
		 */
		public ArrayJsonSchemaProperty minItems(int count) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.minItems(count));
		}

		/**
		 * @param count
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#maxItems(int)
		 */
		public ArrayJsonSchemaProperty maxItems(int count) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.maxItems(count));
		}

		/**
		 * @param items must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#items(Collection)
		 */
		public ArrayJsonSchemaProperty items(JsonSchemaObject... items) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.items(Arrays.asList(items)));
		}

		/**
		 * @param items must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#items(Collection)
		 */
		public ArrayJsonSchemaProperty items(Collection<JsonSchemaObject> items) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.items(items));
		}

		/**
		 * @param additionalItemsAllowed
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#additionalItems(boolean)
		 */
		public ArrayJsonSchemaProperty additionalItems(boolean additionalItemsAllowed) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.additionalItems(additionalItemsAllowed));
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#possibleValues(Collection)
		 */
		public ArrayJsonSchemaProperty possibleValues(Object... possibleValues) {
			return possibleValues(new LinkedHashSet<>(Arrays.asList(possibleValues)));
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#allOf(Collection)
		 */
		public ArrayJsonSchemaProperty allOf(JsonSchemaObject... allOf) {
			return allOf(new LinkedHashSet<>(Arrays.asList(allOf)));
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#anyOf(Collection)
		 */
		public ArrayJsonSchemaProperty anyOf(JsonSchemaObject... anyOf) {
			return anyOf(new LinkedHashSet<>(Arrays.asList(anyOf)));
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#oneOf(Collection)
		 */
		public ArrayJsonSchemaProperty oneOf(JsonSchemaObject... oneOf) {
			return oneOf(new LinkedHashSet<>(Arrays.asList(oneOf)));
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#possibleValues(Collection)
		 */
		public ArrayJsonSchemaProperty possibleValues(Collection<Object> possibleValues) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.possibleValues(possibleValues));
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#allOf(Collection)
		 */
		public ArrayJsonSchemaProperty allOf(Collection<JsonSchemaObject> allOf) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.allOf(allOf));
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#anyOf(Collection)
		 */
		public ArrayJsonSchemaProperty anyOf(Collection<JsonSchemaObject> anyOf) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.anyOf(anyOf));
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#oneOf(Collection)
		 */
		public ArrayJsonSchemaProperty oneOf(Collection<JsonSchemaObject> oneOf) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.oneOf(oneOf));
		}

		/**
		 * @param notMatch must not be {@literal null}.
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#notMatch(JsonSchemaObject)
		 */
		public ArrayJsonSchemaProperty notMatch(JsonSchemaObject notMatch) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.notMatch(notMatch));
		}

		/**
		 * @param description must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#description(String)
		 */
		public ArrayJsonSchemaProperty description(String description) {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.description(description));
		}

		/**
		 * @return new instance of {@link ArrayJsonSchemaProperty}.
		 * @see ArrayJsonSchemaObject#generateDescription()
		 */
		public ArrayJsonSchemaProperty generatedDescription() {
			return new ArrayJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.generatedDescription());
		}
	}

	/**
	 * Convenience {@link JsonSchemaProperty} implementation for a {@code type : 'boolean'} property.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class BooleanJsonSchemaProperty extends IdentifiableJsonSchemaProperty<BooleanJsonSchemaObject> {

		BooleanJsonSchemaProperty(String identifier, BooleanJsonSchemaObject schemaObject) {
			super(identifier, schemaObject);
		}

		/**
		 * @param description must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaProperty}.
		 * @see BooleanJsonSchemaObject#description(String)
		 */
		public BooleanJsonSchemaProperty description(String description) {
			return new BooleanJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.description(description));
		}

		/**
		 * @return new instance of {@link BooleanJsonSchemaProperty}.
		 * @see BooleanJsonSchemaObject#generateDescription()
		 */
		public BooleanJsonSchemaProperty generatedDescription() {
			return new BooleanJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.generatedDescription());
		}
	}

	/**
	 * Convenience {@link JsonSchemaProperty} implementation for a {@code type : 'null'} property.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class NullJsonSchemaProperty extends IdentifiableJsonSchemaProperty<NullJsonSchemaObject> {

		NullJsonSchemaProperty(String identifier, NullJsonSchemaObject schemaObject) {
			super(identifier, schemaObject);
		}

		/**
		 * @param description must not be {@literal null}.
		 * @return new instance of {@link NullJsonSchemaProperty}.
		 * @see NullJsonSchemaObject#description(String)
		 */
		public NullJsonSchemaProperty description(String description) {
			return new NullJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.description(description));
		}

		/**
		 * @return new instance of {@link NullJsonSchemaProperty}.
		 * @see NullJsonSchemaObject#generateDescription()
		 */
		public NullJsonSchemaProperty generatedDescription() {
			return new NullJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.generatedDescription());
		}
	}

	/**
	 * Convenience {@link JsonSchemaProperty} implementation for a {@code type : 'date'} property.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class DateJsonSchemaProperty extends IdentifiableJsonSchemaProperty<DateJsonSchemaObject> {

		DateJsonSchemaProperty(String identifier, DateJsonSchemaObject schemaObject) {
			super(identifier, schemaObject);
		}

		/**
		 * @param description must not be {@literal null}.
		 * @return new instance of {@link DateJsonSchemaProperty}.
		 * @see DateJsonSchemaProperty#description(String)
		 */
		public DateJsonSchemaProperty description(String description) {
			return new DateJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.description(description));
		}

		/**
		 * @return new instance of {@link DateJsonSchemaProperty}.
		 * @see DateJsonSchemaProperty#generatedDescription()
		 */
		public DateJsonSchemaProperty generatedDescription() {
			return new DateJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.generatedDescription());
		}
	}

	/**
	 * Convenience {@link JsonSchemaProperty} implementation for a {@code type : 'timestamp'} property.
	 *
	 * @author Mark Paluch
	 * @since 2.1
	 */
	public static class TimestampJsonSchemaProperty extends IdentifiableJsonSchemaProperty<TimestampJsonSchemaObject> {

		TimestampJsonSchemaProperty(String identifier, TimestampJsonSchemaObject schemaObject) {
			super(identifier, schemaObject);
		}

		/**
		 * @param description must not be {@literal null}.
		 * @return new instance of {@link TimestampJsonSchemaProperty}.
		 * @see TimestampJsonSchemaProperty#description(String)
		 */
		public TimestampJsonSchemaProperty description(String description) {
			return new TimestampJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.description(description));
		}

		/**
		 * @return new instance of {@link TimestampJsonSchemaProperty}.
		 * @see TimestampJsonSchemaProperty#generatedDescription()
		 */
		public TimestampJsonSchemaProperty generatedDescription() {
			return new TimestampJsonSchemaProperty(identifier, jsonSchemaObjectDelegate.generatedDescription());
		}
	}

	/**
	 * Delegating {@link JsonSchemaProperty} implementation having a {@literal required} flag for evaluation during schema
	 * creation process.
	 *
	 * @author Christoph Strobl
	 * @since 2.2
	 */
	public static class RequiredJsonSchemaProperty implements JsonSchemaProperty {

		private final JsonSchemaProperty delegate;
		private final boolean required;

		RequiredJsonSchemaProperty(JsonSchemaProperty delegate, boolean required) {

			this.delegate = delegate;
			this.required = required;
		}

		@Override
		public String getIdentifier() {
			return delegate.getIdentifier();
		}

		@Override
		public Set<Type> getTypes() {
			return delegate.getTypes();
		}

		@Override
		public Document toDocument() {
			return delegate.toDocument();
		}

		@Override
		public boolean isRequired() {
			return required;
		}
	}

	/**
	 * {@link JsonSchemaProperty} implementation for encrypted fields.
	 *
	 * @author Christoph Strobl
	 * @since 2.2
	 */
	public static class EncryptedJsonSchemaProperty implements JsonSchemaProperty {

		private final JsonSchemaProperty targetProperty;
		private final @Nullable String algorithm;
		private final @Nullable String keyId;
		private final @Nullable List<?> keyIds;

		/**
		 * Create new instance of {@link EncryptedJsonSchemaProperty} wrapping the given {@link JsonSchemaProperty target}.
		 *
		 * @param target must not be {@literal null}.
		 */
		public EncryptedJsonSchemaProperty(JsonSchemaProperty target) {
			this(target, null, null, null);
		}

		private EncryptedJsonSchemaProperty(JsonSchemaProperty target, @Nullable String algorithm, @Nullable String keyId,
				@Nullable List<?> keyIds) {

			Assert.notNull(target, "Target must not be null");
			this.targetProperty = target;
			this.algorithm = algorithm;
			this.keyId = keyId;
			this.keyIds = keyIds;
		}

		/**
		 * Create new instance of {@link EncryptedJsonSchemaProperty} wrapping the given {@link JsonSchemaProperty target}.
		 *
		 * @param target must not be {@literal null}.
		 * @return new instance of {@link EncryptedJsonSchemaProperty}.
		 */
		public static EncryptedJsonSchemaProperty encrypted(JsonSchemaProperty target) {
			return new EncryptedJsonSchemaProperty(target);
		}

		/**
		 * Use {@literal AEAD_AES_256_CBC_HMAC_SHA_512-Random} algorithm.
		 *
		 * @return new instance of {@link EncryptedJsonSchemaProperty}.
		 */
		public EncryptedJsonSchemaProperty aead_aes_256_cbc_hmac_sha_512_random() {
			return algorithm("AEAD_AES_256_CBC_HMAC_SHA_512-Random");
		}

		/**
		 * Use {@literal AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic} algorithm.
		 *
		 * @return new instance of {@link EncryptedJsonSchemaProperty}.
		 */
		public EncryptedJsonSchemaProperty aead_aes_256_cbc_hmac_sha_512_deterministic() {
			return algorithm("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic");
		}

		/**
		 * Use the given algorithm identified via its name.
		 *
		 * @return new instance of {@link EncryptedJsonSchemaProperty}.
		 */
		public EncryptedJsonSchemaProperty algorithm(String algorithm) {
			return new EncryptedJsonSchemaProperty(targetProperty, algorithm, keyId, keyIds);
		}

		/**
		 * @param keyId must not be {@literal null}.
		 * @return new instance of {@link EncryptedJsonSchemaProperty}.
		 */
		public EncryptedJsonSchemaProperty keyId(String keyId) {
			return new EncryptedJsonSchemaProperty(targetProperty, algorithm, keyId, null);
		}

		/**
		 * @param keyId must not be {@literal null}.
		 * @return new instance of {@link EncryptedJsonSchemaProperty}.
		 */
		public EncryptedJsonSchemaProperty keys(UUID... keyId) {
			return new EncryptedJsonSchemaProperty(targetProperty, algorithm, null, Arrays.asList(keyId));
		}

		/**
		 * @param keyId must not be {@literal null}.
		 * @return new instance of {@link EncryptedJsonSchemaProperty}.
		 */
		public EncryptedJsonSchemaProperty keys(Object... keyId) {
			return new EncryptedJsonSchemaProperty(targetProperty, algorithm, null, Arrays.asList(keyId));
		}

		@Override
		public Document toDocument() {

			Document doc = targetProperty.toDocument();
			Document propertySpecification = doc.get(targetProperty.getIdentifier(), Document.class);

			Document enc = new Document();

			if (!ObjectUtils.isEmpty(keyId)) {
				enc.append("keyId", keyId);
			} else if (!ObjectUtils.isEmpty(keyIds)) {
				enc.append("keyId", keyIds);
			}

			Type type = extractPropertyType(propertySpecification);
			if (type != null) {

				propertySpecification.remove(type.representation());
				enc.append("bsonType", type.toBsonType().value()); // TODO: no samples with type -> is it bson type all the way?
			}

			if (StringUtils.hasText(algorithm)) {
				enc.append("algorithm", algorithm);
			}

			propertySpecification.append("encrypt", enc);

			return doc;
		}

		@Override
		public String getIdentifier() {
			return targetProperty.getIdentifier();
		}

		@Override
		public Set<Type> getTypes() {
			return targetProperty.getTypes();
		}

		@Nullable
		private Type extractPropertyType(Document source) {

			if (source.containsKey("type")) {
				return Type.of(source.get("type", String.class));
			}
			if (source.containsKey("bsonType")) {
				return Type.of(source.get("bsonType", String.class));
			}

			return null;
		}
	}
}
