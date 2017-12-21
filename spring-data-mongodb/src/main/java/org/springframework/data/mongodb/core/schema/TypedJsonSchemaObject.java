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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
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
 * Common base for {@link JsonSchemaObject} with shared types and {@link JsonSchemaObject#toDocument()} implementation.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public class TypedJsonSchemaObject implements JsonSchemaObject {

	protected final Set<Type> types;
	protected final @Nullable String description;
	protected final Restrictions restrictions;

	/**
	 * @param type can be {@literal null}.
	 * @param description can be {@literal null}.
	 * @param restrictions can be {@literal null}.
	 */
	protected TypedJsonSchemaObject(@Nullable Type type, @Nullable String description,
			@Nullable Restrictions restrictions) {
		this(type != null ? Collections.singleton(type) : Collections.emptySet(), description, restrictions);
	}

	/**
	 * @param types must not be {@literal null}.
	 * @param description can be {@literal null}.
	 * @param restrictions can be {@literal null}. Defaults to {@link Restrictions#empty()}.
	 */
	protected TypedJsonSchemaObject(Set<Type> types, @Nullable String description, @Nullable Restrictions restrictions) {

		Assert.notNull(types, "Types must not be null! Please consider using 'Collections.emptySet()'.");

		this.types = types;
		this.description = description;
		this.restrictions = restrictions != null ? restrictions : Restrictions.empty();
	}

	/**
	 * Creates new {@link TypedJsonSchemaObject} of given types.
	 * 
	 * @param types must not be {@literal null}.
	 * @return
	 */
	public static TypedJsonSchemaObject of(Type... types) {

		Assert.noNullElements(types, "Types must not contain null!");
		return new TypedJsonSchemaObject(new LinkedHashSet<>(Arrays.asList(types)), null, Restrictions.empty());
	}

	/**
	 * Set the {@literal description}.
	 *
	 * @param description must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public TypedJsonSchemaObject description(String description) {
		return new TypedJsonSchemaObject(types, description, restrictions);
	}

	/**
	 * {@literal enum}erates all possible values of the field.
	 *
	 * @param possibleValues must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public TypedJsonSchemaObject possibleValues(Collection<Object> possibleValues) {
		return new TypedJsonSchemaObject(types, description, restrictions.possibleValues(possibleValues));
	}

	/**
	 * The field value must match all specified schemas.
	 *
	 * @param allOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public TypedJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
		return new TypedJsonSchemaObject(types, description, restrictions.allOf(allOf));
	}

	/**
	 * The field value must match at least one of the specified schemas.
	 *
	 * @param anyOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public TypedJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
		return new TypedJsonSchemaObject(types, description, restrictions.anyOf(anyOf));
	}

	/**
	 * The field value must match exactly one of the specified schemas.
	 *
	 * @param oneOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public TypedJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
		return new TypedJsonSchemaObject(types, description, restrictions.oneOf(oneOf));
	}

	/**
	 * The field value must not match the specified schemas.
	 *
	 * @param oneOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	public TypedJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
		return new TypedJsonSchemaObject(types, description, restrictions.notMatch(notMatch));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.schema.JsonSchemaObject#getTypes()
	 */
	@Override
	public Set<Type> getTypes() {
		return types;
	}

	/**
	 * Create the json schema complying {@link Document} representation. This includes {@literal type},
	 * {@literal description} and the fields of {@link Restrictions#toDocument()} if set.
	 */
	@Override
	public Document toDocument() {

		Document document = new Document();

		if (!CollectionUtils.isEmpty(types)) {

			Type theType = types.iterator().next();
			if (types.size() == 1) {
				document.append(theType.representation(), theType.value());
			} else {
				document.append(theType.representation(), types.stream().map(Type::value).collect(Collectors.toList()));
			}
		}

		getOrCreateDescription().ifPresent(val -> document.append("description", val));

		if (restrictions != null) {
			document.putAll(restrictions.toDocument());
		}

		return document;
	}

	private Optional<String> getOrCreateDescription() {

		if (StringUtils.hasText(description)) {
			return Optional.of(description);
		}

		return Optional.ofNullable(generateDescription());
	}

	/**
	 * Customization hook for creating description out of defined values.<br />
	 * Called by {@link #toDocument()} when no explicit {@link #description} is set.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	protected String generateDescription() {
		return null;
	}

	/**
	 * {@link Restrictions} encapsulate common json schema restrictions like {@literal enum}, {@literal allOf}, ...
	 * 
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class Restrictions {

		private final Collection<Object> possibleValues;
		private final Collection<JsonSchemaObject> allOf;
		private final Collection<JsonSchemaObject> anyOf;
		private final Collection<JsonSchemaObject> oneOf;
		private final @Nullable JsonSchemaObject notMatch;

		Restrictions(Collection<Object> possibleValues, Collection<JsonSchemaObject> allOf,
				Collection<JsonSchemaObject> anyOf, Collection<JsonSchemaObject> oneOf, JsonSchemaObject notMatch) {

			this.possibleValues = possibleValues;
			this.allOf = allOf;
			this.anyOf = anyOf;
			this.oneOf = oneOf;
			this.notMatch = notMatch;
		}

		/**
		 * @return new empty {@link Restrictions}.
		 */
		static Restrictions empty() {

			return new Restrictions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
					Collections.emptySet(), null);
		}

		/**
		 * @param possibleValues must not be {@literal null}.
		 * @return
		 */
		Restrictions possibleValues(Collection<Object> possibleValues) {

			Assert.notNull(possibleValues, "PossibleValues must not be null!");
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		/**
		 * @param allOf must not be {@literal null}.
		 * @return
		 */
		Restrictions allOf(Collection<JsonSchemaObject> allOf) {

			Assert.notNull(allOf, "AllOf must not be null!");
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		/**
		 * @param anyOf must not be {@literal null}.
		 * @return
		 */
		Restrictions anyOf(Collection<JsonSchemaObject> anyOf) {

			Assert.notNull(anyOf, "AnyOf must not be null!");
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		/**
		 * @param oneOf must not be {@literal null}.
		 * @return
		 */
		Restrictions oneOf(Collection<JsonSchemaObject> oneOf) {

			Assert.notNull(oneOf, "OneOf must not be null!");
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		/**
		 * @param notMatch must not be {@literal null}.
		 * @return
		 */
		Restrictions notMatch(JsonSchemaObject notMatch) {

			Assert.notNull(notMatch, "NotMatch must not be null!");
			return new Restrictions(possibleValues, allOf, anyOf, oneOf, notMatch);
		}

		/**
		 * Create the json schema complying {@link Document} representation. This includes {@literal enum},
		 * {@literal allOf}, {@literal anyOf}, {@literal oneOf}, {@literal notMatch} if set.
		 *
		 * @return never {@literal null}
		 */
		Document toDocument() {

			Document document = new Document();

			if (!CollectionUtils.isEmpty(possibleValues)) {
				document.append("enum", possibleValues);
			}
			if (!CollectionUtils.isEmpty(allOf)) {
				document.append("allOf", allOf.stream().map(JsonSchemaObject::toDocument).collect(Collectors.toList()));
			}
			if (!CollectionUtils.isEmpty(anyOf)) {
				document.append("anyOf", anyOf.stream().map(JsonSchemaObject::toDocument).collect(Collectors.toList()));
			}
			if (!CollectionUtils.isEmpty(oneOf)) {
				document.append("oneOf", oneOf.stream().map(JsonSchemaObject::toDocument).collect(Collectors.toList()));
			}
			if (notMatch != null) {
				document.append("notMatch", notMatch.toDocument());
			}

			return document;
		}
	}

	/**
	 * {@link JsonSchemaObject} implementation of {@code type : 'object'} schema elements.<br />
	 * Provides programmatic access to schema specifics like {@literal required, properties, patternProperties,...} via a
	 * fluent API producing immutable {@link JsonSchemaObject schema objects}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class ObjectJsonSchemaObject extends TypedJsonSchemaObject {

		private @Nullable Range<Integer> nrProperties;
		private @Nullable Object additionalProperties;
		private List<String> requiredProperties = Collections.emptyList();
		private List<JsonSchemaProperty> properties = Collections.emptyList();
		private List<JsonSchemaProperty> patternProperties = Collections.emptyList();

		public ObjectJsonSchemaObject() {
			this(null, null);
		}

		/**
		 * @param description can be {@literal null}.
		 * @param restrictions can be {@literal null};
		 */
		ObjectJsonSchemaObject(@Nullable String description, @Nullable Restrictions restrictions) {
			super(Type.objectType(), description, restrictions);
		}

		/**
		 * Define the {@literal minProperties} and {@literal maxProperties} via the given {@link Range}.<br />
		 * In-/Exclusions via {@link Bound#isInclusive() range bounds} are not taken into account.
		 *
		 * @param range must not be {@literal null}. Consider {@link Range#unbounded()} instead.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject nrProperties(Range<Integer> range) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.nrProperties = range;
			return newInstance;
		}

		/**
		 * Define the {@literal minProperties}.
		 *
		 * @param nrProperties the allowed minimal number of properties.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject minNrProperties(int nrProperties) {

			Bound upper = this.nrProperties != null ? this.nrProperties.getUpperBound() : Bound.unbounded();
			return nrProperties(Range.of(Bound.inclusive(nrProperties), upper));
		}

		/**
		 * Define the {@literal maxProperties}.
		 *
		 * @param nrProperties the allowed maximum number of properties.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject maxNrProperties(int nrProperties) {

			Bound lower = this.nrProperties != null ? this.nrProperties.getLowerBound() : Bound.unbounded();
			return nrProperties(Range.of(lower, Bound.inclusive(nrProperties)));
		}

		/**
		 * Define the Object’s {@literal required} properties.
		 *
		 * @param properties the names of required properties.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject required(String... properties) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.requiredProperties = new ArrayList<>(this.requiredProperties.size() + properties.length);
			newInstance.requiredProperties.addAll(this.requiredProperties);
			newInstance.requiredProperties.addAll(Arrays.asList(properties));
			return newInstance;
		}

		/**
		 * If set to {@literal false}, no additional fields are allowed.
		 *
		 * @param additionalPropertiesAllowed
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject additionalProperties(boolean additionalPropertiesAllowed) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.additionalProperties = additionalPropertiesAllowed;
			return newInstance;
		}

		/**
		 * If specified, additional fields must validate against the given schema.
		 *
		 * @param schema must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject additionalProperties(ObjectJsonSchemaObject schema) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.additionalProperties = schema;
			return newInstance;
		}

		/**
		 * Append the objects properties along with the {@link JsonSchemaObject} validating against.
		 *
		 * @param properties must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject properties(JsonSchemaProperty... properties) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.properties = new ArrayList<>(this.properties.size() + properties.length);
			newInstance.properties.addAll(this.properties);
			newInstance.properties.addAll(Arrays.asList(properties));
			return newInstance;
		}

		/**
		 * Append regular expression patterns along with the {@link JsonSchemaObject} matching properties validating
		 * against.
		 *
		 * @param regularExpressions must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject patternProperties(JsonSchemaProperty... regularExpressions) {

			ObjectJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.patternProperties = new ArrayList<>(this.patternProperties.size() + regularExpressions.length);
			newInstance.patternProperties.addAll(this.patternProperties);
			newInstance.patternProperties.addAll(Arrays.asList(regularExpressions));
			return newInstance;
		}

		/**
		 * Append the objects property along with the {@link JsonSchemaObject} validating against.
		 *
		 * @param properties must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject property(JsonSchemaProperty property) {
			return properties(property);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#possibleValues(java.util.Collection)
		 */
		@Override
		public ObjectJsonSchemaObject possibleValues(Collection<Object> possibleValues) {
			return newInstance(description, restrictions.possibleValues(possibleValues));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#allOf(java.util.Collection)
		 */
		@Override
		public ObjectJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return newInstance(description, restrictions.allOf(allOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#anyOf(java.util.Collection)
		 */
		@Override
		public ObjectJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return newInstance(description, restrictions.anyOf(anyOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#oneOf(java.util.Collection)
		 */
		@Override
		public ObjectJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return newInstance(description, restrictions.oneOf(oneOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#notMatch(org.springframework.data.mongodb.core.schema.JsonSchemaObject)
		 */
		@Override
		public ObjectJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return newInstance(description, restrictions.notMatch(notMatch));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#description(java.lang.String)
		 */
		@Override
		public ObjectJsonSchemaObject description(String description) {
			return newInstance(description, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.JsonSchemaObject#toDocument()
		 */
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

	/**
	 * {@link JsonSchemaObject} implementation of {@code type : 'number'}, {@code bsonType : 'int'},
	 * {@code bsonType : 'long'}, {@code bsonType : 'double'} and {@code bsonType : 'decimal128'} schema elements.<br />
	 * Provides programmatic access to schema specifics like {@literal multipleOf, minimum, maximum,...} via a fluent API
	 * producing immutable {@link JsonSchemaObject schema objects}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class NumericJsonSchemaObject extends TypedJsonSchemaObject {

		private static final Set<Type> NUMERIC_TYPES = new HashSet<>(
				Arrays.asList(Type.doubleType(), Type.intType(), Type.longType(), Type.numberType(), Type.bigintType()));

		@Nullable Number multipleOf;
		@Nullable Range<? extends Number> range;

		public NumericJsonSchemaObject() {
			this(Type.numberType());
		}

		public NumericJsonSchemaObject(Type type) {
			this(type, null);
		}

		private NumericJsonSchemaObject(Type type, @Nullable String description) {
			this(Collections.singleton(type), description, null);
		}

		private NumericJsonSchemaObject(Set<Type> types, @Nullable String description,
				@Nullable Restrictions restrictions) {

			super(validateTypes(types), description, restrictions);
		}

		/**
		 * Set the value a valid field value must be the multiple of.
		 *
		 * @param value must not be {@literal null}.
		 * @return must not be {@literal null}.
		 */
		NumericJsonSchemaObject multipleOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			NumericJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.multipleOf = value;
			return newInstance;
		}

		/**
		 * Set the {@link Range} of valid field values translating to {@literal minimum}, {@literal exclusiveMinimum},
		 * {@literal maximum} and {@literal exclusiveMaximum}.
		 *
		 * @param range must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaObject}.
		 */
		public NumericJsonSchemaObject within(Range<? extends Number> range) {

			Assert.notNull(range, "Range must not be null!");

			NumericJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.range = range;
			return newInstance;
		}

		/**
		 * Set {@literal minimum} to given {@code min} value and {@literal exclusiveMinimum} to {@literal true}.
		 *
		 * @param min must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaObject}.
		 */
		public NumericJsonSchemaObject gt(Number min) {

			Assert.notNull(min, "Min must not be null!");

			Bound upper = this.range != null ? this.range.getUpperBound() : Bound.unbounded();
			return within(Range.of(createBound(min, false), upper));
		}

		/**
		 * Set {@literal minimum} to given {@code min} value and {@literal exclusiveMinimum} to {@literal false}.
		 *
		 * @param min must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaObject}.
		 */
		public NumericJsonSchemaObject gte(Number min) {

			Assert.notNull(min, "Min must not be null!");

			Bound upper = this.range != null ? this.range.getUpperBound() : Bound.unbounded();
			return within(Range.of(createBound(min, true), upper));
		}

		/**
		 * Set {@literal maximum} to given {@code max} value and {@literal exclusiveMaximum} to {@literal true}.
		 *
		 * @param max must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaObject}.
		 */
		public NumericJsonSchemaObject lt(Number max) {

			Assert.notNull(max, "Max must not be null!");

			Bound lower = this.range != null ? this.range.getLowerBound() : Bound.unbounded();
			return within(Range.of(lower, createBound(max, false)));
		}

		/**
		 * Set {@literal maximum} to given {@code max} value and {@literal exclusiveMaximum} to {@literal false}.
		 *
		 * @param max must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaObject}.
		 */
		NumericJsonSchemaObject lte(Number max) {

			Assert.notNull(max, "Max must not be null!");

			Bound lower = this.range != null ? this.range.getLowerBound() : Bound.unbounded();
			return within(Range.of(lower, createBound(max, true)));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#possibleValues(java.util.Collection)
		 */
		@Override
		public NumericJsonSchemaObject possibleValues(Collection<Object> possibleValues) {
			return newInstance(description, restrictions.possibleValues(possibleValues));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#allOf(java.util.Collection)
		 */
		@Override
		public NumericJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return newInstance(description, restrictions.allOf(allOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#anyOf(java.util.Collection)
		 */
		@Override
		public NumericJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return newInstance(description, restrictions.anyOf(anyOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#oneOf(java.util.Collection)
		 */
		@Override
		public NumericJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return newInstance(description, restrictions.oneOf(oneOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#notMatch(org.springframework.data.mongodb.core.schema.JsonSchemaObject)
		 */
		@Override
		public NumericJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return newInstance(description, restrictions.notMatch(notMatch));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#description(java.lang.String)
		 */
		@Override
		public NumericJsonSchemaObject description(String description) {
			return newInstance(description, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.JsonSchemaObject#toDocument()
		 */
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

		private Bound createBound(Number number, boolean inclusive) {

			if (number instanceof Long) {
				return inclusive ? Bound.inclusive((Long) number) : Bound.exclusive((Long) number);
			}
			if (number instanceof Double) {
				return inclusive ? Bound.inclusive((Double) number) : Bound.exclusive((Double) number);
			}
			if (number instanceof Float) {
				return inclusive ? Bound.inclusive((Float) number) : Bound.exclusive((Float) number);
			}
			if (number instanceof Integer) {
				return inclusive ? Bound.inclusive((Integer) number) : Bound.exclusive((Integer) number);
			}
			if (number instanceof BigDecimal) {
				return inclusive ? Bound.inclusive((BigDecimal) number) : Bound.exclusive((BigDecimal) number);
			}

			throw new IllegalArgumentException("Unsupported numeric value.");
		}

		private static Set<Type> validateTypes(Set<Type> types) {

			types.forEach(type -> {
				Assert.isTrue(NUMERIC_TYPES.contains(type),
						() -> String.format("%s is not a valid numeric type. Expected one of %s.", type, NUMERIC_TYPES));
			});

			return types;
		}
	}

	/**
	 * {@link JsonSchemaObject} implementation of {@code type : 'string'} schema elements.<br />
	 * Provides programmatic access to schema specifics like {@literal minLength, maxLength, pattern,...} via a fluent API
	 * producing immutable {@link JsonSchemaObject schema objects}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class StringJsonSchemaObject extends TypedJsonSchemaObject {

		@Nullable Range<Integer> length;
		@Nullable String pattern;

		public StringJsonSchemaObject() {
			this(null, null);
		}

		private StringJsonSchemaObject(@Nullable String description, @Nullable Restrictions restrictions) {
			super(Type.stringType(), description, restrictions);
		}

		/**
		 * Define the valid length range ({@literal minLength} and {@literal maxLength}) for a valid field.
		 *
		 * @param range must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaObject}.
		 */
		public StringJsonSchemaObject length(Range<Integer> range) {

			Assert.notNull(range, "Range must not be null!");

			StringJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.length = range;
			return newInstance;
		}

		/**
		 * Define the valid length range ({@literal minLength} for a valid field.
		 *
		 * @param length
		 * @return new instance of {@link StringJsonSchemaObject}.
		 */
		public StringJsonSchemaObject minLength(int length) {

			Bound upper = this.length != null ? this.length.getUpperBound() : Bound.unbounded();
			return length(Range.of(Bound.inclusive(length), upper));
		}

		/**
		 * Define the valid length range ({@literal maxLength} for a valid field.
		 *
		 * @param length
		 * @return new instance of {@link StringJsonSchemaObject}.
		 */
		public StringJsonSchemaObject maxLength(int length) {

			Bound lower = this.length != null ? this.length.getLowerBound() : Bound.unbounded();
			return length(Range.of(lower, Bound.inclusive(length)));
		}

		/**
		 * Define the regex pattern to validate field values against.
		 *
		 * @param pattern must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaObject}.
		 */
		public StringJsonSchemaObject matching(String pattern) {

			Assert.notNull(pattern, "Pattern must not be null!");

			StringJsonSchemaObject newInstance = newInstance(description, restrictions);
			newInstance.pattern = pattern;
			return newInstance;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#possibleValues(java.util.Collection)
		 */
		@Override
		public StringJsonSchemaObject possibleValues(Collection<Object> possibleValues) {
			return newInstance(description, restrictions.possibleValues(possibleValues));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#allOf(java.util.Collection)
		 */
		@Override
		public StringJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return newInstance(description, restrictions.allOf(allOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#anyOf(java.util.Collection)
		 */
		@Override
		public StringJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return newInstance(description, restrictions.anyOf(anyOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#oneOf(java.util.Collection)
		 */
		@Override
		public StringJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return newInstance(description, restrictions.oneOf(oneOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#notMatch(org.springframework.data.mongodb.core.schema.JsonSchemaObject)
		 */
		@Override
		public StringJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return newInstance(description, restrictions.notMatch(notMatch));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#description(java.lang.String)
		 */
		@Override
		public StringJsonSchemaObject description(String description) {
			return newInstance(description, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.JsonSchemaObject#toDocument()
		 */
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

		private StringJsonSchemaObject newInstance(String description, Restrictions restrictions) {

			StringJsonSchemaObject newInstance = new StringJsonSchemaObject(description, restrictions);

			newInstance.length = this.length;
			newInstance.pattern = this.pattern;
			return newInstance;
		}
	}

	static class ArrayJsonSchemaObject extends TypedJsonSchemaObject {

		private @Nullable Boolean uniqueItems;
		private @Nullable Range<Integer> range;
		private Collection<JsonSchemaObject> items;

		public ArrayJsonSchemaObject() {
			this(null, null);
		}

		private ArrayJsonSchemaObject(@Nullable String description, @Nullable Restrictions restrictions) {
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

	static class BooleanJsonSchemaObject extends TypedJsonSchemaObject {

		public BooleanJsonSchemaObject() {
			this(null, null);
		}

		private BooleanJsonSchemaObject(@Nullable String description, @Nullable Restrictions restrictions) {
			super(Type.booleanType(), description, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#possibleValues(java.util.Collection)
		 */
		@Override
		public BooleanJsonSchemaObject possibleValues(Collection<Object> possibleValues) {
			return new BooleanJsonSchemaObject(description, restrictions.possibleValues(possibleValues));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#allOf(java.util.Collection)
		 */
		@Override
		public BooleanJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return new BooleanJsonSchemaObject(description, restrictions.allOf(allOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#anyOf(java.util.Collection)
		 */
		@Override
		public BooleanJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return new BooleanJsonSchemaObject(description, restrictions.anyOf(anyOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#oneOf(java.util.Collection)
		 */
		@Override
		public BooleanJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return new BooleanJsonSchemaObject(description, restrictions.oneOf(oneOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#notMatch(org.springframework.data.mongodb.core.schema.JsonSchemaObject)
		 */
		@Override
		public BooleanJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return new BooleanJsonSchemaObject(description, restrictions.notMatch(notMatch));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#description(java.lang.String)
		 */
		@Override
		public BooleanJsonSchemaObject description(String description) {
			return new BooleanJsonSchemaObject(description, restrictions);
		}

	}

	static class NullJsonSchemaObject extends TypedJsonSchemaObject {

		public NullJsonSchemaObject() {
			this(null, null);
		}

		private NullJsonSchemaObject(@Nullable String description, @Nullable Restrictions restrictions) {
			super(Type.nullType(), description, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#possibleValues(java.util.Collection)
		 */
		@Override
		public NullJsonSchemaObject possibleValues(Collection<Object> possibleValues) {
			return new NullJsonSchemaObject(description, restrictions.possibleValues(possibleValues));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#allOf(java.util.Collection)
		 */
		@Override
		public NullJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return new NullJsonSchemaObject(description, restrictions.allOf(allOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#anyOf(java.util.Collection)
		 */
		@Override
		public NullJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return new NullJsonSchemaObject(description, restrictions.anyOf(anyOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#oneOf(java.util.Collection)
		 */
		@Override
		public NullJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return new NullJsonSchemaObject(description, restrictions.oneOf(oneOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#notMatch(org.springframework.data.mongodb.core.schema.JsonSchemaObject)
		 */
		@Override
		public NullJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return new NullJsonSchemaObject(description, restrictions.notMatch(notMatch));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#description(java.lang.String)
		 */
		@Override
		public NullJsonSchemaObject description(String description) {
			return new NullJsonSchemaObject(description, restrictions);
		}
	}
}
