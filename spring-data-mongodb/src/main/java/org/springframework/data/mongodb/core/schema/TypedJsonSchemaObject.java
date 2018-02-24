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
 * A {@link JsonSchemaObject} of a given {@link org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
public class TypedJsonSchemaObject extends UntypedJsonSchemaObject {

	protected final Set<Type> types;

	/**
	 * @param type can be {@literal null}.
	 * @param description can be {@literal null}.
	 * @param restrictions can be {@literal null}.
	 */
	TypedJsonSchemaObject(@Nullable Type type, @Nullable String description, boolean generateDescription,
			@Nullable Restrictions restrictions) {

		this(type != null ? Collections.singleton(type) : Collections.emptySet(), description, generateDescription,
				restrictions);
	}

	/**
	 * @param types must not be {@literal null}.
	 * @param description can be {@literal null}.
	 * @param restrictions can be {@literal null}. Defaults to {@link Restrictions#empty()}.
	 */
	TypedJsonSchemaObject(Set<Type> types, @Nullable String description, boolean generateDescription,
			@Nullable Restrictions restrictions) {

		super(restrictions, description, generateDescription);

		Assert.notNull(types, "Types must not be null! Please consider using 'Collections.emptySet()'.");

		this.types = types;
	}

	/**
	 * Creates new {@link TypedJsonSchemaObject} of given types.
	 *
	 * @param types must not be {@literal null}.
	 * @return
	 */
	public static TypedJsonSchemaObject of(Type... types) {

		Assert.notNull(types, "Types must not be null!");
		Assert.noNullElements(types, "Types must not contain null!");

		return new TypedJsonSchemaObject(new LinkedHashSet<>(Arrays.asList(types)), null, false, Restrictions.empty());
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
	 * Set the {@literal description}.
	 *
	 * @param description must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	@Override
	public TypedJsonSchemaObject description(String description) {
		return new TypedJsonSchemaObject(types, description, generateDescription, restrictions);
	}

	/**
	 * Auto generate the {@literal description} if not explicitly set.
	 *
	 * @param description must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	@Override
	public TypedJsonSchemaObject generatedDescription() {
		return new TypedJsonSchemaObject(types, description, true, restrictions);
	}

	/**
	 * {@literal enum}erates all possible values of the field.
	 *
	 * @param possibleValues must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	@Override
	public TypedJsonSchemaObject possibleValues(Collection<? extends Object> possibleValues) {
		return new TypedJsonSchemaObject(types, description, generateDescription,
				restrictions.possibleValues(possibleValues));
	}

	/**
	 * The field value must match all specified schemas.
	 *
	 * @param allOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	@Override
	public TypedJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
		return new TypedJsonSchemaObject(types, description, generateDescription, restrictions.allOf(allOf));
	}

	/**
	 * The field value must match at least one of the specified schemas.
	 *
	 * @param anyOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	@Override
	public TypedJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
		return new TypedJsonSchemaObject(types, description, generateDescription, restrictions.anyOf(anyOf));
	}

	/**
	 * The field value must match exactly one of the specified schemas.
	 *
	 * @param oneOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	@Override
	public TypedJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
		return new TypedJsonSchemaObject(types, description, generateDescription, restrictions.oneOf(oneOf));
	}

	/**
	 * The field value must not match the specified schemas.
	 *
	 * @param oneOf must not be {@literal null}.
	 * @return new instance of {@link TypedJsonSchemaObject}.
	 */
	@Override
	public TypedJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
		return new TypedJsonSchemaObject(types, description, generateDescription, restrictions.notMatch(notMatch));
	}

	/**
	 * Create the JSON schema complying {@link Document} representation. This includes {@literal type},
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
		document.putAll(restrictions.toDocument());

		return document;
	}

	private Optional<String> getOrCreateDescription() {

		if (description != null) {
			return description.isEmpty() ? Optional.empty() : Optional.of(description);
		}

		return generateDescription ? Optional.ofNullable(generateDescription()) : Optional.empty();
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
	 * {@link JsonSchemaObject} implementation of {@code type : 'object'} schema elements.<br />
	 * Provides programmatic access to schema specifics like {@literal required, properties, patternProperties,...} via a
	 * fluent API producing immutable {@link JsonSchemaObject schema objects}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class ObjectJsonSchemaObject extends TypedJsonSchemaObject {

		private @Nullable Range<Integer> propertiesCount;
		private @Nullable Object additionalProperties;
		private List<String> requiredProperties = Collections.emptyList();
		private List<JsonSchemaProperty> properties = Collections.emptyList();
		private List<JsonSchemaProperty> patternProperties = Collections.emptyList();

		public ObjectJsonSchemaObject() {
			this(null, false, null);
		}

		/**
		 * @param description can be {@literal null}.
		 * @param restrictions can be {@literal null};
		 */
		ObjectJsonSchemaObject(@Nullable String description, boolean generateDescription,
				@Nullable Restrictions restrictions) {
			super(Type.objectType(), description, generateDescription, restrictions);
		}

		/**
		 * Define the {@literal minProperties} and {@literal maxProperties} via the given {@link Range}.<br />
		 * In-/Exclusions via {@link Bound#isInclusive() range bounds} are not taken into account.
		 *
		 * @param range must not be {@literal null}. Consider {@link Range#unbounded()} instead.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject propertiesCount(Range<Integer> range) {

			ObjectJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
			newInstance.propertiesCount = range;
			return newInstance;
		}

		/**
		 * Define the {@literal minProperties}.
		 *
		 * @param count the allowed minimal number of properties.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject minProperties(int count) {

			Bound<Integer> upper = this.propertiesCount != null ? this.propertiesCount.getUpperBound() : Bound.unbounded();
			return propertiesCount(Range.of(Bound.inclusive(count), upper));
		}

		/**
		 * Define the {@literal maxProperties}.
		 *
		 * @param count the allowed maximum number of properties.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject maxProperties(int count) {

			Bound<Integer> lower = this.propertiesCount != null ? this.propertiesCount.getLowerBound() : Bound.unbounded();
			return propertiesCount(Range.of(lower, Bound.inclusive(count)));
		}

		/**
		 * Define the Object’s {@literal required} properties.
		 *
		 * @param properties the names of required properties.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject required(String... properties) {

			ObjectJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
			newInstance.requiredProperties = new ArrayList<>(this.requiredProperties.size() + properties.length);
			newInstance.requiredProperties.addAll(this.requiredProperties);
			newInstance.requiredProperties.addAll(Arrays.asList(properties));

			return newInstance;
		}

		/**
		 * If set to {@literal false}, additional fields besides
		 * {@link #properties(JsonSchemaProperty...)}/{@link #patternProperties(JsonSchemaProperty...)} are not allowed.
		 *
		 * @param additionalPropertiesAllowed
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject additionalProperties(boolean additionalPropertiesAllowed) {

			ObjectJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
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

			ObjectJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
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

			ObjectJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
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

			ObjectJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
			newInstance.patternProperties = new ArrayList<>(this.patternProperties.size() + regularExpressions.length);
			newInstance.patternProperties.addAll(this.patternProperties);
			newInstance.patternProperties.addAll(Arrays.asList(regularExpressions));

			return newInstance;
		}

		/**
		 * Append the objects property along with the {@link JsonSchemaObject} validating against.
		 *
		 * @param property must not be {@literal null}.
		 * @return new instance of {@link ObjectJsonSchemaObject}.
		 */
		public ObjectJsonSchemaObject property(JsonSchemaProperty property) {
			return properties(property);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.UntypedJsonSchemaObject#possibleValues(java.util.Collection)
		 */
		@Override
		public ObjectJsonSchemaObject possibleValues(Collection<? extends Object> possibleValues) {
			return newInstance(description, generateDescription, restrictions.possibleValues(possibleValues));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.UntypedJsonSchemaObject#allOf(java.util.Collection)
		 */
		@Override
		public ObjectJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return newInstance(description, generateDescription, restrictions.allOf(allOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.UntypedJsonSchemaObject#anyOf(java.util.Collection)
		 */
		@Override
		public ObjectJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return newInstance(description, generateDescription, restrictions.anyOf(anyOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.UntypedJsonSchemaObject#oneOf(java.util.Collection)
		 */
		@Override
		public ObjectJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return newInstance(description, generateDescription, restrictions.oneOf(oneOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.UntypedJsonSchemaObject#notMatch(org.springframework.data.mongodb.core.schema.JsonSchemaObject)
		 */
		@Override
		public ObjectJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return newInstance(description, generateDescription, restrictions.notMatch(notMatch));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.UntypedJsonSchemaObject#description(java.lang.String)
		 */
		@Override
		public ObjectJsonSchemaObject description(String description) {
			return newInstance(description, generateDescription, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.UntypedJsonSchemaObject#generatedDescription()
		 */
		@Override
		public ObjectJsonSchemaObject generatedDescription() {
			return newInstance(description, true, restrictions);
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

			if (propertiesCount != null) {

				propertiesCount.getLowerBound().getValue().ifPresent(it -> doc.append("minProperties", it));
				propertiesCount.getUpperBound().getValue().ifPresent(it -> doc.append("maxProperties", it));
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

		private ObjectJsonSchemaObject newInstance(@Nullable String description, boolean generateDescription,
				Restrictions restrictions) {

			ObjectJsonSchemaObject newInstance = new ObjectJsonSchemaObject(description, generateDescription, restrictions);

			newInstance.properties = this.properties;
			newInstance.requiredProperties = this.requiredProperties;
			newInstance.additionalProperties = this.additionalProperties;
			newInstance.propertiesCount = this.propertiesCount;
			newInstance.patternProperties = this.patternProperties;

			return newInstance;
		}

		private Document reduceToDocument(Collection<JsonSchemaProperty> source) {

			return source.stream() //
					.map(JsonSchemaProperty::toDocument) //
					.collect(Document::new, Document::putAll, (target, propertyDocument) -> {});
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#generateDescription()
		 */
		@Override
		protected String generateDescription() {

			String description = "Must be an object";

			if (propertiesCount != null) {
				description += String.format(" with %s properties", propertiesCount);
			}

			if (!CollectionUtils.isEmpty(requiredProperties)) {

				if (requiredProperties.size() == 1) {
					description += String.format(" where %sis mandatory", requiredProperties.iterator().next());
				} else {
					description += String.format(" where %s are mandatory",
							StringUtils.collectionToDelimitedString(requiredProperties, ", "));
				}
			}
			if (additionalProperties instanceof Boolean) {
				description += (((Boolean) additionalProperties) ? " " : " not ") + "allowing additional properties";
			}

			if (!CollectionUtils.isEmpty(properties)) {
				description += String.format(" defining restrictions for %s", StringUtils.collectionToDelimitedString(
						properties.stream().map(JsonSchemaProperty::getIdentifier).collect(Collectors.toList()), ", "));
			}

			if (!CollectionUtils.isEmpty(patternProperties)) {
				description += String.format(" defining restrictions for patterns %s", StringUtils.collectionToDelimitedString(
						patternProperties.stream().map(JsonSchemaProperty::getIdentifier).collect(Collectors.toList()), ", "));
			}

			return description + ".";
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
	public static class NumericJsonSchemaObject extends TypedJsonSchemaObject {

		private static final Set<Type> NUMERIC_TYPES = new HashSet<>(
				Arrays.asList(Type.doubleType(), Type.intType(), Type.longType(), Type.numberType(), Type.bigDecimalType()));

		@Nullable Number multipleOf;
		@Nullable Range<? extends Number> range;

		NumericJsonSchemaObject() {
			this(Type.numberType());
		}

		NumericJsonSchemaObject(Type type) {
			this(type, null, false);
		}

		private NumericJsonSchemaObject(Type type, @Nullable String description, boolean generateDescription) {
			this(Collections.singleton(type), description, generateDescription, null);
		}

		private NumericJsonSchemaObject(Set<Type> types, @Nullable String description, boolean generateDescription,
				@Nullable Restrictions restrictions) {

			super(validateTypes(types), description, generateDescription, restrictions);
		}

		/**
		 * Set the value a valid field value must be the multiple of.
		 *
		 * @param value must not be {@literal null}.
		 * @return must not be {@literal null}.
		 */
		NumericJsonSchemaObject multipleOf(Number value) {

			Assert.notNull(value, "Value must not be null!");
			NumericJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
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

			NumericJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
			newInstance.range = range;

			return newInstance;
		}

		/**
		 * Set {@literal minimum} to given {@code min} value and {@literal exclusiveMinimum} to {@literal true}.
		 *
		 * @param min must not be {@literal null}.
		 * @return new instance of {@link NumericJsonSchemaObject}.
		 */
		@SuppressWarnings("unchecked")
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
		@SuppressWarnings("unchecked")
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
		@SuppressWarnings("unchecked")
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
		@SuppressWarnings("unchecked")
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
		public NumericJsonSchemaObject possibleValues(Collection<? extends Object> possibleValues) {
			return newInstance(description, generateDescription, restrictions.possibleValues(possibleValues));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#allOf(java.util.Collection)
		 */
		@Override
		public NumericJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return newInstance(description, generateDescription, restrictions.allOf(allOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#anyOf(java.util.Collection)
		 */
		@Override
		public NumericJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return newInstance(description, generateDescription, restrictions.anyOf(anyOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#oneOf(java.util.Collection)
		 */
		@Override
		public NumericJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return newInstance(description, generateDescription, restrictions.oneOf(oneOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#notMatch(org.springframework.data.mongodb.core.schema.JsonSchemaObject)
		 */
		@Override
		public NumericJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return newInstance(description, generateDescription, restrictions.notMatch(notMatch));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#description(java.lang.String)
		 */
		@Override
		public NumericJsonSchemaObject description(String description) {
			return newInstance(description, generateDescription, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#generatedDescription()
		 */
		@Override
		public NumericJsonSchemaObject generatedDescription() {
			return newInstance(description, true, restrictions);
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

					range.getLowerBound().getValue().ifPresent(it -> doc.append("minimum", it));
					if (!range.getLowerBound().isInclusive()) {
						doc.append("exclusiveMinimum", true);
					}
				}

				if (range.getUpperBound().isBounded()) {

					range.getUpperBound().getValue().ifPresent(it -> doc.append("maximum", it));
					if (!range.getUpperBound().isInclusive()) {
						doc.append("exclusiveMaximum", true);
					}
				}
			}

			return doc;
		}

		private NumericJsonSchemaObject newInstance(@Nullable String description, boolean generateDescription,
				Restrictions restrictions) {

			NumericJsonSchemaObject newInstance = new NumericJsonSchemaObject(types, description, generateDescription,
					restrictions);

			newInstance.multipleOf = this.multipleOf;
			newInstance.range = this.range;

			return newInstance;

		}

		private static Bound<?> createBound(Number number, boolean inclusive) {

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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#generateDescription()
		 */
		@Override
		protected String generateDescription() {

			String description = "Must be a numeric value";

			if (multipleOf != null) {
				description += String.format(" multiple of %s", multipleOf);
			}
			if (range != null) {
				description += String.format(" within range %s", range);
			}

			return description + ".";
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
	public static class StringJsonSchemaObject extends TypedJsonSchemaObject {

		@Nullable Range<Integer> length;
		@Nullable String pattern;

		StringJsonSchemaObject() {
			this(null, false, null);
		}

		private StringJsonSchemaObject(@Nullable String description, boolean generateDescription,
				@Nullable Restrictions restrictions) {
			super(Type.stringType(), description, generateDescription, restrictions);
		}

		/**
		 * Define the valid length range ({@literal minLength} and {@literal maxLength}) for a valid field.
		 *
		 * @param range must not be {@literal null}.
		 * @return new instance of {@link StringJsonSchemaObject}.
		 */
		public StringJsonSchemaObject length(Range<Integer> range) {

			Assert.notNull(range, "Range must not be null!");

			StringJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
			newInstance.length = range;

			return newInstance;
		}

		/**
		 * Define the valid length range ({@literal minLength}) for a valid field.
		 *
		 * @param length
		 * @return new instance of {@link StringJsonSchemaObject}.
		 */
		public StringJsonSchemaObject minLength(int length) {

			Bound<Integer> upper = this.length != null ? this.length.getUpperBound() : Bound.unbounded();
			return length(Range.of(Bound.inclusive(length), upper));
		}

		/**
		 * Define the valid length range ({@literal maxLength}) for a valid field.
		 *
		 * @param length
		 * @return new instance of {@link StringJsonSchemaObject}.
		 */
		public StringJsonSchemaObject maxLength(int length) {

			Bound<Integer> lower = this.length != null ? this.length.getLowerBound() : Bound.unbounded();
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

			StringJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
			newInstance.pattern = pattern;

			return newInstance;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#possibleValues(java.util.Collection)
		 */
		@Override
		public StringJsonSchemaObject possibleValues(Collection<? extends Object> possibleValues) {
			return newInstance(description, generateDescription, restrictions.possibleValues(possibleValues));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#allOf(java.util.Collection)
		 */
		@Override
		public StringJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return newInstance(description, generateDescription, restrictions.allOf(allOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#anyOf(java.util.Collection)
		 */
		@Override
		public StringJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return newInstance(description, generateDescription, restrictions.anyOf(anyOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#oneOf(java.util.Collection)
		 */
		@Override
		public StringJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return newInstance(description, generateDescription, restrictions.oneOf(oneOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#notMatch(org.springframework.data.mongodb.core.schema.JsonSchemaObject)
		 */
		@Override
		public StringJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return newInstance(description, generateDescription, restrictions.notMatch(notMatch));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#description(java.lang.String)
		 */
		@Override
		public StringJsonSchemaObject description(String description) {
			return newInstance(description, generateDescription, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#generatedDescription()
		 */
		@Override
		public StringJsonSchemaObject generatedDescription() {
			return newInstance(description, true, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.JsonSchemaObject#toDocument()
		 */
		@Override
		public Document toDocument() {

			Document doc = new Document(super.toDocument());

			if (length != null) {

				length.getLowerBound().getValue().ifPresent(it -> doc.append("minLength", it));
				length.getUpperBound().getValue().ifPresent(it -> doc.append("maxLength", it));
			}

			if (!StringUtils.isEmpty(pattern)) {
				doc.append("pattern", pattern);
			}

			return doc;
		}

		private StringJsonSchemaObject newInstance(@Nullable String description, boolean generateDescription,
				Restrictions restrictions) {

			StringJsonSchemaObject newInstance = new StringJsonSchemaObject(description, generateDescription, restrictions);

			newInstance.length = this.length;
			newInstance.pattern = this.pattern;

			return newInstance;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#generateDescription()
		 */
		@Override
		protected String generateDescription() {

			String description = "Must be a string";

			if (length != null) {
				description += String.format(" with length %s", length);
			}
			if (pattern != null) {
				description += String.format(" matching %s", pattern);
			}

			return description + ".";
		}
	}

	/**
	 * {@link JsonSchemaObject} implementation of {@code type : 'array'} schema elements.<br />
	 * Provides programmatic access to schema specifics like {@literal range, minItems, maxItems,...} via a fluent API
	 * producing immutable {@link JsonSchemaObject schema objects}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.1
	 */
	public static class ArrayJsonSchemaObject extends TypedJsonSchemaObject {

		private @Nullable Boolean uniqueItems;
		private @Nullable Boolean additionalItems;
		private @Nullable Range<Integer> range;
		private Collection<JsonSchemaObject> items = Collections.emptyList();

		ArrayJsonSchemaObject() {
			this(null, false, null);
		}

		private ArrayJsonSchemaObject(@Nullable String description, boolean generateDescription,
				@Nullable Restrictions restrictions) {
			super(Collections.singleton(Type.arrayType()), description, generateDescription, restrictions);
		}

		/**
		 * Define the whether the array must contain unique items.
		 *
		 * @param uniqueItems
		 * @return new instance of {@link ArrayJsonSchemaObject}.
		 */
		public ArrayJsonSchemaObject uniqueItems(boolean uniqueItems) {

			ArrayJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
			newInstance.uniqueItems = uniqueItems;

			return newInstance;
		}

		/**
		 * Define the {@literal minItems} and {@literal maxItems} via the given {@link Range}.<br />
		 * In-/Exclusions via {@link Bound#isInclusive() range bounds} are not taken into account.
		 *
		 * @param range must not be {@literal null}. Consider {@link Range#unbounded()} instead.
		 * @return new instance of {@link ArrayJsonSchemaObject}.
		 */
		public ArrayJsonSchemaObject range(Range<Integer> range) {

			ArrayJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
			newInstance.range = range;

			return newInstance;
		}

		/**
		 * Define the {@literal maxItems}.
		 *
		 * @param count the allowed minimal number of array items.
		 * @return new instance of {@link ArrayJsonSchemaObject}.
		 */
		public ArrayJsonSchemaObject minItems(int count) {

			Bound<Integer> upper = this.range != null ? this.range.getUpperBound() : Bound.unbounded();
			return range(Range.of(Bound.inclusive(count), upper));
		}

		/**
		 * Define the {@literal maxItems}.
		 *
		 * @param count the allowed maximal number of array items.
		 * @return new instance of {@link ArrayJsonSchemaObject}.
		 */
		public ArrayJsonSchemaObject maxItems(int count) {

			Bound<Integer> lower = this.range != null ? this.range.getLowerBound() : Bound.unbounded();
			return range(Range.of(lower, Bound.inclusive(count)));
		}

		/**
		 * Define the {@code items} allowed in the array.
		 *
		 * @param items the allowed items in the array.
		 * @return new instance of {@link ArrayJsonSchemaObject}.
		 */
		public ArrayJsonSchemaObject items(Collection<JsonSchemaObject> items) {

			ArrayJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
			newInstance.items = new ArrayList<>(items);

			return newInstance;
		}

		/**
		 * If set to {@literal false}, no additional items besides {@link #items(Collection)} are allowed.
		 *
		 * @param additionalItemsAllowed {@literal true} to allow additional items in the array, {@literal false} otherwise.
		 * @return new instance of {@link ArrayJsonSchemaObject}.
		 */
		public ArrayJsonSchemaObject additionalItems(boolean additionalItemsAllowed) {

			ArrayJsonSchemaObject newInstance = newInstance(description, generateDescription, restrictions);
			newInstance.additionalItems = additionalItemsAllowed;

			return newInstance;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#possibleValues(java.util.Collection)
		 */
		@Override
		public ArrayJsonSchemaObject possibleValues(Collection<? extends Object> possibleValues) {
			return newInstance(description, generateDescription, restrictions.possibleValues(possibleValues));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#allOf(java.util.Collection)
		 */
		@Override
		public ArrayJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return newInstance(description, generateDescription, restrictions.allOf(allOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#anyOf(java.util.Collection)
		 */
		@Override
		public ArrayJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return newInstance(description, generateDescription, restrictions.anyOf(anyOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#oneOf(java.util.Collection)
		 */
		@Override
		public ArrayJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return newInstance(description, generateDescription, restrictions.oneOf(oneOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#notMatch(org.springframework.data.mongodb.core.schema.JsonSchemaObject)
		 */
		@Override
		public ArrayJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return newInstance(description, generateDescription, restrictions.notMatch(notMatch));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#description(java.lang.String)
		 */
		@Override
		public ArrayJsonSchemaObject description(String description) {
			return newInstance(description, generateDescription, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#generatedDescription()
		 */
		@Override
		public ArrayJsonSchemaObject generatedDescription() {
			return newInstance(description, true, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#toDocument()
		 */
		@Override
		public Document toDocument() {

			Document doc = new Document(super.toDocument());

			if (!CollectionUtils.isEmpty(items)) {
				doc.append("items", items.size() == 1 ? items.iterator().next()
						: items.stream().map(JsonSchemaObject::toDocument).collect(Collectors.toList()));
			}

			if (range != null) {

				range.getLowerBound().getValue().ifPresent(it -> doc.append("minItems", it));
				range.getUpperBound().getValue().ifPresent(it -> doc.append("maxItems", it));
			}

			if (ObjectUtils.nullSafeEquals(uniqueItems, Boolean.TRUE)) {
				doc.append("uniqueItems", true);
			}

			if (additionalItems != null) {
				doc.append("additionalItems", additionalItems);
			}

			return doc;
		}

		private ArrayJsonSchemaObject newInstance(@Nullable String description, boolean generateDescription,
				Restrictions restrictions) {

			ArrayJsonSchemaObject newInstance = new ArrayJsonSchemaObject(description, generateDescription, restrictions);

			newInstance.uniqueItems = this.uniqueItems;
			newInstance.range = this.range;
			newInstance.items = this.items;
			newInstance.additionalItems = this.additionalItems;

			return newInstance;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#generateDescription()
		 */
		@Override
		protected String generateDescription() {

			String description = "Must be an array";

			if (ObjectUtils.nullSafeEquals(uniqueItems, Boolean.TRUE)) {
				description += " of unique values";
			}

			if (ObjectUtils.nullSafeEquals(additionalItems, Boolean.TRUE)) {
				description += " with additional items";
			}

			if (ObjectUtils.nullSafeEquals(additionalItems, Boolean.FALSE)) {
				description += " with no additional items";
			}

			if (range != null) {
				description += String.format(" having size %s", range);
			}

			if (!ObjectUtils.isEmpty(items)) {
				description += String.format(" with items %s", StringUtils.collectionToDelimitedString(
						items.stream().map(JsonSchemaObject::toDocument).collect(Collectors.toList()), ", "));
			}

			return description + ".";
		}
	}

	/**
	 * {@link JsonSchemaObject} implementation of {@code type : 'boolean'} schema elements.<br />
	 * Provides programmatic access to schema specifics via a fluent API producing immutable {@link JsonSchemaObject
	 * schema objects}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.1
	 */
	public static class BooleanJsonSchemaObject extends TypedJsonSchemaObject {

		BooleanJsonSchemaObject() {
			this(null, false, null);
		}

		private BooleanJsonSchemaObject(@Nullable String description, boolean generateDescription,
				@Nullable Restrictions restrictions) {
			super(Type.booleanType(), description, generateDescription, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#possibleValues(java.util.Collection)
		 */
		@Override
		public BooleanJsonSchemaObject possibleValues(Collection<? extends Object> possibleValues) {
			return new BooleanJsonSchemaObject(description, generateDescription, restrictions.possibleValues(possibleValues));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#allOf(java.util.Collection)
		 */
		@Override
		public BooleanJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return new BooleanJsonSchemaObject(description, generateDescription, restrictions.allOf(allOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#anyOf(java.util.Collection)
		 */
		@Override
		public BooleanJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return new BooleanJsonSchemaObject(description, generateDescription, restrictions.anyOf(anyOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#oneOf(java.util.Collection)
		 */
		@Override
		public BooleanJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return new BooleanJsonSchemaObject(description, generateDescription, restrictions.oneOf(oneOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#notMatch(org.springframework.data.mongodb.core.schema.JsonSchemaObject)
		 */
		@Override
		public BooleanJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return new BooleanJsonSchemaObject(description, generateDescription, restrictions.notMatch(notMatch));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#description(java.lang.String)
		 */
		@Override
		public BooleanJsonSchemaObject description(String description) {
			return new BooleanJsonSchemaObject(description, generateDescription, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#generatedDescription()
		 */
		@Override
		public BooleanJsonSchemaObject generatedDescription() {
			return new BooleanJsonSchemaObject(description, true, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#generateDescription()
		 */
		@Override
		protected String generateDescription() {
			return "Must be a boolean.";
		}
	}

	/**
	 * {@link JsonSchemaObject} implementation of {@code type : 'null'} schema elements.<br />
	 * Provides programmatic access to schema specifics via a fluent API producing immutable {@link JsonSchemaObject
	 * schema objects}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.1
	 */
	static class NullJsonSchemaObject extends TypedJsonSchemaObject {

		NullJsonSchemaObject() {
			this(null, false, null);
		}

		private NullJsonSchemaObject(@Nullable String description, boolean generateDescription,
				@Nullable Restrictions restrictions) {
			super(Type.nullType(), description, generateDescription, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#possibleValues(java.util.Collection)
		 */
		@Override
		public NullJsonSchemaObject possibleValues(Collection<? extends Object> possibleValues) {
			return new NullJsonSchemaObject(description, generateDescription, restrictions.possibleValues(possibleValues));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#allOf(java.util.Collection)
		 */
		@Override
		public NullJsonSchemaObject allOf(Collection<JsonSchemaObject> allOf) {
			return new NullJsonSchemaObject(description, generateDescription, restrictions.allOf(allOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#anyOf(java.util.Collection)
		 */
		@Override
		public NullJsonSchemaObject anyOf(Collection<JsonSchemaObject> anyOf) {
			return new NullJsonSchemaObject(description, generateDescription, restrictions.anyOf(anyOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#oneOf(java.util.Collection)
		 */
		@Override
		public NullJsonSchemaObject oneOf(Collection<JsonSchemaObject> oneOf) {
			return new NullJsonSchemaObject(description, generateDescription, restrictions.oneOf(oneOf));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#notMatch(org.springframework.data.mongodb.core.schema.JsonSchemaObject)
		 */
		@Override
		public NullJsonSchemaObject notMatch(JsonSchemaObject notMatch) {
			return new NullJsonSchemaObject(description, generateDescription, restrictions.notMatch(notMatch));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#description(java.lang.String)
		 */
		@Override
		public NullJsonSchemaObject description(String description) {
			return new NullJsonSchemaObject(description, generateDescription, restrictions);
		}

		/*
		* (non-Javadoc)
		* @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#generatedDescription()
		*/
		@Override
		public NullJsonSchemaObject generatedDescription() {
			return new NullJsonSchemaObject(description, true, restrictions);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject#generateDescription()
		 */
		@Override
		protected String generateDescription() {
			return "Must be null.";
		}
	}
}
