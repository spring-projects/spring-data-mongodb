/*
 * Copyright 2019-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.schema.IdentifiableJsonSchemaProperty.ArrayJsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.IdentifiableJsonSchemaProperty.EncryptedJsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.IdentifiableJsonSchemaProperty.ObjectJsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type;
import org.springframework.data.mongodb.core.schema.JsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema.MongoJsonSchemaBuilder;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * {@link MongoJsonSchemaCreator} implementation using both {@link MongoConverter} and {@link MappingContext} to obtain
 * domain type meta information which considers {@link org.springframework.data.mongodb.core.mapping.Field field names}
 * and {@link org.springframework.data.mongodb.core.convert.MongoCustomConversions custom conversions}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.2
 */
class MappingMongoJsonSchemaCreator implements MongoJsonSchemaCreator {

	private final MongoConverter converter;
	private final MappingContext<MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final Predicate<JsonSchemaPropertyContext> filter;
	private final LinkedMultiValueMap<String, Class<?>> mergeProperties;

	/**
	 * Create a new instance of {@link MappingMongoJsonSchemaCreator}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	MappingMongoJsonSchemaCreator(MongoConverter converter) {

		this(converter, (MappingContext<MongoPersistentEntity<?>, MongoPersistentProperty>) converter.getMappingContext(),
				(property) -> true, new LinkedMultiValueMap<>());
	}

	@SuppressWarnings("unchecked")
	MappingMongoJsonSchemaCreator(MongoConverter converter,
			MappingContext<MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
			Predicate<JsonSchemaPropertyContext> filter, LinkedMultiValueMap<String, Class<?>> mergeProperties) {

		Assert.notNull(converter, "Converter must not be null");
		this.converter = converter;
		this.mappingContext = mappingContext;
		this.filter = filter;
		this.mergeProperties = mergeProperties;
	}

	@Override
	public MongoJsonSchemaCreator filter(Predicate<JsonSchemaPropertyContext> filter) {
		return new MappingMongoJsonSchemaCreator(converter, mappingContext, filter, mergeProperties);
	}

	@Override
	public PropertySpecifier property(String path) {
		return types -> withTypesFor(path, types);
	}

	/**
	 * Specify additional types to be considered when rendering the schema for the given path.
	 *
	 * @param path path the path using {@literal dot '.'} notation.
	 * @param types must not be {@literal null}.
	 * @return new instance of {@link MongoJsonSchemaCreator}.
	 * @since 3.4
	 */
	public MongoJsonSchemaCreator withTypesFor(String path, Class<?>... types) {

		LinkedMultiValueMap<String, Class<?>> clone = mergeProperties.clone();
		for (Class<?> type : types) {
			clone.add(path, type);
		}
		return new MappingMongoJsonSchemaCreator(converter, mappingContext, filter, clone);
	}

	@Override
	public MongoJsonSchema createSchemaFor(Class<?> type) {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);
		MongoJsonSchemaBuilder schemaBuilder = MongoJsonSchema.builder();

		{
			Encrypted encrypted = entity.findAnnotation(Encrypted.class);
			if (encrypted != null) {

				Document encryptionMetadata = new Document();

				Collection<Object> encryptionKeyIds = entity.getEncryptionKeyIds();
				if (!CollectionUtils.isEmpty(encryptionKeyIds)) {
					encryptionMetadata.append("keyId", encryptionKeyIds);
				}

				if (StringUtils.hasText(encrypted.algorithm())) {
					encryptionMetadata.append("algorithm", encrypted.algorithm());
				}

				schemaBuilder.encryptionMetadata(encryptionMetadata);
			}
		}

		List<JsonSchemaProperty> schemaProperties = computePropertiesForEntity(Collections.emptyList(), entity);
		schemaBuilder.properties(schemaProperties.toArray(new JsonSchemaProperty[0]));

		return schemaBuilder.build();
	}

	private List<JsonSchemaProperty> computePropertiesForEntity(List<MongoPersistentProperty> path,
			MongoPersistentEntity<?> entity) {

		List<JsonSchemaProperty> schemaProperties = new ArrayList<>();

		for (MongoPersistentProperty nested : entity) {

			List<MongoPersistentProperty> currentPath = new ArrayList<>(path);

			String stringPath = currentPath.stream().map(PersistentProperty::getName).collect(Collectors.joining("."));
			stringPath = StringUtils.hasText(stringPath) ? (stringPath + "." + nested.getName()) : nested.getName();
			if (!filter.test(new PropertyContext(stringPath, nested))) {
				if (!mergeProperties.containsKey(stringPath)) {
					continue;
				}
			}

			if (path.contains(nested)) { // cycle guard
				schemaProperties.add(createSchemaProperty(computePropertyFieldName(CollectionUtils.lastElement(currentPath)),
						Object.class, false));
				break;
			}

			currentPath.add(nested);
			schemaProperties.add(computeSchemaForProperty(currentPath));
		}

		return schemaProperties;
	}

	private JsonSchemaProperty computeSchemaForProperty(List<MongoPersistentProperty> path) {

		String stringPath = path.stream().map(MongoPersistentProperty::getName).collect(Collectors.joining("."));
		MongoPersistentProperty property = CollectionUtils.lastElement(path);

		boolean required = isRequiredProperty(property);
		Class<?> rawTargetType = computeTargetType(property); // target type before conversion
		Class<?> targetType = converter.getTypeMapper().getWriteTargetTypeFor(rawTargetType); // conversion target type

		if (!isCollection(property) && ObjectUtils.nullSafeEquals(rawTargetType, targetType)) {
			if (property.isEntity() || mergeProperties.containsKey(stringPath)) {
				List<JsonSchemaProperty> targetProperties = new ArrayList<>();

				if (property.isEntity()) {
					targetProperties.add(createObjectSchemaPropertyForEntity(path, property, required));
				}
				if (mergeProperties.containsKey(stringPath)) {
					for (Class<?> theType : mergeProperties.get(stringPath)) {

						ObjectJsonSchemaProperty target = JsonSchemaProperty.object(property.getName());
						List<JsonSchemaProperty> nestedProperties = computePropertiesForEntity(path,
								mappingContext.getRequiredPersistentEntity(theType));

						targetProperties.add(createPotentiallyRequiredSchemaProperty(
								target.properties(nestedProperties.toArray(new JsonSchemaProperty[0])), required));
					}
				}
				JsonSchemaProperty schemaProperty = targetProperties.size() == 1 ? targetProperties.iterator().next()
						: JsonSchemaProperty.merged(targetProperties);
				return applyEncryptionDataIfNecessary(property, schemaProperty);
			}
		}

		String fieldName = computePropertyFieldName(property);

		JsonSchemaProperty schemaProperty;
		if (isCollection(property)) {
			schemaProperty = createArraySchemaProperty(fieldName, property, required);
		} else if (property.isMap()) {
			schemaProperty = createSchemaProperty(fieldName, Type.objectType(), required);
		} else if (ClassUtils.isAssignable(Enum.class, targetType)) {
			schemaProperty = createEnumSchemaProperty(fieldName, targetType, required);
		} else {
			schemaProperty = createSchemaProperty(fieldName, targetType, required);
		}

		return applyEncryptionDataIfNecessary(property, schemaProperty);
	}

	private JsonSchemaProperty createArraySchemaProperty(String fieldName, MongoPersistentProperty property,
			boolean required) {

		ArrayJsonSchemaProperty schemaProperty = JsonSchemaProperty.array(fieldName);

		if (isSpecificType(property)) {
			schemaProperty = potentiallyEnhanceArraySchemaProperty(property, schemaProperty);
		}

		return createPotentiallyRequiredSchemaProperty(schemaProperty, required);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ArrayJsonSchemaProperty potentiallyEnhanceArraySchemaProperty(MongoPersistentProperty property,
			ArrayJsonSchemaProperty schemaProperty) {

		MongoPersistentEntity<?> persistentEntity = mappingContext
				.getPersistentEntity(property.getTypeInformation().getRequiredComponentType());

		if (persistentEntity != null) {

			List<JsonSchemaProperty> nestedProperties = computePropertiesForEntity(Collections.emptyList(), persistentEntity);

			if (nestedProperties.isEmpty()) {
				return schemaProperty;
			}

			return schemaProperty
					.items(JsonSchemaObject.object().properties(nestedProperties.toArray(new JsonSchemaProperty[0])));
		}

		if (ClassUtils.isAssignable(Enum.class, property.getActualType())) {

			List<Object> possibleValues = getPossibleEnumValues((Class<Enum>) property.getActualType());

			return schemaProperty
					.items(createSchemaObject(computeTargetType(property.getActualType(), possibleValues), possibleValues));
		}

		return schemaProperty.items(JsonSchemaObject.of(property.getActualType()));
	}

	private boolean isSpecificType(MongoPersistentProperty property) {
		return !TypeInformation.OBJECT.equals(property.getTypeInformation().getActualType());
	}

	private JsonSchemaProperty applyEncryptionDataIfNecessary(MongoPersistentProperty property,
			JsonSchemaProperty schemaProperty) {

		Encrypted encrypted = property.findAnnotation(Encrypted.class);
		if (encrypted == null) {
			return schemaProperty;
		}

		EncryptedJsonSchemaProperty enc = new EncryptedJsonSchemaProperty(schemaProperty);
		if (StringUtils.hasText(encrypted.algorithm())) {
			enc = enc.algorithm(encrypted.algorithm());
		}
		if (!ObjectUtils.isEmpty(encrypted.keyId())) {
			enc = enc.keys(property.getEncryptionKeyIds());
		}
		return enc;
	}

	private JsonSchemaProperty createObjectSchemaPropertyForEntity(List<MongoPersistentProperty> path,
			MongoPersistentProperty property, boolean required) {

		ObjectJsonSchemaProperty target = JsonSchemaProperty.object(property.getName());
		List<JsonSchemaProperty> nestedProperties = computePropertiesForEntity(path,
				mappingContext.getRequiredPersistentEntity(property));

		return createPotentiallyRequiredSchemaProperty(
				target.properties(nestedProperties.toArray(new JsonSchemaProperty[0])), required);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private JsonSchemaProperty createEnumSchemaProperty(String fieldName, Class<?> targetType, boolean required) {

		List<Object> possibleValues = getPossibleEnumValues((Class<Enum>) targetType);

		targetType = computeTargetType(targetType, possibleValues);
		return createSchemaProperty(fieldName, targetType, required, possibleValues);
	}

	JsonSchemaProperty createSchemaProperty(String fieldName, Object type, boolean required) {
		return createSchemaProperty(fieldName, type, required, Collections.emptyList());
	}

	JsonSchemaProperty createSchemaProperty(String fieldName, Object type, boolean required,
			Collection<?> possibleValues) {

		TypedJsonSchemaObject schemaObject = createSchemaObject(type, possibleValues);

		return createPotentiallyRequiredSchemaProperty(JsonSchemaProperty.named(fieldName).with(schemaObject), required);
	}

	private TypedJsonSchemaObject createSchemaObject(Object type, Collection<?> possibleValues) {

		TypedJsonSchemaObject schemaObject = type instanceof Type typeObject ? JsonSchemaObject.of(typeObject)
				: JsonSchemaObject.of(Class.class.cast(type));

		if (!CollectionUtils.isEmpty(possibleValues)) {
			schemaObject = schemaObject.possibleValues(possibleValues);
		}
		return schemaObject;
	}

	private String computePropertyFieldName(PersistentProperty<?> property) {

		return property instanceof MongoPersistentProperty  mongoPersistentProperty ?
				mongoPersistentProperty.getFieldName() : property.getName();
	}

	private boolean isRequiredProperty(PersistentProperty<?> property) {
		return property.getType().isPrimitive();
	}

	private Class<?> computeTargetType(PersistentProperty<?> property) {

		if (!(property instanceof MongoPersistentProperty mongoProperty)) {
			return property.getType();
		}

		if (!mongoProperty.isIdProperty()) {
			return mongoProperty.getFieldType();
		}

		if (mongoProperty.hasExplicitWriteTarget()) {
			return mongoProperty.getRequiredAnnotation(Field.class).targetType().getJavaClass();
		}

		return mongoProperty.getFieldType() != mongoProperty.getActualType() ? Object.class : mongoProperty.getFieldType();
	}

	private static Class<?> computeTargetType(Class<?> fallback, List<Object> possibleValues) {
		return possibleValues.isEmpty() ? fallback : possibleValues.iterator().next().getClass();
	}

	private <E extends Enum<E>> List<Object> getPossibleEnumValues(Class<E> targetType) {

		EnumSet<E> enumSet = EnumSet.allOf(targetType);
		List<Object> possibleValues = new ArrayList<>(enumSet.size());

		for (Object enumValue : enumSet) {
			possibleValues.add(converter.convertToMongoType(enumValue));
		}

		return possibleValues;
	}

	private static boolean isCollection(MongoPersistentProperty property) {
		return property.isCollectionLike() && !property.getType().equals(byte[].class);
	}

	static JsonSchemaProperty createPotentiallyRequiredSchemaProperty(JsonSchemaProperty property, boolean required) {
		return required ? JsonSchemaProperty.required(property) : property;
	}

	class PropertyContext implements JsonSchemaPropertyContext {

		private final String path;
		private final MongoPersistentProperty property;

		public PropertyContext(String path, MongoPersistentProperty property) {
			this.path = path;
			this.property = property;
		}

		@Override
		public String getPath() {
			return path;
		}

		@Override
		public MongoPersistentProperty getProperty() {
			return property;
		}

		@Override
		public <T> MongoPersistentEntity<T> resolveEntity(MongoPersistentProperty property) {
			return (MongoPersistentEntity<T>) mappingContext.getPersistentEntity(property);
		}
	}
}
