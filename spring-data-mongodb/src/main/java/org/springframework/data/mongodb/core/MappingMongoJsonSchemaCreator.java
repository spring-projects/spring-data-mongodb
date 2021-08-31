/*
 * Copyright 2019-2021 the original author or authors.
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
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.context.ApplicationContext;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.schema.IdentifiableJsonSchemaProperty.EncryptedJsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.IdentifiableJsonSchemaProperty.ObjectJsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type;
import org.springframework.data.mongodb.core.schema.JsonSchemaProperty;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema.MongoJsonSchemaBuilder;
import org.springframework.data.mongodb.core.schema.TypedJsonSchemaObject;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
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
	private final Predicate<JsonSchemaProperty> filter;

	/**
	 * Create a new instance of {@link MappingMongoJsonSchemaCreator}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	MappingMongoJsonSchemaCreator(MongoConverter converter) {

		this(converter, (MappingContext<MongoPersistentEntity<?>, MongoPersistentProperty>) converter.getMappingContext(),
				(property) -> true);
	}

	@SuppressWarnings("unchecked")
	MappingMongoJsonSchemaCreator(MongoConverter converter,
			MappingContext<MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
			Predicate<JsonSchemaProperty> filter) {

		Assert.notNull(converter, "Converter must not be null!");
		this.converter = converter;
		this.mappingContext = mappingContext;
		this.filter = filter;
	}

	@Override
	public MongoJsonSchemaCreator filter(Predicate<JsonSchemaProperty> filter) {
		return new MappingMongoJsonSchemaCreator(converter, mappingContext, filter);
	}

	/*
	 * (non-Javadoc)
	 * org.springframework.data.mongodb.core.MongoJsonSchemaCreator#createSchemaFor(java.lang.Class)
	 */
	@Override
	public MongoJsonSchema createSchemaFor(Class<?> type) {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);
		if(entity instanceof BasicMongoPersistentEntity) {
			((BasicMongoPersistentEntity<?>) entity).getEvaluationContext(null);
		}
		MongoJsonSchemaBuilder schemaBuilder = MongoJsonSchema.builder();

		if (entity.isAnnotationPresent(Encrypted.class)) {
			Encrypted encrypted = entity.findAnnotation(Encrypted.class);
			Document encryptionMetadata = new Document("keyId", objectToKeyId(entity.getEncryptionKeyIds()));
			if (StringUtils.hasText(encrypted.algorithm())) {
				encryptionMetadata.append("algorithm", encrypted.algorithm());
			}
			schemaBuilder.encryptionMetadata(encryptionMetadata);
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

			if (path.contains(nested)) { // cycle guard
				schemaProperties.add(createSchemaProperty(computePropertyFieldName(CollectionUtils.lastElement(currentPath)),
						Object.class, false));
				break;
			}

			currentPath.add(nested);
			schemaProperties.add(computeSchemaForProperty(currentPath));
		}

		// if(!encryptedFieldsOnly) {
		// return schemaProperties;
		// }
		return schemaProperties.stream().filter(filter).collect(Collectors.toList());
	}

	// private boolean containsEncrypted(JsonSchemaProperty property) {
	// if(property instanceof EncryptedJsonSchemaProperty) {
	// return true;
	// }
	//
	// if(property instanceof ObjectJsonSchemaProperty) {
	// ObjectJsonSchemaProperty val = (ObjectJsonSchemaProperty) property;
	// for(JsonSchemaProperty p : val.getProperties()) {
	// if(containsEncrypted(p) ) {
	// return true;
	// }
	// }
	// }
	//
	// return false;
	// }

	private JsonSchemaProperty computeSchemaForProperty(List<MongoPersistentProperty> path) {

		MongoPersistentProperty property = CollectionUtils.lastElement(path);

		boolean required = isRequiredProperty(property);
		Class<?> rawTargetType = computeTargetType(property); // target type before conversion
		Class<?> targetType = converter.getTypeMapper().getWriteTargetTypeFor(rawTargetType); // conversion target type

		if (property.isEntity() && ObjectUtils.nullSafeEquals(rawTargetType, targetType)) {
			return createObjectSchemaPropertyForEntity(path, property, required);
		}

		String fieldName = computePropertyFieldName(property);

		JsonSchemaProperty schemaProperty = null;
		if (property.isCollectionLike()) {
			schemaProperty = createSchemaProperty(fieldName, targetType, required);
		} else if (property.isMap()) {
			schemaProperty = createSchemaProperty(fieldName, Type.objectType(), required);
		} else if (ClassUtils.isAssignable(Enum.class, targetType)) {
			schemaProperty = createEnumSchemaProperty(fieldName, targetType, required);
		} else {
			schemaProperty = createSchemaProperty(fieldName, targetType, required);
		}

		if (property.findAnnotation(Encrypted.class) != null) {
			EncryptedJsonSchemaProperty enc = new EncryptedJsonSchemaProperty(schemaProperty);

			Encrypted annotation = property.findAnnotation(Encrypted.class);
			enc = enc.algorithm(annotation.algorithm());

			if (!ObjectUtils.isEmpty(annotation.keyId())) {
				enc.keys(objectToKeyId(property.getEncryptionKeyIds()));
			}
			return enc;
		}

		return schemaProperty;
	}

	private JsonSchemaProperty createObjectSchemaPropertyForEntity(List<MongoPersistentProperty> path,
			MongoPersistentProperty property, boolean required) {

		ObjectJsonSchemaProperty target = JsonSchemaProperty.object(property.getName());
		List<JsonSchemaProperty> nestedProperties = computePropertiesForEntity(path,
				mappingContext.getRequiredPersistentEntity(property));

		return createPotentiallyRequiredSchemaProperty(
				target.properties(nestedProperties.toArray(new JsonSchemaProperty[0])), required);
	}

	private JsonSchemaProperty createEnumSchemaProperty(String fieldName, Class<?> targetType, boolean required) {

		List<Object> possibleValues = new ArrayList<>();

		for (Object enumValue : EnumSet.allOf((Class) targetType)) {
			possibleValues.add(converter.convertToMongoType(enumValue));
		}

		targetType = possibleValues.isEmpty() ? targetType : possibleValues.iterator().next().getClass();
		return createSchemaProperty(fieldName, targetType, required, possibleValues);
	}

	JsonSchemaProperty createSchemaProperty(String fieldName, Object type, boolean required) {
		return createSchemaProperty(fieldName, type, required, Collections.emptyList());
	}

	JsonSchemaProperty createSchemaProperty(String fieldName, Object type, boolean required,
			Collection<?> possibleValues) {

		TypedJsonSchemaObject schemaObject = type instanceof Type ? JsonSchemaObject.of(Type.class.cast(type))
				: JsonSchemaObject.of(Class.class.cast(type));

		if (!CollectionUtils.isEmpty(possibleValues)) {
			schemaObject = schemaObject.possibleValues(possibleValues);
		}

		return createPotentiallyRequiredSchemaProperty(JsonSchemaProperty.named(fieldName).with(schemaObject), required);
	}

	private String computePropertyFieldName(PersistentProperty property) {

		return property instanceof MongoPersistentProperty ? ((MongoPersistentProperty) property).getFieldName()
				: property.getName();
	}

	private boolean isRequiredProperty(PersistentProperty property) {
		return property.getType().isPrimitive();
	}

	private Class<?> computeTargetType(PersistentProperty<?> property) {

		if (!(property instanceof MongoPersistentProperty)) {
			return property.getType();
		}

		MongoPersistentProperty mongoProperty = (MongoPersistentProperty) property;
		if (!mongoProperty.isIdProperty()) {
			return mongoProperty.getFieldType();
		}

		if (mongoProperty.hasExplicitWriteTarget()) {
			return mongoProperty.getRequiredAnnotation(Field.class).targetType().getJavaClass();
		}

		return mongoProperty.getFieldType() != mongoProperty.getActualType() ? Object.class : mongoProperty.getFieldType();
	}

	static JsonSchemaProperty createPotentiallyRequiredSchemaProperty(JsonSchemaProperty property, boolean required) {

		if (!required) {
			return property;
		}

		return JsonSchemaProperty.required(property);
	}

	private List<Object> objectToKeyId(Object[] values) {



		List<Object> target = new ArrayList<>();
		for (Object key : values) {

			if(key instanceof  UUID) {
				target.add(key);
				continue;
			}

			if(key instanceof String) {
				try {
					target.add(UUID.fromString((String)key));
				} catch (IllegalArgumentException e) {

					target.add(Document.parse("{ val : { $binary : { base64 : '" + key + "', subType : '04'} } }").get("val"));
					// target.add(UuidHelper.decodeBinaryToUuid(key.getBytes(StandardCharsets.UTF_8),
					// BsonBinarySubType.UUID_STANDARD.getValue(), UuidRepresentation.STANDARD));
					// BsonBinary
					// Document d = Document.parse()
					// target.add(UUID.nameUUIDFromBytes(Base64Utils.decodeFromString(key)));
					// target.add(new Document().append("$binary", new Document().append("base64", key).append("subType", "04")));
				}
				continue;
			}

			target.add(key);

		}
		return target;
	}

//	boolean isSpelExpression(String value) {
//
//		Expression expression = new SpelExpressionParser(new SpelParserConfiguration(null, this.getClass().getClassLoader())).parseExpression(value, ParserContext.TEMPLATE_EXPRESSION);
//		return expression instanceof LiteralExpression ? false : true;
//	}
//
//	Object evaluateSpelExpression(String path, String value) {
//
//		SpelExpression spelExpression = new SpelExpressionParser(new SpelParserConfiguration(null, this.getClass().getClassLoader())).parseRaw(value);
//
//		StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
//
//		if(mappingContext instanceof MongoMappingContext) {
//
//			ApplicationContext applicationContext = ((MongoMappingContext) mappingContext).getApplicationContext();
//			//evaluationContext.registerFunction();
//			evaluationContext.setBeanResolver(new BeanFactoryResolver(applicationContext));
//			// evaluationContext.setMethodResolvers();
//			evaluationContext.setVariable("target", path);
//		}
//
////		if (factory != null) {
////			evaluationContext.setBeanResolver(new BeanFactoryResolver(factory));
////		}
//
//		spelExpression.setEvaluationContext(evaluationContext);
//		return spelExpression.getValue();
//	}



}
