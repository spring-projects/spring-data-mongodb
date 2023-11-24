/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Reference;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.domain.Example;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.InvalidPersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.aggregation.AggregationExpression;
import org.springframework.data.mongodb.core.aggregation.RelaxedTypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter.NestedDocument;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty.PropertyToFieldNameConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.mongodb.util.DotPath;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

/**
 * A helper class to encapsulate any modifications of a Query object before it gets submitted to the database.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Patryk Wasik
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author David Julia
 * @author Divya Srivastava
 */
public class QueryMapper {

	protected static final Log LOGGER = LogFactory.getLog(QueryMapper.class);

	private static final List<String> DEFAULT_ID_NAMES = Arrays.asList("id", FieldName.ID.name());
	private static final Document META_TEXT_SCORE = new Document("$meta", "textScore");
	static final TypeInformation<?> NESTED_DOCUMENT = TypeInformation.of(NestedDocument.class);

	private enum MetaMapping {
		FORCE, WHEN_PRESENT, IGNORE
	}

	private final ConversionService conversionService;
	private final MongoConverter converter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final MongoExampleMapper exampleMapper;
	private final MongoJsonSchemaMapper schemaMapper;

	/**
	 * Creates a new {@link QueryMapper} with the given {@link MongoConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public QueryMapper(MongoConverter converter) {

		Assert.notNull(converter, "MongoConverter must not be null");

		this.conversionService = converter.getConversionService();
		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
		this.exampleMapper = new MongoExampleMapper(converter);
		this.schemaMapper = new MongoJsonSchemaMapper(converter);
	}

	public Document getMappedObject(Bson query, Optional<? extends MongoPersistentEntity<?>> entity) {
		return getMappedObject(query, entity.orElse(null));
	}

	/**
	 * Replaces the property keys used in the given {@link Document} with the appropriate keys by using the
	 * {@link PersistentEntity} metadata.
	 *
	 * @param query must not be {@literal null}.
	 * @param entity can be {@literal null}.
	 * @return
	 */
	public Document getMappedObject(Bson query, @Nullable MongoPersistentEntity<?> entity) {

		if (isNestedKeyword(query)) {
			return getMappedKeyword(new Keyword(query), entity);
		}

		Document result = new Document();

		for (String key : BsonUtils.asMap(query).keySet()) {

			// TODO: remove one once QueryMapper can work with Query instances directly
			if (Query.isRestrictedTypeKey(key)) {

				Set<Class<?>> restrictedTypes = BsonUtils.get(query, key);
				this.converter.getTypeMapper().writeTypeRestrictions(result, restrictedTypes);
				continue;
			}

			if (isTypeKey(key)) {
				result.put(key, BsonUtils.get(query, key));
				continue;
			}

			if (isKeyword(key)) {
				result.putAll(getMappedKeyword(new Keyword(query, key), entity));
				continue;
			}

			try {

				Field field = createPropertyField(entity, key, mappingContext);

				// TODO: move to dedicated method
				if (field.getProperty() != null && field.getProperty().isUnwrapped()) {

					Object theNestedObject = BsonUtils.get(query, key);
					Document mappedValue = (Document) getMappedValue(field, theNestedObject);
					if (!StringUtils.hasText(field.getMappedKey())) {
						result.putAll(mappedValue);
					} else {
						result.put(field.getMappedKey(), mappedValue);
					}
				} else {

					Entry<String, Object> entry = getMappedObjectForField(field, BsonUtils.get(query, key));

					/*
					 * Note to future self:
					 * ----
					 * This could be the place to plug in a query rewrite mechanism that allows to transform comparison
					 * against field that has a dot in its name (like 'a.b') into an $expr so that { "a.b" : "some value" }
					 * eventually becomes { $expr : { $eq : [ { $getField : "a.b" }, "some value" ] } }
					 * ----
					 */
					result.put(entry.getKey(), entry.getValue());
				}
			} catch (InvalidPersistentPropertyPath invalidPathException) {

				// in case the object has not already been mapped
				if (!(BsonUtils.get(query, key) instanceof Document)) {
					throw invalidPathException;
				}
				result.put(key, BsonUtils.get(query, key));
			}
		}

		return result;
	}

	/**
	 * Maps fields used for sorting to the {@link MongoPersistentEntity}s properties. <br />
	 * Also converts properties to their {@code $meta} representation if present.
	 *
	 * @param sortObject
	 * @param entity
	 * @return
	 * @since 1.6
	 */
	public Document getMappedSort(Document sortObject, @Nullable MongoPersistentEntity<?> entity) {

		Assert.notNull(sortObject, "SortObject must not be null");

		if (sortObject.isEmpty()) {
			return BsonUtils.EMPTY_DOCUMENT;
		}

		Document mappedSort = mapFieldsToPropertyNames(sortObject, entity);
		return mapMetaAttributes(mappedSort, entity, MetaMapping.WHEN_PRESENT);
	}

	/**
	 * Maps fields to retrieve to the {@link MongoPersistentEntity}s properties. <br />
	 * Also converts and potentially adds missing property {@code $meta} representation.
	 *
	 * @param fieldsObject must not be {@literal null}.
	 * @param entity can be {@literal null}.
	 * @return
	 * @since 1.6
	 */
	public Document getMappedFields(Document fieldsObject, @Nullable MongoPersistentEntity<?> entity) {

		Assert.notNull(fieldsObject, "FieldsObject must not be null");

		Document mappedFields = mapFieldsToPropertyNames(fieldsObject, entity);
		return mapMetaAttributes(mappedFields, entity, MetaMapping.FORCE);
	}

	private Document mapFieldsToPropertyNames(Document fields, @Nullable MongoPersistentEntity<?> entity) {

		if (fields.isEmpty()) {
			return BsonUtils.EMPTY_DOCUMENT;
		}

		Document target = new Document();

		BsonUtils.asMap(filterUnwrappedObjects(fields, entity)).forEach((k, v) -> {

			Field field = createPropertyField(entity, k, mappingContext);
			if (field.getProperty() != null && field.getProperty().isUnwrapped()) {
				return;
			}

			target.put(field.getMappedKey(), v);
		});

		return target;
	}

	/**
	 * Adds missing {@code $meta} representation if required.
	 *
	 * @param source must not be {@literal null}.
	 * @param entity can be {@literal null}.
	 * @return never {@literal null}.
	 * @since 3.4
	 */
	public Document addMetaAttributes(Document source, @Nullable MongoPersistentEntity<?> entity) {
		return mapMetaAttributes(source, entity, MetaMapping.FORCE);
	}

	private Document mapMetaAttributes(Document source, @Nullable MongoPersistentEntity<?> entity,
			MetaMapping metaMapping) {

		if (entity == null) {
			return source;
		}

		if (entity.hasTextScoreProperty() && !MetaMapping.IGNORE.equals(metaMapping)) {

			if (source == BsonUtils.EMPTY_DOCUMENT) {
				source = new Document();
			}

			MongoPersistentProperty textScoreProperty = entity.getTextScoreProperty();
			if (MetaMapping.FORCE.equals(metaMapping)
					|| (MetaMapping.WHEN_PRESENT.equals(metaMapping) && source.containsKey(textScoreProperty.getFieldName()))) {
				source.putAll(getMappedTextScoreField(textScoreProperty));
			}
		}

		return source;
	}

	private Document filterUnwrappedObjects(Document fieldsObject, @Nullable MongoPersistentEntity<?> entity) {

		if (fieldsObject.isEmpty() || entity == null) {
			return fieldsObject;
		}

		Document target = new Document();

		for (Entry<String, Object> field : fieldsObject.entrySet()) {

			try {

				PropertyPath path = PropertyPath.from(field.getKey(), entity.getTypeInformation());
				PersistentPropertyPath<MongoPersistentProperty> persistentPropertyPath = mappingContext
						.getPersistentPropertyPath(path);
				MongoPersistentProperty property = mappingContext.getPersistentPropertyPath(path).getLeafProperty();

				if (property.isUnwrapped() && property.isEntity()) {

					MongoPersistentEntity<?> unwrappedEntity = mappingContext.getRequiredPersistentEntity(property);

					for (MongoPersistentProperty unwrappedProperty : unwrappedEntity) {

						DotPath dotPath = DotPath.from(persistentPropertyPath.toDotPath()).append(unwrappedProperty.getName());
						target.put(dotPath.toString(), field.getValue());
					}

				} else {
					target.put(field.getKey(), field.getValue());
				}
			} catch (RuntimeException e) {
				target.put(field.getKey(), field.getValue());
			}

		}
		return target;
	}

	private Document getMappedTextScoreField(MongoPersistentProperty property) {
		return new Document(property.getFieldName(), META_TEXT_SCORE);
	}

	/**
	 * Extracts the mapped object value for given field out of rawValue taking nested {@link Keyword}s into account
	 *
	 * @param field
	 * @param rawValue
	 * @return
	 */
	protected Entry<String, Object> getMappedObjectForField(Field field, Object rawValue) {

		String key = field.getMappedKey();
		Object value;

		if (rawValue instanceof MongoExpression mongoExpression) {
			return createMapEntry(key, getMappedObject(mongoExpression.toDocument(), field.getEntity()));
		}

		if (isNestedKeyword(rawValue) && !field.isIdField()) {
			Keyword keyword = new Keyword((Document) rawValue);
			value = getMappedKeyword(field, keyword);
		} else {
			value = getMappedValue(field, rawValue);
		}

		return createMapEntry(key, value);
	}

	/**
	 * @param entity
	 * @param key
	 * @param mappingContext
	 * @return
	 */
	protected Field createPropertyField(@Nullable MongoPersistentEntity<?> entity, String key,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		if (entity == null) {
			return new Field(key);
		}

		if (FieldName.ID.name().equals(key)) {
			return new MetadataBackedField(key, entity, mappingContext, entity.getIdProperty());
		}

		return new MetadataBackedField(key, entity, mappingContext);
	}

	/**
	 * Returns the given {@link Document} representing a keyword by mapping the keyword's value.
	 *
	 * @param keyword the {@link Document} representing a keyword (e.g. {@code $ne : … } )
	 * @param entity
	 * @return
	 */
	protected Document getMappedKeyword(Keyword keyword, @Nullable MongoPersistentEntity<?> entity) {

		// $or/$nor
		if (keyword.isOrOrNor() || (keyword.hasIterableValue() && !keyword.isGeometry())) {

			Iterable<?> conditions = keyword.getValue();
			List<Object> newConditions = conditions instanceof Collection<?> collection ? new ArrayList<>(collection.size())
					: new ArrayList<>();

			for (Object condition : conditions) {
				newConditions.add(isDocument(condition) ? getMappedObject((Document) condition, entity)
						: convertSimpleOrDocument(condition, entity));
			}

			return new Document(keyword.getKey(), newConditions);
		}

		if (keyword.isSample()) {
			return exampleMapper.getMappedExample(keyword.getValue(), entity);
		}

		if (keyword.isJsonSchema()) {
			return schemaMapper.mapSchema(new Document(keyword.getKey(), keyword.getValue()),
					entity != null ? entity.getType() : Object.class);
		}

		return new Document(keyword.getKey(), convertSimpleOrDocument(keyword.getValue(), entity));
	}

	/**
	 * Returns the mapped keyword considered defining a criteria for the given property.
	 *
	 * @param property
	 * @param keyword
	 * @return
	 */
	protected Document getMappedKeyword(Field property, Keyword keyword) {

		boolean needsAssociationConversion = property.isAssociation() && !keyword.isExists() && keyword.mayHoldDbRef();
		Object value = keyword.getValue();

		Object convertedValue = needsAssociationConversion ? convertAssociation(value, property)
				: getMappedValue(property.with(keyword.getKey()), value);

		if (keyword.isSample() && convertedValue instanceof Document document) {
			return document;
		}

		return new Document(keyword.key, convertedValue);
	}

	/**
	 * Returns the mapped value for the given source object assuming it's a value for the given
	 * {@link MongoPersistentProperty}.
	 *
	 * @param documentField the key the value will be bound to eventually
	 * @param sourceValue the source object to be mapped
	 * @return
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	protected Object getMappedValue(Field documentField, Object sourceValue) {

		Object value = applyFieldTargetTypeHintToValue(documentField, sourceValue);

		if (documentField.getProperty() != null
				&& converter.getCustomConversions().hasValueConverter(documentField.getProperty())) {

			MongoConversionContext conversionContext = new MongoConversionContext(new PropertyValueProvider<>() {
				@Override
				public <T> T getPropertyValue(MongoPersistentProperty property) {
					throw new IllegalStateException("No enclosing property available");
				}
			}, documentField.getProperty(), converter);
			PropertyValueConverter<Object, Object, ValueConversionContext<MongoPersistentProperty>> valueConverter = converter
					.getCustomConversions().getPropertyValueConversions().getValueConverter(documentField.getProperty());

			/* might be an $in clause with multiple entries */
			if (!documentField.getProperty().isCollectionLike() && sourceValue instanceof Collection<?> collection) {
				return collection.stream().map(it -> valueConverter.write(it, conversionContext)).collect(Collectors.toList());
			}

			return valueConverter.write(value, conversionContext);
		}

		if (documentField.isIdField() && !documentField.isAssociation()) {

			if (isDBObject(value)) {
				DBObject valueDbo = (DBObject) value;
				Document resultDbo = new Document(valueDbo.toMap());

				if (valueDbo.containsField("$in") || valueDbo.containsField("$nin")) {
					String inKey = valueDbo.containsField("$in") ? "$in" : "$nin";
					List<Object> ids = new ArrayList<>();
					for (Object id : (Iterable<?>) valueDbo.get(inKey)) {
						ids.add(convertId(id, getIdTypeForField(documentField)));
					}
					resultDbo.put(inKey, ids);
				} else if (valueDbo.containsField("$ne")) {
					resultDbo.put("$ne", convertId(valueDbo.get("$ne"), getIdTypeForField(documentField)));
				} else {
					return getMappedObject(resultDbo, Optional.empty());
				}
				return resultDbo;
			}

			else if (isDocument(value)) {
				Document valueDbo = (Document) value;
				Document resultDbo = new Document(valueDbo);

				if (valueDbo.containsKey("$in") || valueDbo.containsKey("$nin")) {
					String inKey = valueDbo.containsKey("$in") ? "$in" : "$nin";
					List<Object> ids = new ArrayList<>();
					for (Object id : (Iterable<?>) valueDbo.get(inKey)) {
						ids.add(convertId(id, getIdTypeForField(documentField)));
					}
					resultDbo.put(inKey, ids);
				} else if (valueDbo.containsKey("$ne")) {
					resultDbo.put("$ne", convertId(valueDbo.get("$ne"), getIdTypeForField(documentField)));
				} else {
					return getMappedObject(resultDbo, Optional.empty());
				}
				return resultDbo;

			} else {
				return convertId(value, getIdTypeForField(documentField));
			}
		}

		if (value == null) {
			return null;
		}

		if (isNestedKeyword(value)) {
			return getMappedKeyword(new Keyword((Bson) value), documentField.getPropertyEntity());
		}

		if (isAssociationConversionNecessary(documentField, value)) {
			return convertAssociation(value, documentField);
		}

		return convertSimpleOrDocument(value, documentField.getPropertyEntity());
	}

	private boolean isIdField(Field documentField) {
		return documentField.getProperty() != null && documentField.getProperty().isIdProperty();
	}

	private Class<?> getIdTypeForField(Field documentField) {
		return isIdField(documentField) ? documentField.getProperty().getFieldType() : ObjectId.class;
	}

	/**
	 * Returns whether the given {@link Field} represents an association reference that together with the given value
	 * requires conversion to a {@link org.springframework.data.mongodb.core.mapping.DBRef} object. We check whether the
	 * type of the given value is compatible with the type of the given document field in order to deal with potential
	 * query field exclusions, since MongoDB uses the {@code int} {@literal 0} as an indicator for an excluded field.
	 *
	 * @param documentField must not be {@literal null}.
	 * @param value
	 * @return
	 */
	protected boolean isAssociationConversionNecessary(Field documentField, @Nullable Object value) {

		Assert.notNull(documentField, "Document field must not be null");

		if (value == null) {
			return false;
		}

		if (!documentField.isAssociation()) {
			return false;
		}

		Class<?> type = value.getClass();
		MongoPersistentProperty property = documentField.getProperty();

		if (property.getActualType().isAssignableFrom(type)) {
			return true;
		}

		if (property.isDocumentReference()) {
			return true;
		}

		MongoPersistentEntity<?> entity = documentField.getPropertyEntity();
		return entity.hasIdProperty()
				&& (type.equals(DBRef.class) || entity.getRequiredIdProperty().getActualType().isAssignableFrom(type));
	}

	/**
	 * Retriggers mapping if the given source is a {@link Document} or simply invokes the
	 *
	 * @param source
	 * @param entity
	 * @return
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	protected Object convertSimpleOrDocument(Object source, @Nullable MongoPersistentEntity<?> entity) {

		if (source instanceof Example<?> example) {
			return exampleMapper.getMappedExample(example, entity);
		}

		if (source instanceof AggregationExpression age) {
			return age
					.toDocument(new RelaxedTypeBasedAggregationOperationContext(entity.getType(), this.mappingContext, this));
		}

		if (source instanceof MongoExpression exr) {
			return exr.toDocument();
		}

		if (source instanceof List) {
			return delegateConvertToMongoType(source, entity);
		}

		if (isDocument(source)) {
			return getMappedObject((Document) source, entity);
		}

		if (source instanceof BasicDBList) {
			return delegateConvertToMongoType(source, entity);
		}

		if (isDBObject(source)) {
			return getMappedObject((BasicDBObject) source, entity);
		}

		if (source instanceof BsonValue) {
			return source;
		}

		if (source instanceof Map<?,?> sourceMap) {

			Map<String, Object> map = new LinkedHashMap<>(sourceMap.size(), 1F);

			sourceMap.entrySet().forEach(it -> {

				String key = ObjectUtils.nullSafeToString(converter.convertToMongoType(it.getKey()));

				if (it.getValue() instanceof Document document) {
					map.put(key, getMappedObject(document, entity));
				} else {
					map.put(key, delegateConvertToMongoType(it.getValue(), entity));
				}
			});

			return map;
		}

		return delegateConvertToMongoType(source, entity);
	}

	/**
	 * Converts the given source Object to a mongo type with the type information of the original source type omitted.
	 * Subclasses may overwrite this method to retain the type information of the source type on the resulting mongo type.
	 *
	 * @param source
	 * @param entity
	 * @return the converted mongo type or null if source is null
	 */
	@Nullable
	protected Object delegateConvertToMongoType(Object source, @Nullable MongoPersistentEntity<?> entity) {

		if (entity != null && entity.isUnwrapped()) {
			return converter.convertToMongoType(source, entity);
		}

		return converter.convertToMongoType(source, entity == null ? null : entity.getTypeInformation());
	}

	protected Object convertAssociation(Object source, Field field) {
		Object value = convertAssociation(source, field.getProperty());
		if (value != null && field.isIdField() && field.getFieldType() != value.getClass()) {
			return convertId(value, field.getFieldType());
		}
		return value;
	}

	/**
	 * Converts the given source assuming it's actually an association to another object.
	 *
	 * @param source
	 * @param property
	 * @return
	 */
	@Nullable
	protected Object convertAssociation(@Nullable Object source, @Nullable MongoPersistentProperty property) {

		if (property == null || source == null || source instanceof Document || source instanceof DBObject) {
			return source;
		}

		if (source instanceof DBRef ref) {

			Object id = convertId(ref.getId(),
					property != null && property.isIdProperty() ? property.getFieldType() : ObjectId.class);

			if (StringUtils.hasText(ref.getDatabaseName())) {
				return new DBRef(ref.getDatabaseName(), ref.getCollectionName(), id);
			} else {
				return new DBRef(ref.getCollectionName(), id);
			}
		}

		if (source instanceof Iterable<?> iterable) {
			BasicDBList result = new BasicDBList();
			for (Object element : iterable) {
				result.add(createReferenceFor(element, property));
			}
			return result;
		}

		if (property.isMap()) {
			Document result = new Document();
			Document dbObject = (Document) source;
			for (String key : dbObject.keySet()) {
				result.put(key, createReferenceFor(dbObject.get(key), property));
			}
			return result;
		}

		return createReferenceFor(source, property);
	}

	/**
	 * Checks whether the given value is a {@link Document}.
	 *
	 * @param value can be {@literal null}.
	 * @return
	 */
	protected final boolean isDocument(@Nullable Object value) {
		return value instanceof Document;
	}

	/**
	 * Checks whether the given value is a {@link DBObject}.
	 *
	 * @param value can be {@literal null}.
	 * @return
	 */
	protected final boolean isDBObject(@Nullable Object value) {
		return value instanceof DBObject;
	}

	/**
	 * Creates a new {@link Entry} for the given {@link Field} with the given value.
	 *
	 * @param field must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return
	 */
	protected final Entry<String, Object> createMapEntry(Field field, @Nullable Object value) {
		return createMapEntry(field.getMappedKey(), value);
	}

	/**
	 * Creates a new {@link Entry} with the given key and value.
	 *
	 * @param key must not be {@literal null} or empty.
	 * @param value can be {@literal null}.
	 * @return
	 */
	private Entry<String, Object> createMapEntry(String key, @Nullable Object value) {

		Assert.hasText(key, "Key must not be null or empty");
		return new AbstractMap.SimpleEntry<>(key, value);
	}

	private Object createReferenceFor(Object source, MongoPersistentProperty property) {

		if (source instanceof DBRef dbRef) {
			return dbRef;
		}

		if (property != null && (property.isDocumentReference()
				|| (!property.isDbReference() && property.findAnnotation(Reference.class) != null))) {
			return converter.toDocumentPointer(source, property).getPointer();
		}

		return converter.toDBRef(source, property);
	}

	/**
	 * Converts the given raw id value into either {@link ObjectId} or {@link String}.
	 *
	 * @param id
	 * @return
	 * @since 2.2
	 */
	@Nullable
	public Object convertId(@Nullable Object id) {
		return convertId(id, ObjectId.class);
	}

	/**
	 * Converts the given raw id value into either {@link ObjectId} or {@link Class targetType}.
	 *
	 * @param id can be {@literal null}.
	 * @param targetType
	 * @return the converted {@literal id} or {@literal null} if the source was already {@literal null}.
	 * @since 2.2
	 */
	@Nullable
	public Object convertId(@Nullable Object id, Class<?> targetType) {
		return converter.convertId(id, targetType);
	}

	/**
	 * Returns whether the given {@link Object} is a keyword, i.e. if it's a {@link Document} with a keyword key.
	 *
	 * @param candidate
	 * @return
	 */
	protected boolean isNestedKeyword(@Nullable Object candidate) {

		if (!(candidate instanceof Document)) {
			return false;
		}

		Map<String, Object> map = BsonUtils.asMap((Bson) candidate);

		if (map.size() != 1) {
			return false;
		}

		return isKeyword(map.entrySet().iterator().next().getKey());
	}

	/**
	 * Returns whether the given {@link String} is the type key.
	 *
	 * @param key
	 * @return
	 * @see MongoTypeMapper#isTypeKey(String)
	 * @since 2.2
	 */
	protected boolean isTypeKey(String key) {
		return converter.getTypeMapper().isTypeKey(key);
	}

	/**
	 * Returns whether the given {@link String} is a MongoDB keyword. The default implementation will check against the
	 * set of registered keywords.
	 *
	 * @param candidate
	 * @return
	 */
	protected boolean isKeyword(String candidate) {
		return candidate.startsWith("$");
	}

	/**
	 * Convert the given field value into its desired
	 * {@link org.springframework.data.mongodb.core.mapping.Field#targetType() target type} before applying further
	 * conversions. In case of a {@link Collection} (used eg. for {@code $in} queries) the individual values will be
	 * converted one by one.
	 *
	 * @param documentField the field and its meta data
	 * @param value the actual value. Can be {@literal null}.
	 * @return the potentially converted target value.
	 */
	@Nullable
	private Object applyFieldTargetTypeHintToValue(Field documentField, @Nullable Object value) {

		if (value == null || documentField.getProperty() == null || !documentField.getProperty().hasExplicitWriteTarget()
				|| value instanceof Document || value instanceof DBObject) {
			return value;
		}

		if (!conversionService.canConvert(value.getClass(), documentField.getProperty().getFieldType())) {
			return value;
		}

		if (value instanceof Collection<?> source) {

			Collection<Object> converted = new ArrayList<>(source.size());

			for (Object o : source) {
				converted.add(conversionService.convert(o, documentField.getProperty().getFieldType()));
			}

			return converted;
		}

		return conversionService.convert(value, documentField.getProperty().getFieldType());
	}

	/**
	 * Value object to capture a query keyword representation.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 */
	static class Keyword {

		private static final Set<String> NON_DBREF_CONVERTING_KEYWORDS = Set.of("$", "$size", "$slice", "$gt", "$lt");

		private final String key;
		private final Object value;

		public Keyword(Bson source, String key) {
			this.key = key;
			this.value = BsonUtils.get(source, key);
		}

		public Keyword(Bson bson) {

			Map<String, Object> map = BsonUtils.asMap(bson);
			Assert.isTrue(map.size() == 1, "Can only use a single value Document");

			Set<Entry<String, Object>> entries = map.entrySet();
			Entry<String, Object> entry = entries.iterator().next();

			this.key = entry.getKey();
			this.value = entry.getValue();
		}

		/**
		 * Returns whether the current keyword is the {@code $exists} keyword.
		 *
		 * @return
		 */
		public boolean isExists() {
			return "$exists".equalsIgnoreCase(key);
		}

		public boolean isOrOrNor() {
			return key.equalsIgnoreCase("$or") || key.equalsIgnoreCase("$nor");
		}

		/**
		 * Returns whether the current keyword is the {@code $geometry} keyword.
		 *
		 * @return
		 * @since 1.8
		 */
		public boolean isGeometry() {
			return "$geometry".equalsIgnoreCase(key);
		}

		/**
		 * Returns whether the current keyword indicates a {@link Example} object.
		 *
		 * @return
		 * @since 1.8
		 */
		public boolean isSample() {
			return "$example".equalsIgnoreCase(key);
		}

		public boolean hasIterableValue() {
			return value instanceof Iterable;
		}

		public String getKey() {
			return key;
		}

		@SuppressWarnings("unchecked")
		public <T> T getValue() {
			return (T) value;
		}

		/**
		 * @return {@literal true} if key may hold a DbRef.
		 * @since 2.1.4
		 */
		public boolean mayHoldDbRef() {
			return !NON_DBREF_CONVERTING_KEYWORDS.contains(key);
		}

		/**
		 * Returns whether the current keyword indicates a {@literal $jsonSchema} object.
		 *
		 * @return {@literal true} if {@code key} equals {@literal $jsonSchema}.
		 * @since 2.1
		 */
		public boolean isJsonSchema() {
			return "$jsonSchema".equalsIgnoreCase(key);
		}
	}

	/**
	 * Value object to represent a field and its meta-information.
	 *
	 * @author Oliver Gierke
	 */
	protected static class Field {

		protected static final Pattern POSITIONAL_OPERATOR = Pattern.compile("\\$\\[.*\\]");

		protected final String name;

		/**
		 * Creates a new {@link Field} without meta-information but the given name.
		 *
		 * @param name must not be {@literal null} or empty.
		 */
		public Field(String name) {

			Assert.hasText(name, "Name must not be null");
			this.name = name;
		}

		/**
		 * Returns a new {@link Field} with the given name.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @return
		 */
		public Field with(String name) {
			return new Field(name);
		}

		/**
		 * Returns whether the current field is the id field.
		 *
		 * @return
		 */
		public boolean isIdField() {
			return FieldName.ID.name().equals(name);
		}

		/**
		 * Returns the underlying {@link MongoPersistentProperty} backing the field. For path traversals this will be the
		 * property that represents the value to handle. This means it'll be the leaf property for plain paths or the
		 * association property in case we refer to an association somewhere in the path.
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		public MongoPersistentProperty getProperty() {
			return null;
		}

		/**
		 * Returns the {@link MongoPersistentEntity} that field is conatined in.
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		public MongoPersistentEntity<?> getPropertyEntity() {
			return null;
		}

		@Nullable
		MongoPersistentEntity<?> getEntity() {
			return null;
		}

		/**
		 * Returns whether the field represents an association.
		 *
		 * @return
		 */
		public boolean isAssociation() {
			return false;
		}

		/**
		 * Returns the key to be used in the mapped document eventually.
		 *
		 * @return
		 */
		public String getMappedKey() {
			return isIdField() ? FieldName.ID.name() : name;
		}

		/**
		 * Returns whether the field references an association in case it refers to a nested field.
		 *
		 * @return
		 */
		public boolean containsAssociation() {
			return false;
		}

		@Nullable
		public Association<MongoPersistentProperty> getAssociation() {
			return null;
		}

		/**
		 * Returns whether the field references a {@link java.util.Map}.
		 *
		 * @return {@literal true} if property information is available and references a {@link java.util.Map}.
		 * @see PersistentProperty#isMap()
		 */
		public boolean isMap() {
			return getProperty() != null && getProperty().isMap();
		}

		public TypeInformation<?> getTypeHint() {
			return TypeInformation.OBJECT;
		}

		public Class<?> getFieldType() {
			return Object.class;
		}
	}

	/**
	 * Extension of {@link Field} to be backed with mapping metadata.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	protected static class MetadataBackedField extends Field {

		private static final Pattern POSITIONAL_PARAMETER_PATTERN = Pattern.compile("\\.\\$(\\[.*?\\])?");
		private static final Pattern NUMERIC_SEGMENT = Pattern.compile("\\d+");
		private static final String INVALID_ASSOCIATION_REFERENCE = "Invalid path reference %s; Associations can only be pointed to directly or via their id property";

		private final MongoPersistentEntity<?> entity;
		private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
		private final MongoPersistentProperty property;
		private final @Nullable PersistentPropertyPath<MongoPersistentProperty> path;
		private final @Nullable Association<MongoPersistentProperty> association;

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link MongoPersistentEntity} and
		 * {@link MappingContext}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 */
		public MetadataBackedField(String name, MongoPersistentEntity<?> entity,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context) {
			this(name, entity, context, null);
		}

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link MongoPersistentEntity} and
		 * {@link MappingContext} with the given {@link MongoPersistentProperty}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 * @param property may be {@literal null}.
		 */
		public MetadataBackedField(String name, MongoPersistentEntity<?> entity,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context,
				@Nullable MongoPersistentProperty property) {

			super(name);

			Assert.notNull(entity, "MongoPersistentEntity must not be null");

			this.entity = entity;
			this.mappingContext = context;

			this.path = getPath(removePlaceholders(POSITIONAL_PARAMETER_PATTERN, name), property);
			this.property = path == null ? property : path.getLeafProperty();
			this.association = findAssociation();
		}

		@Override
		public MetadataBackedField with(String name) {
			return new MetadataBackedField(name, entity, mappingContext, property);
		}

		@Override
		public boolean isIdField() {

			if (property != null) {
				return property.isIdProperty();
			}

			MongoPersistentProperty idProperty = entity.getIdProperty();

			if (idProperty != null) {
				return name.equals(idProperty.getName()) || name.equals(idProperty.getFieldName());
			}

			return DEFAULT_ID_NAMES.contains(name);
		}

		@Override
		public MongoPersistentProperty getProperty() {
			return association == null ? property : association.getInverse();
		}

		@Override
		public MongoPersistentEntity<?> getPropertyEntity() {
			MongoPersistentProperty property = getProperty();
			return property == null ? null : mappingContext.getPersistentEntity(property);
		}

		@Nullable
		@Override
		public MongoPersistentEntity<?> getEntity() {
			return entity;
		}

		@Override
		public boolean isAssociation() {
			return association != null;
		}

		@Override
		public Association<MongoPersistentProperty> getAssociation() {
			return association;
		}

		/**
		 * Finds the association property in the {@link PersistentPropertyPath}.
		 *
		 * @return
		 */
		@Nullable
		private Association<MongoPersistentProperty> findAssociation() {

			if (this.path != null) {
				for (MongoPersistentProperty p : this.path) {

					Association<MongoPersistentProperty> association = p.getAssociation();

					if (association != null) {
						return association;
					}
				}
			}

			return null;
		}

		@Override
		public Class<?> getFieldType() {
			return property.getFieldType();
		}

		@Override
		public String getMappedKey() {

			if (getProperty() != null && getProperty().getMongoField().getName().isKey()) {
				return getProperty().getFieldName();
			}

			return path == null ? name : path.toDotPath(isAssociation() ? getAssociationConverter() : getPropertyConverter());
		}

		@Nullable
		protected PersistentPropertyPath<MongoPersistentProperty> getPath() {
			return path;
		}

		/**
		 * Returns the {@link PersistentPropertyPath} for the given {@code pathExpression}.
		 *
		 * @param pathExpression
		 * @return
		 */
		@Nullable
		private PersistentPropertyPath<MongoPersistentProperty> getPath(String pathExpression,
				@Nullable MongoPersistentProperty sourceProperty) {

			if (sourceProperty != null && sourceProperty.getOwner().equals(entity)) {
				return mappingContext.getPersistentPropertyPath(
						PropertyPath.from(Pattern.quote(sourceProperty.getName()), entity.getTypeInformation()));
			}

			String rawPath = resolvePath(pathExpression);

			PropertyPath path = forName(rawPath);
			if (path == null || isPathToJavaLangClassProperty(path)) {
				return null;
			}

			PersistentPropertyPath<MongoPersistentProperty> propertyPath = tryToResolvePersistentPropertyPath(path);

			if (propertyPath == null) {

				if (QueryMapper.LOGGER.isInfoEnabled()) {

					String types = StringUtils.collectionToDelimitedString(
							path.stream().map(it -> it.getType().getSimpleName()).collect(Collectors.toList()), " -> ");
					QueryMapper.LOGGER.info(String.format(
							"Could not map '%s'; Maybe a fragment in '%s' is considered a simple type; Mapper continues with %s",
							path, types, pathExpression));
				}
				return null;
			}

			Iterator<MongoPersistentProperty> iterator = propertyPath.iterator();
			boolean associationDetected = false;

			while (iterator.hasNext()) {

				MongoPersistentProperty property = iterator.next();

				if (property.isAssociation()) {
					associationDetected = true;
					continue;
				}

				if (associationDetected && !property.isIdProperty()) {
					throw new MappingException(String.format(INVALID_ASSOCIATION_REFERENCE, pathExpression));
				}
			}

			return propertyPath;
		}

		@Nullable
		private PersistentPropertyPath<MongoPersistentProperty> tryToResolvePersistentPropertyPath(PropertyPath path) {

			try {
				return mappingContext.getPersistentPropertyPath(path);
			} catch (MappingException e) {
				return null;
			}
		}

		/**
		 * Querydsl happens to map id fields directly to {@literal _id} which breaks {@link PropertyPath} resolution. So if
		 * the first attempt fails we try to replace {@literal _id} with just {@literal id} and see if we can resolve if
		 * then.
		 *
		 * @param path
		 * @return the path or {@literal null}
		 */
		@Nullable
		private PropertyPath forName(String path) {

			try {

				if (entity.getPersistentProperty(path) != null) {
					return PropertyPath.from(Pattern.quote(path), entity.getTypeInformation());
				}

				return PropertyPath.from(path, entity.getTypeInformation());
			} catch (PropertyReferenceException | InvalidPersistentPropertyPath e) {

				if (path.endsWith("_id")) {
					return forName(path.substring(0, path.length() - 3) + "id");
				}

				// Ok give it another try quoting
				try {
					return PropertyPath.from(Pattern.quote(path), entity.getTypeInformation());
				} catch (PropertyReferenceException | InvalidPersistentPropertyPath ex) {

				}

				return null;
			}
		}

		private boolean isPathToJavaLangClassProperty(PropertyPath path) {

			return (path.getType() == Class.class || path.getType().equals(Object.class))
					&& path.getLeafProperty().getType() == Class.class;
		}

		private static String resolvePath(String source) {

			String[] segments = source.split("\\.");
			if (segments.length == 1) {
				return source;
			}

			List<String> path = new ArrayList<>(segments.length);

			/* always start from a property, so we can skip the first segment.
			   from there remove any position placeholder */
			for (int i = 1; i < segments.length; i++) {
				String segment = segments[i];
				if (segment.startsWith("[") && segment.endsWith("]")) {
					continue;
				}
				if (NUMERIC_SEGMENT.matcher(segment).matches()) {
					continue;
				}
				path.add(segment);
			}

			// when property is followed only by placeholders eg. 'values.0.3.90'
			// or when there is no difference in the number of segments
			if (path.isEmpty() || segments.length == path.size() + 1) {
				return source;
			}

			path.add(0, segments[0]);
			return StringUtils.collectionToDelimitedString(path, ".");
		}

		/**
		 * Return the {@link Converter} to be used to created the mapped key. Default implementation will use
		 * {@link PropertyToFieldNameConverter}.
		 *
		 * @return
		 */
		protected Converter<MongoPersistentProperty, String> getPropertyConverter() {
			return new PositionParameterRetainingPropertyKeyConverter(name, mappingContext);
		}

		/**
		 * Return the {@link Converter} to use for creating the mapped key of an association. Default implementation is
		 * {@link AssociationConverter}.
		 *
		 * @return
		 * @since 1.7
		 */
		protected Converter<MongoPersistentProperty, String> getAssociationConverter() {
			return new AssociationConverter(name, getAssociation());
		}

		protected MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> getMappingContext() {
			return mappingContext;
		}

		private static String removePlaceholders(Pattern pattern, String raw) {
			return pattern.matcher(raw).replaceAll("");
		}

		/**
		 * @author Christoph Strobl
		 * @since 1.8
		 */
		static class PositionParameterRetainingPropertyKeyConverter implements Converter<MongoPersistentProperty, String> {

			private final KeyMapper keyMapper;

			public PositionParameterRetainingPropertyKeyConverter(String rawKey,
					MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> ctx) {
				this.keyMapper = new KeyMapper(rawKey, ctx);
			}

			@Override
			public String convert(MongoPersistentProperty source) {
				return keyMapper.mapPropertyName(source);
			}
		}

		@Override
		public TypeInformation<?> getTypeHint() {

			MongoPersistentProperty property = getProperty();

			if (property == null) {
				return super.getTypeHint();
			}

			if (property.getActualType().isInterface()
					|| java.lang.reflect.Modifier.isAbstract(property.getActualType().getModifiers())) {
				return TypeInformation.OBJECT;
			}

			return NESTED_DOCUMENT;
		}

		/**
		 * @author Christoph Strobl
		 * @since 1.8
		 */
		static class KeyMapper {

			private final Iterator<String> iterator;
			private int currentIndex;
			private String currentPropertyRoot;
			private final List<String> pathParts;

			public KeyMapper(String key,
					MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

				this.pathParts = Arrays.asList(key.split("\\."));
				this.iterator = pathParts.iterator();
				this.currentPropertyRoot = iterator.next();
				this.currentIndex = 0;
			}

			String nextToken() {
				return pathParts.get(currentIndex + 1);
			}

			boolean hasNexToken() {
				return pathParts.size() > currentIndex + 1;
			}

			/**
			 * Maps the property name while retaining potential positional operator {@literal $}.
			 *
			 * @param property
			 * @return
			 */
			protected String mapPropertyName(MongoPersistentProperty property) {

				StringBuilder mappedName = new StringBuilder(PropertyToFieldNameConverter.INSTANCE.convert(property));
				if (!hasNexToken()) {
					return mappedName.toString();
				}

				String nextToken = nextToken();
				if (isPositionalParameter(nextToken)) {

					mappedName.append(".").append(nextToken);
					currentIndex += 2;
					return mappedName.toString();
				}

				if (property.isMap()) {

					mappedName.append(".").append(nextToken);
					currentIndex += 2;
					return mappedName.toString();
				}

				currentIndex++;
				return mappedName.toString();
			}

			static boolean isPositionalParameter(String partial) {

				if ("$".equals(partial)) {
					return true;
				}

				Matcher matcher = POSITIONAL_OPERATOR.matcher(partial);
				if (matcher.find()) {
					return true;
				}

				try {
					Long.valueOf(partial);
					return true;
				} catch (NumberFormatException e) {
					return false;
				}
			}
		}
	}

	/**
	 * Converter to skip all properties after an association property was rendered.
	 *
	 * @author Oliver Gierke
	 */
	protected static class AssociationConverter implements Converter<MongoPersistentProperty, String> {

		private final String name;
		private final MongoPersistentProperty property;
		private boolean associationFound;

		/**
		 * Creates a new {@link AssociationConverter} for the given {@link Association}.
		 *
		 * @param association must not be {@literal null}.
		 */
		public AssociationConverter(String name, Association<MongoPersistentProperty> association) {

			Assert.notNull(association, "Association must not be null");
			this.property = association.getInverse();
			this.name = name;
		}

		@Override
		public String convert(MongoPersistentProperty source) {

			if (associationFound) {
				return null;
			}

			if (property.equals(source)) {
				associationFound = true;
			}

			if (associationFound) {
				if (name.endsWith("$") && property.isCollectionLike()) {
					return source.getFieldName() + ".$";
				}
			}

			return source.getFieldName();
		}
	}

	public MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> getMappingContext() {
		return mappingContext;
	}

	public MongoConverter getConverter() {
		return converter;
	}
}
