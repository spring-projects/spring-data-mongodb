/*
 * Copyright 2011-present the original author or authors.
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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BsonRegularExpression;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.jspecify.annotations.Nullable;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Reference;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.core.PropertyPath;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.domain.Example;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.InvalidPersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.aggregation.AggregationExpression;
import org.springframework.data.mongodb.core.aggregation.FieldLookupPolicy;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter.NestedDocument;
import org.springframework.data.mongodb.core.convert.MongoConversionContext.OperatorContext;
import org.springframework.data.mongodb.core.convert.MongoConversionContext.QueryOperatorContext;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty.PropertyToFieldNameConverter;
import org.springframework.data.mongodb.core.mapping.path.BoundMongoPath;
import org.springframework.data.mongodb.core.mapping.path.BoundMongoPath.Segment;
import org.springframework.data.mongodb.core.mapping.path.MongoPath;
import org.springframework.data.mongodb.core.mapping.path.MongoPathBinder;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PathType;
import org.springframework.data.mongodb.core.mapping.path.Scope;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.mongodb.util.DotPath;
import org.springframework.lang.Contract;
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
 * @author Gyungrai Wang
 * @author Ross Lawley
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
	@SuppressWarnings("NullAway")
	public Document getMappedObject(Bson query, @Nullable MongoPersistentEntity<?> entity) {

		if (isNestedKeyword(query)) {
			return getMappedKeyword(new Keyword(query), entity);
		}

		return mapKeys(query, entity, key -> createPropertyField(entity, key, mappingContext), null);
	}

	/**
	 * Replaces the property keys used in the given {@link Bson query} by resolving them against the given {@link Scope}.
	 *
	 * @param query must not be {@literal null}.
	 * @param scope must not be {@literal null}.
	 * @return the mapped {@link Document}.
	 * @since 5.3
	 */
	public Document getMappedObject(Bson query, Scope scope) {

		Assert.notNull(scope, "Scope must not be null");

		if (scope instanceof Scope.EntityScope entityScope) {
			return getMappedObject(query, entityScope.entity());
		}

		if (scope instanceof Scope.NoScope) {
			return getMappedObject(query, (MongoPersistentEntity<?>) null);
		}

		return mapKeys(query, null, key -> MetadataBackedField.forScope(key, scope, mappingContext), scope);
	}

	@SuppressWarnings("NullAway")
	private Document mapKeys(Bson query, @Nullable MongoPersistentEntity<?> entity, Function<String, Field> fields,
			@Nullable Scope scope) {

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

				Keyword keyword = new Keyword(query, key);
				if (scope != null && keyword.isLogicalOperator()) {
					result.put(key, mapConditions(keyword.getValue(), scope));
				} else {
					result.putAll(getMappedKeyword(keyword, entity));
				}
				continue;
			}

			try {

				Field field = fields.apply(key);

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

	@SuppressWarnings("NullAway")
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
		MongoPathBinder binder = MongoPathBinder.of(mappingContext);

		for (Entry<String, Object> field : fieldsObject.entrySet()) {

			BoundMongoPath path;
			try {
				path = binder.bind(field.getKey(), PathType.PROPERTY, Scope.of(entity));
			} catch (MappingException ex) {
				target.put(field.getKey(), field.getValue());
				continue;
			}

			if (path.isResolved() && path.leaf() instanceof Segment.Property leaf && leaf.isUnwrapped()
					&& leaf.property().isEntity()) {

				MongoPersistentEntity<?> unwrappedEntity = mappingContext.getRequiredPersistentEntity(leaf.property());
				DotPath dotPath = DotPath.from(path.properties().stream().map(MongoPersistentProperty::getName)
						.collect(Collectors.joining(".")));

				for (MongoPersistentProperty unwrappedProperty : unwrappedEntity) {
					target.put(dotPath.append(unwrappedProperty.getName()).toString(), field.getValue());
				}
			} else {
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
	protected Entry<String, @Nullable Object> getMappedObjectForField(Field field, @Nullable Object rawValue) {

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
			return new MetadataBackedField(key, entity, mappingContext, entity.getIdProperty(), getPathType());
		}

		return new MetadataBackedField(key, entity, mappingContext, null, getPathType());
	}

	/**
	 * @return the {@link PathType} keys of mapped documents are parsed as. Defaults to {@link PathType#QUERY}.
	 * @since 5.3
	 */
	protected PathType getPathType() {
		return PathType.QUERY;
	}

	private List<Object> mapConditions(Iterable<?> conditions, Scope scope) {

		List<Object> result = new ArrayList<>();
		for (Object condition : conditions) {
			result.add(condition instanceof Bson bson ? getMappedObject(bson, scope) : condition);
		}
		return result;
	}

	/**
	 * Map the keys of a {@link Document} nested in the value of {@code parent} using
	 * {@link #createNestedPropertyField(Field, String)}.
	 *
	 * @param document the nested document.
	 * @param parent the field holding the nested document.
	 * @return the mapped document.
	 * @since 5.3
	 */
	protected Document getMappedNestedObject(Document document, Field parent) {

		if (isNestedKeyword(document)) {
			return getMappedKeyword(new Keyword(document), parent.getPropertyEntity());
		}

		return mapKeys(document, parent.getPropertyEntity(), key -> createNestedPropertyField(parent, key), null);
	}

	/**
	 * Create the {@link Field} for a {@code key} of a document nested in the value of {@code parent}. The default
	 * implementation resolves the key relative to the path of {@code parent} if possible and falls back to
	 * {@link #createPropertyField(MongoPersistentEntity, String, MappingContext)} using the
	 * {@link Field#getPropertyEntity() parent property entity}.
	 *
	 * @param parent the field holding the nested document.
	 * @param key the key within the nested document.
	 * @return the {@link Field} for the nested key.
	 * @since 5.3
	 */
	protected Field createNestedPropertyField(Field parent, String key) {

		if (parent instanceof MetadataBackedField metadataBackedField) {

			Field nested = metadataBackedField.nested(key);
			if (nested != null) {
				return nested;
			}
		}

		return createPropertyField(parent.getPropertyEntity(), key, mappingContext);
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

			Example<?> example = keyword.getValue();
			return exampleMapper.getMappedExample(example,
					entity != null ? entity : mappingContext.getRequiredPersistentEntity(example.getProbeType()));
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
	@SuppressWarnings("NullAway")
	protected Object getMappedValue(Field documentField, @Nullable Object sourceValue) {

		Object value = applyFieldTargetTypeHintToValue(documentField, sourceValue);

		if (documentField.getProperty() != null
				&& converter.getCustomConversions().hasValueConverter(documentField.getProperty())) {

			PropertyValueConverter<Object, Object, ValueConversionContext<MongoPersistentProperty>> valueConverter = converter
					.getCustomConversions().getPropertyValueConversions().getValueConverter(documentField.getProperty());

			return convertValue(documentField, sourceValue, value, valueConverter);
		}

		if (documentField.isIdField() && !documentField.isAssociation()) {
			return convertIdField(documentField, value);
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

		if (isDocument(value)) {
			return getMappedNestedObject((Document) value, documentField);
		}

		return convertSimpleOrDocument(value, documentField.getPropertyEntity());
	}

	private boolean isIdField(Field documentField) {
		return documentField.getProperty() != null
				&& documentField.getProperty().getOwner().isIdProperty(documentField.getProperty());
	}

	@SuppressWarnings("NullAway")
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

		if (value == null || documentField.getProperty() == null) {
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
		return entity != null && entity.hasIdProperty()
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
	protected Object convertSimpleOrDocument(Object source, @Nullable MongoPersistentEntity<?> entity) {

		if (source instanceof Example<?> example) {
			return exampleMapper.getMappedExample(example, entity);
		}

		if (source instanceof AggregationExpression age) {
			return entity == null ? age.toDocument() : //
					age.toDocument(new TypeBasedAggregationOperationContext(entity.getType(), this.mappingContext, this,
							FieldLookupPolicy.relaxed()));
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

		if (isDBObject(source)) {
			return getMappedObject((BasicDBObject) source, entity);
		}

		if (source instanceof BsonValue) {
			return source;
		}

		if (source instanceof Map<?, ?> sourceMap) {

			Map<String, Object> map = new LinkedHashMap<>(sourceMap.size(), 1F);

			for (Entry<?, ?> entry : sourceMap.entrySet()) {

				String key = ObjectUtils.nullSafeToString(converter.convertToMongoType(entry.getKey()));

				if (entry.getValue() instanceof Document document) {
					map.put(key, getMappedObject(document, entity));
				} else {
					map.put(key, delegateConvertToMongoType(entry.getValue(), entity));
				}
			}

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

	@Nullable
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
					property.getOwner().isIdProperty(property) ? property.getFieldType() : ObjectId.class);

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

		if (property.isMap() && source instanceof Document dbObject) {
			Document result = new Document();
			for (String key : dbObject.keySet()) {
				result.put(key, createReferenceFor(dbObject.get(key), property));
			}
			return result;
		}

		return createReferenceFor(source, property);
	}

	private @Nullable Object convertValue(Field documentField, @Nullable Object sourceValue, @Nullable Object value,
			PropertyValueConverter<Object, Object, ValueConversionContext<MongoPersistentProperty>> valueConverter) {

		MongoPersistentProperty property = documentField.getProperty();

		OperatorContext criteriaContext = new QueryOperatorContext(
				isKeyword(documentField.name) ? documentField.name : "$eq",
				property != null ? property.getFieldName() : documentField.name);

		MongoConversionContext conversionContext;
		if (valueConverter instanceof MongoConversionContext mcc) {
			conversionContext = mcc.forOperator(criteriaContext);
		} else {
			conversionContext = new MongoConversionContext(NoPropertyPropertyValueProvider.INSTANCE, property, converter,
					criteriaContext);
		}

		return convertValueWithConversionContext(documentField, sourceValue, value, valueConverter, conversionContext);
	}

	@SuppressWarnings("NullAway")
	protected @Nullable Object convertValueWithConversionContext(Field documentField, @Nullable Object sourceValue,
			@Nullable Object value,
			PropertyValueConverter<Object, Object, ValueConversionContext<MongoPersistentProperty>> valueConverter,
			MongoConversionContext conversionContext) {

		MongoPersistentProperty property = documentField.getProperty();
		/* might be an $in clause with multiple entries */
		if (property != null && !property.isCollectionLike() && sourceValue instanceof Collection<?> collection) {

			if (collection.isEmpty()) {
				return collection;
			}

			List<Object> converted = new ArrayList<>(collection.size());
			for (Object o : collection) {
				converted.add(valueConverter.write(o, conversionContext));
			}

			return converted;
		}

		if (property != null && !property.isMap() && sourceValue instanceof Document document) {

			return BsonUtils.mapValues(document, (key, val) -> {
				if (isKeyword(key)) {
					return convertValueWithConversionContext(documentField, val, val, valueConverter, conversionContext
							.forOperator(new QueryOperatorContext(key, conversionContext.getOperatorContext().path())));
				}
				return val;
			});
		}

		return value != null ? valueConverter.write(value, conversionContext) : value;
	}

	@Nullable
	@SuppressWarnings({ "unchecked", "NullAway" })
	private Object convertIdField(Field documentField, Object source) {

		Object value = source;
		if (isDBObject(source)) {
			DBObject valueDbo = (DBObject) source;
			value = new Document(valueDbo.toMap());
		}

		if (!isDocument(value)) {
			return convertId(value, getIdTypeForField(documentField));
		}

		Document valueDbo = (Document) value;
		Document resultDbo = new Document(valueDbo);

		for (Entry<String, Object> entry : valueDbo.entrySet()) {

			String key = entry.getKey();
			if ("$nin".equals(key) || "$in".equals(key) || "$all".equals(key)) {
				List<Object> ids = new ArrayList<>();
				for (Object id : (Iterable<?>) valueDbo.get(key)) {
					ids.add(convertId(id, getIdTypeForField(documentField)));
				}
				resultDbo.put(key, ids);
			} else if (isKeyword(key)) {
				resultDbo.put(key, convertIdField(documentField, entry.getValue()));
			} else {
				if (documentField.getProperty() != null && documentField.getProperty().isEntity()) {
					Field propertyField = createNestedPropertyField(documentField, key);
					resultDbo.put(key, getMappedValue(propertyField, entry.getValue()));
				} else {
					resultDbo.put(key, getMappedValue(documentField, entry.getValue()));
				}
			}
		}

		return resultDbo;
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

		if (Quirks.skipConversion(id)) {
			return id;
		}

		return converter.convertId(id, targetType);
	}

	/**
	 * Returns whether the given {@link Object} is a keyword, i.e. if it's a {@link Document} with a keyword key.
	 *
	 * @param candidate
	 * @return
	 */
	@Contract("null -> false")
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
	protected boolean isKeyword(@Nullable String candidate) {
		return candidate != null && candidate.startsWith("$");
	}

	/**
	 * Convert the given field value into its desired
	 * {@link org.springframework.data.mongodb.core.mapping.Field#targetType() target type} before applying further
	 * conversions. In case of a {@link Collection} (used eg. for {@code $in} queries) the individual values will be
	 * converted one by one.
	 *
	 * @param documentField the field and its metadata
	 * @param value the actual value. Can be {@literal null}.
	 * @return the potentially converted target value.
	 */
	@Nullable
	private Object applyFieldTargetTypeHintToValue(Field documentField, @Nullable Object value) {

		if (value == null || documentField.getProperty() == null || !documentField.getProperty().hasExplicitWriteTarget()
				|| value instanceof Document || value instanceof DBObject || Quirks.skipConversion(value)) {
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
	@SuppressWarnings("NullAway")
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
		 * @return {@literal true} if the keyword is {@code $and}, {@code $or} or {@code $nor} holding a list of
		 *         conditions.
		 */
		boolean isLogicalOperator() {
			return (isOrOrNor() || key.equalsIgnoreCase("$and")) && value instanceof Iterable;
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

		/**
		 * @deprecated since 5.3, positional operators are recognized by {@link MongoPath}.
		 */
		@Deprecated(since = "5.3")
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
	public static class MetadataBackedField extends Field {

		private final @Nullable MongoPersistentEntity<?> entity;
		private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
		private final @Nullable BoundMongoPath boundPath;
		private final int offset;
		private final @Nullable String keyPrefix;
		private final PathType pathType;
		private final MongoPersistentProperty property;
		private final @Nullable Association<MongoPersistentProperty> association;
		private @Nullable PersistentPropertyPath<MongoPersistentProperty> path;
		private boolean pathResolved;

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
			this(name, entity, context, property, PathType.QUERY);
		}

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link MongoPersistentEntity} and
		 * {@link MappingContext} with the given {@link MongoPersistentProperty}, parsing {@code name} as {@code pathType}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 * @param property may be {@literal null}.
		 * @param pathType must not be {@literal null}.
		 * @since 5.3
		 */
		@SuppressWarnings("NullAway")
		public MetadataBackedField(String name, MongoPersistentEntity<?> entity,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context,
				@Nullable MongoPersistentProperty property, PathType pathType) {

			this(name, entity, context, bind(name, entity, context, property, pathType), 0, null, property, pathType);
		}

		/**
		 * @param boundPath the full path, including segments of parent fields.
		 * @param offset index of the first segment rendered by {@link #getMappedKey()}.
		 * @param keyPrefix rendered in front of the mapped key, e.g. an array filter identifier.
		 */
		@SuppressWarnings("NullAway")
		private MetadataBackedField(String name, @Nullable MongoPersistentEntity<?> entity,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context,
				@Nullable BoundMongoPath boundPath, int offset, @Nullable String keyPrefix,
				@Nullable MongoPersistentProperty property, PathType pathType) {

			super(name);

			Assert.notNull(pathType, "PathType must not be null");

			this.entity = entity;
			this.mappingContext = context;
			this.boundPath = boundPath;
			this.offset = offset;
			this.keyPrefix = keyPrefix;
			this.pathType = pathType;

			boolean resolved = boundPath != null && boundPath.isResolved() && boundPath.leafProperty() != null;
			this.property = resolved ? boundPath.leafProperty() : property;
			this.association = resolved ? findAssociation(boundPath) : null;
		}

		private static @Nullable BoundMongoPath bind(String name, MongoPersistentEntity<?> entity,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
				@Nullable MongoPersistentProperty property, PathType pathType) {

			Assert.notNull(entity, "MongoPersistentEntity must not be null");

			MongoPathBinder binder = MongoPathBinder.of(mappingContext);

			if (property == null) {
				return binder.bind(name, pathType, Scope.of(entity));
			}

			return property.getOwner().equals(entity) ? binder.bind(property.getName(), pathType, Scope.of(entity)) : null;
		}

		/**
		 * Create a {@link MetadataBackedField} for {@code key} resolved against the given {@link Scope}.
		 */
		static MetadataBackedField forScope(String key, Scope scope,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context) {

			BoundMongoPath path = MongoPathBinder.of(context).bind(key, PathType.QUERY, scope);
			MongoPersistentEntity<?> entity = scope instanceof Scope.EntityScope entityScope ? entityScope.entity() : null;

			return new MetadataBackedField(key, entity, context, path, 0, null, null, PathType.QUERY);
		}

		@Override
		@SuppressWarnings("NullAway")
		public MetadataBackedField with(String name) {

			BoundMongoPath boundPath = getBoundPath();
			if (boundPath != null && boundPath.isResolved()) {
				return new MetadataBackedField(name, entity, mappingContext, boundPath, offset, keyPrefix, property, pathType);
			}
			if (entity == null) {
				return new MetadataBackedField(name, null, mappingContext, null, 0, null, property, pathType);
			}
			return new MetadataBackedField(name, entity, mappingContext, property, pathType);
		}

		/**
		 * Create a {@link MetadataBackedField} for {@code key} of a document nested in the value of this field, resolved
		 * relative to this field's path.
		 *
		 * @param key the nested key.
		 * @return the nested field or {@literal null} if the key cannot be resolved relative to this field, e.g. when
		 *         nested in an association.
		 */
		@Nullable
		MetadataBackedField nested(String key) {
			return nested(key, null);
		}

		/**
		 * Create a {@link MetadataBackedField} for the element an array filter {@code identifier} refers to.
		 *
		 * @param identifier the array filter identifier.
		 * @param elementPath path ending with the {@code $[identifier]} segment.
		 */
		static MetadataBackedField arrayFilterElement(String identifier, BoundMongoPath elementPath,
				MongoPersistentEntity<?> entity,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context) {
			return new MetadataBackedField(identifier, entity, context, elementPath, elementPath.segments().size(),
					identifier, null, elementPath.source().type());
		}

		@Nullable
		MetadataBackedField nested(String key, @Nullable String keyPrefix) {

			BoundMongoPath boundPath = getBoundPath();
			if (boundPath == null || boundPath.association() != null) {
				return null;
			}

			BoundMongoPath nestedPath;
			try {
				nestedPath = MongoPathBinder.of(mappingContext).append(boundPath, MongoPath.parse(key, pathType));
			} catch (MappingException | IllegalArgumentException ex) {
				return null;
			}

			MongoPersistentEntity<?> propertyEntity = getPropertyEntity();
			return new MetadataBackedField(key, propertyEntity != null ? propertyEntity : entity, mappingContext, nestedPath,
					boundPath.segments().size(), keyPrefix, null, pathType);
		}

		@Override
		public boolean isIdField() {

			if (property != null) {
				return property.getOwner().isIdProperty(property);
			}

			if (entity == null) {
				return super.isIdField();
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
		public @Nullable MongoPersistentEntity<?> getPropertyEntity() {
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
		public @Nullable Association<MongoPersistentProperty> getAssociation() {
			return association;
		}

		/**
		 * Associations are only considered if the path ends at the association or its identifier. Paths to other
		 * properties of a {@link org.springframework.data.mongodb.core.mapping.DocumentReference} target are plain paths.
		 */
		private static @Nullable Association<MongoPersistentProperty> findAssociation(BoundMongoPath path) {

			MongoPersistentProperty association = path.association();

			if (association == null) {
				return null;
			}

			for (Segment segment : path.segments()) {

				Segment.Property property = segment instanceof Segment.Exposed exposed ? exposed.reference()
						: segment instanceof Segment.Property it ? it : null;

				if (property != null && property.isReferenced() && !property.isId()) {
					return null;
				}
			}

			return association.getAssociation();
		}

		@Override
		public boolean isMap() {
			BoundMongoPath boundPath = getBoundPath();
			return super.isMap() && !(boundPath != null && boundPath.leaf() instanceof Segment.MapKey);
		}

		@Override
		public Class<?> getFieldType() {
			return property.getFieldType();
		}

		@Override
		public String getMappedKey() {

			String mappedKey = mapKey();
			if (mappedKey.startsWith("$") && !name.startsWith("$")) {
				throw new MappingException("Key '%s' must not be mapped to operator '%s'".formatted(name, mappedKey));
			}
			return mappedKey;
		}

		private String mapKey() {

			BoundMongoPath boundPath = getBoundPath();
			if (boundPath != null) {

				String mappedKey = boundPath.toRelativeMappedPath(offset);
				if (keyPrefix == null) {
					return mappedKey;
				}
				return mappedKey.isEmpty() ? keyPrefix : keyPrefix + "." + mappedKey;
			}

			if (getProperty() != null && getProperty().getMongoField().getName().isKey()) {
				return getProperty().getFieldName();
			}

			return name;
		}

		/**
		 * Return the {@link BoundMongoPath} used to render {@link #getMappedKey()} and to resolve nested fields. Override
		 * to customize the mapped key. {@link #getProperty()} and {@link #getAssociation()} are determined on creation and
		 * not affected.
		 *
		 * @return the {@link BoundMongoPath} backing this field or {@literal null} if the field was created for a property
		 *         not owned by the entity.
		 * @since 5.3
		 */
		protected @Nullable BoundMongoPath getBoundPath() {
			return boundPath;
		}

		/**
		 * @return the {@link PersistentPropertyPath} of this field or {@literal null} if it cannot be resolved.
		 * @deprecated since 5.3, no longer used by the mapping. Use {@link #getBoundPath()} instead.
		 */
		@Deprecated(since = "5.3")
		@Nullable
		protected PersistentPropertyPath<MongoPersistentProperty> getPath() {

			if (!pathResolved) {
				path = resolvePersistentPropertyPath();
				pathResolved = true;
			}
			return path;
		}

		private @Nullable PersistentPropertyPath<MongoPersistentProperty> resolvePersistentPropertyPath() {

			BoundMongoPath boundPath = getBoundPath();
			if (entity == null || boundPath == null || !boundPath.isResolved()) {
				return null;
			}

			List<Segment> segments = boundPath.segments();
			String dotPath = segments.subList(offset, segments.size()).stream()
					.filter(Segment.Property.class::isInstance).map(it -> ((Segment.Property) it).property().getName())
					.collect(Collectors.joining("."));

			if (dotPath.isEmpty()) {
				return null;
			}

			try {
				return mappingContext
						.getPersistentPropertyPath(PropertyPath.from(Pattern.quote(dotPath), entity.getTypeInformation()));
			} catch (RuntimeException ex) {
				return null;
			}
		}

		/**
		 * Return the {@link Converter} to be used to created the mapped key. Default implementation will use
		 * {@link PropertyToFieldNameConverter}.
		 *
		 * @return
		 * @deprecated since 5.3, no longer used to render {@link #getMappedKey()}. Use {@link #getBoundPath()} instead.
		 */
		@Deprecated(since = "5.3")
		protected Converter<MongoPersistentProperty, String> getPropertyConverter() {
			return PropertyToFieldNameConverter.INSTANCE;
		}

		/**
		 * Return the {@link Converter} to use for creating the mapped key of an association. Default implementation is
		 * {@link AssociationConverter}.
		 *
		 * @return
		 * @since 1.7
		 * @deprecated since 5.3, no longer used to render {@link #getMappedKey()}. Use {@link #getBoundPath()} instead.
		 */
		@Deprecated(since = "5.3")
		@SuppressWarnings("NullAway")
		protected Converter<MongoPersistentProperty, String> getAssociationConverter() {
			return new AssociationConverter(name, getAssociation());
		}

		protected MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> getMappingContext() {
			return mappingContext;
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
	}

	/**
	 * Converter to skip all properties after an association property was rendered.
	 *
	 * @author Oliver Gierke
	 * @deprecated since 5.3, mapped keys are rendered from {@link BoundMongoPath}.
	 */
	@Deprecated(since = "5.3")
	@SuppressWarnings("NullAway")
	protected static class AssociationConverter implements Converter<MongoPersistentProperty, @Nullable String> {

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

	enum NoPropertyPropertyValueProvider implements PropertyValueProvider<MongoPersistentProperty> {

		INSTANCE;

		@Override
		public <T> T getPropertyValue(MongoPersistentProperty property) {
			throw new IllegalStateException("No enclosing property source available");
		}
	}

	/*
	 * Types that must not be converted.
	 */
	static class Quirks {

		private static final Set<Class<?>> types = Set.of(Pattern.class, BsonRegularExpression.class);

		static boolean skipConversion(@Nullable Object value) {

			if (value == null) {
				return false;
			}

			return types.contains(value.getClass());
		}
	}
}
