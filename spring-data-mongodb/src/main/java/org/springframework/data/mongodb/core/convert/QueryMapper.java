/*
 * Copyright 2011-2015 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionException;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.domain.Example;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter.NestedDocument;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty.PropertyToFieldNameConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

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
 */
public class QueryMapper {

	private static final List<String> DEFAULT_ID_NAMES = Arrays.asList("id", "_id");
	private static final DBObject META_TEXT_SCORE = new BasicDBObject("$meta", "textScore");
	static final ClassTypeInformation<?> NESTED_DOCUMENT = ClassTypeInformation.from(NestedDocument.class);

	private enum MetaMapping {
		FORCE, WHEN_PRESENT, IGNORE;
	}

	private final ConversionService conversionService;
	private final MongoConverter converter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final MongoExampleMapper exampleMapper;

	/**
	 * Creates a new {@link QueryMapper} with the given {@link MongoConverter}.
	 * 
	 * @param converter must not be {@literal null}.
	 */
	public QueryMapper(MongoConverter converter) {

		Assert.notNull(converter);

		this.conversionService = converter.getConversionService();
		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
		this.exampleMapper = new MongoExampleMapper(converter);
	}

	/**
	 * Replaces the property keys used in the given {@link DBObject} with the appropriate keys by using the
	 * {@link PersistentEntity} metadata.
	 * 
	 * @param query must not be {@literal null}.
	 * @param entity can be {@literal null}.
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public DBObject getMappedObject(DBObject query, MongoPersistentEntity<?> entity) {

		if (isNestedKeyword(query)) {
			return getMappedKeyword(new Keyword(query), entity);
		}

		DBObject result = new BasicDBObject();

		for (String key : query.keySet()) {

			// TODO: remove one once QueryMapper can work with Query instances directly
			if (Query.isRestrictedTypeKey(key)) {

				@SuppressWarnings("unchecked")
				Set<Class<?>> restrictedTypes = (Set<Class<?>>) query.get(key);
				this.converter.getTypeMapper().writeTypeRestrictions(result, restrictedTypes);

				continue;
			}

			if (isKeyword(key)) {
				result.putAll(getMappedKeyword(new Keyword(query, key), entity));
				continue;
			}

			Field field = createPropertyField(entity, key, mappingContext);
			Entry<String, Object> entry = getMappedObjectForField(field, query.get(key));

			result.put(entry.getKey(), entry.getValue());
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
	public DBObject getMappedSort(DBObject sortObject, MongoPersistentEntity<?> entity) {

		if (sortObject == null) {
			return null;
		}

		DBObject mappedSort = getMappedObject(sortObject, entity);
		mapMetaAttributes(mappedSort, entity, MetaMapping.WHEN_PRESENT);
		return mappedSort;
	}

	/**
	 * Maps fields to retrieve to the {@link MongoPersistentEntity}s properties. <br />
	 * Also onverts and potentially adds missing property {@code $meta} representation.
	 * 
	 * @param fieldsObject
	 * @param entity
	 * @return
	 * @since 1.6
	 */
	public DBObject getMappedFields(DBObject fieldsObject, MongoPersistentEntity<?> entity) {

		DBObject mappedFields = fieldsObject != null ? getMappedObject(fieldsObject, entity) : new BasicDBObject();
		mapMetaAttributes(mappedFields, entity, MetaMapping.FORCE);
		return mappedFields.keySet().isEmpty() ? null : mappedFields;
	}

	private void mapMetaAttributes(DBObject source, MongoPersistentEntity<?> entity, MetaMapping metaMapping) {

		if (entity == null || source == null) {
			return;
		}

		if (entity.hasTextScoreProperty() && !MetaMapping.IGNORE.equals(metaMapping)) {
			MongoPersistentProperty textScoreProperty = entity.getTextScoreProperty();
			if (MetaMapping.FORCE.equals(metaMapping)
					|| (MetaMapping.WHEN_PRESENT.equals(metaMapping) && source.containsField(textScoreProperty.getFieldName()))) {
				source.putAll(getMappedTextScoreField(textScoreProperty));
			}
		}
	}

	private DBObject getMappedTextScoreField(MongoPersistentProperty property) {
		return new BasicDBObject(property.getFieldName(), META_TEXT_SCORE);
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

		if (isNestedKeyword(rawValue) && !field.isIdField()) {
			Keyword keyword = new Keyword((DBObject) rawValue);
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
	protected Field createPropertyField(MongoPersistentEntity<?> entity, String key,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext);
	}

	/**
	 * Returns the given {@link DBObject} representing a keyword by mapping the keyword's value.
	 * 
	 * @param keyword the {@link DBObject} representing a keyword (e.g. {@code $ne : … } )
	 * @param entity
	 * @return
	 */
	protected DBObject getMappedKeyword(Keyword keyword, MongoPersistentEntity<?> entity) {

		// $or/$nor
		if (keyword.isOrOrNor() || (keyword.hasIterableValue() && !keyword.isGeometry())) {

			Iterable<?> conditions = keyword.getValue();
			BasicDBList newConditions = new BasicDBList();

			for (Object condition : conditions) {
				newConditions.add(isDBObject(condition) ? getMappedObject((DBObject) condition, entity)
						: convertSimpleOrDBObject(condition, entity));
			}

			return new BasicDBObject(keyword.getKey(), newConditions);
		}

		if (keyword.isSample()) {
			return exampleMapper.getMappedExample(keyword.<Example<?>> getValue(), entity);
		}

		return new BasicDBObject(keyword.getKey(), convertSimpleOrDBObject(keyword.getValue(), entity));
	}

	/**
	 * Returns the mapped keyword considered defining a criteria for the given property.
	 * 
	 * @param property
	 * @param keyword
	 * @return
	 */
	protected DBObject getMappedKeyword(Field property, Keyword keyword) {

		boolean needsAssociationConversion = property.isAssociation() && !keyword.isExists();
		Object value = keyword.getValue();

		Object convertedValue = needsAssociationConversion ? convertAssociation(value, property) : getMappedValue(
				property.with(keyword.getKey()), value);

		return new BasicDBObject(keyword.key, convertedValue);
	}

	/**
	 * Returns the mapped value for the given source object assuming it's a value for the given
	 * {@link MongoPersistentProperty}.
	 * 
	 * @param value the source object to be mapped
	 * @param property the property the value is a value for
	 * @param newKey the key the value will be bound to eventually
	 * @return
	 */
	protected Object getMappedValue(Field documentField, Object value) {

		if (documentField.isIdField()) {

			if (isDBObject(value)) {
				DBObject valueDbo = (DBObject) value;
				DBObject resultDbo = new BasicDBObject(valueDbo.toMap());

				if (valueDbo.containsField("$in") || valueDbo.containsField("$nin")) {
					String inKey = valueDbo.containsField("$in") ? "$in" : "$nin";
					List<Object> ids = new ArrayList<Object>();
					for (Object id : (Iterable<?>) valueDbo.get(inKey)) {
						ids.add(convertId(id));
					}
					resultDbo.put(inKey, ids.toArray(new Object[ids.size()]));
				} else if (valueDbo.containsField("$ne")) {
					resultDbo.put("$ne", convertId(valueDbo.get("$ne")));
				} else {
					return getMappedObject(resultDbo, null);
				}

				return resultDbo;

			} else {
				return convertId(value);
			}
		}

		if (isNestedKeyword(value)) {
			return getMappedKeyword(new Keyword((DBObject) value), null);
		}

		if (isAssociationConversionNecessary(documentField, value)) {
			return convertAssociation(value, documentField);
		}

		return convertSimpleOrDBObject(value, documentField.getPropertyEntity());
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
	protected boolean isAssociationConversionNecessary(Field documentField, Object value) {

		Assert.notNull(documentField, "Document field must not be null!");

		if (value == null) {
			return false;
		}

		if (!documentField.isAssociation()) {
			return false;
		}

		Class<? extends Object> type = value.getClass();
		MongoPersistentProperty property = documentField.getProperty();

		if (property.getActualType().isAssignableFrom(type)) {
			return true;
		}

		MongoPersistentEntity<?> entity = documentField.getPropertyEntity();
		return entity.hasIdProperty()
				&& (type.equals(DBRef.class) || entity.getIdProperty().getActualType().isAssignableFrom(type));
	}

	/**
	 * Retriggers mapping if the given source is a {@link DBObject} or simply invokes the
	 * 
	 * @param source
	 * @param entity
	 * @return
	 */
	protected Object convertSimpleOrDBObject(Object source, MongoPersistentEntity<?> entity) {

		if (source instanceof BasicDBList) {
			return delegateConvertToMongoType(source, entity);
		}

		if (isDBObject(source)) {
			return getMappedObject((DBObject) source, entity);
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
	protected Object delegateConvertToMongoType(Object source, MongoPersistentEntity<?> entity) {
		return converter.convertToMongoType(source, entity == null ? null : entity.getTypeInformation());
	}

	protected Object convertAssociation(Object source, Field field) {
		return convertAssociation(source, field.getProperty());
	}

	/**
	 * Converts the given source assuming it's actually an association to another object.
	 * 
	 * @param source
	 * @param property
	 * @return
	 */
	protected Object convertAssociation(Object source, MongoPersistentProperty property) {

		if (property == null || source == null || source instanceof DBObject) {
			return source;
		}

		if (source instanceof DBRef) {

			DBRef ref = (DBRef) source;
			return new DBRef(ref.getCollectionName(), convertId(ref.getId()));
		}

		if (source instanceof Iterable) {
			BasicDBList result = new BasicDBList();
			for (Object element : (Iterable<?>) source) {
				result.add(createDbRefFor(element, property));
			}
			return result;
		}

		if (property.isMap()) {
			BasicDBObject result = new BasicDBObject();
			DBObject dbObject = (DBObject) source;
			for (String key : dbObject.keySet()) {
				result.put(key, createDbRefFor(dbObject.get(key), property));
			}
			return result;
		}

		return createDbRefFor(source, property);
	}

	/**
	 * Checks whether the given value is a {@link DBObject}.
	 * 
	 * @param value can be {@literal null}.
	 * @return
	 */
	protected final boolean isDBObject(Object value) {
		return value instanceof DBObject;
	}

	/**
	 * Creates a new {@link Entry} for the given {@link Field} with the given value.
	 * 
	 * @param field must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return
	 */
	protected final Entry<String, Object> createMapEntry(Field field, Object value) {
		return createMapEntry(field.getMappedKey(), value);
	}

	/**
	 * Creates a new {@link Entry} with the given key and value.
	 * 
	 * @param key must not be {@literal null} or empty.
	 * @param value can be {@literal null}
	 * @return
	 */
	private Entry<String, Object> createMapEntry(String key, Object value) {

		Assert.hasText(key, "Key must not be null or empty!");
		return Collections.singletonMap(key, value).entrySet().iterator().next();
	}

	private DBRef createDbRefFor(Object source, MongoPersistentProperty property) {

		if (source instanceof DBRef) {
			return (DBRef) source;
		}

		return converter.toDBRef(source, property);
	}

	/**
	 * Converts the given raw id value into either {@link ObjectId} or {@link String}.
	 * 
	 * @param id
	 * @return
	 */
	public Object convertId(Object id) {

		if (id == null) {
			return null;
		}

		if (id instanceof String) {
			return ObjectId.isValid(id.toString()) ? conversionService.convert(id, ObjectId.class) : id;
		}

		try {
			return conversionService.canConvert(id.getClass(), ObjectId.class) ? conversionService
					.convert(id, ObjectId.class) : delegateConvertToMongoType(id, null);
		} catch (ConversionException o_O) {
			return delegateConvertToMongoType(id, null);
		}
	}

	/**
	 * Returns whether the given {@link Object} is a keyword, i.e. if it's a {@link DBObject} with a keyword key.
	 * 
	 * @param candidate
	 * @return
	 */
	protected boolean isNestedKeyword(Object candidate) {

		if (!(candidate instanceof BasicDBObject)) {
			return false;
		}

		BasicDBObject dbObject = (BasicDBObject) candidate;
		Set<String> keys = dbObject.keySet();

		if (keys.size() != 1) {
			return false;
		}

		return isKeyword(keys.iterator().next().toString());
	}

	/**
	 * Returns whether the given {@link String} is a MongoDB keyword. The default implementation will check against the
	 * set of registered keywords returned by {@link #getKeywords()}.
	 * 
	 * @param candidate
	 * @return
	 */
	protected boolean isKeyword(String candidate) {
		return candidate.startsWith("$");
	}

	/**
	 * Value object to capture a query keyword representation.
	 * 
	 * @author Oliver Gierke
	 */
	static class Keyword {

		private static final String N_OR_PATTERN = "\\$.*or";

		private final String key;
		private final Object value;

		public Keyword(DBObject source, String key) {
			this.key = key;
			this.value = source.get(key);
		}

		public Keyword(DBObject dbObject) {

			Set<String> keys = dbObject.keySet();
			Assert.isTrue(keys.size() == 1, "Can only use a single value DBObject!");

			this.key = keys.iterator().next();
			this.value = dbObject.get(key);
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
			return key.matches(N_OR_PATTERN);
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
		 * Returns wheter the current keyword indicates a sample object.
		 * 
		 * @return
		 * @since 1.8
		 */
		public boolean isSample() {
			return "$sample".equalsIgnoreCase(key);
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
	}

	/**
	 * Value object to represent a field and its meta-information.
	 * 
	 * @author Oliver Gierke
	 */
	protected static class Field {

		private static final String ID_KEY = "_id";

		protected final String name;

		/**
		 * Creates a new {@link DocumentField} without meta-information but the given name.
		 * 
		 * @param name must not be {@literal null} or empty.
		 */
		public Field(String name) {

			Assert.hasText(name, "Name must not be null!");
			this.name = name;
		}

		/**
		 * Returns a new {@link DocumentField} with the given name.
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
			return ID_KEY.equals(name);
		}

		/**
		 * Returns the underlying {@link MongoPersistentProperty} backing the field. For path traversals this will be the
		 * property that represents the value to handle. This means it'll be the leaf property for plain paths or the
		 * association property in case we refer to an association somewhere in the path.
		 * 
		 * @return
		 */
		public MongoPersistentProperty getProperty() {
			return null;
		}

		/**
		 * Returns the {@link MongoPersistentEntity} that field is conatined in.
		 * 
		 * @return
		 */
		public MongoPersistentEntity<?> getPropertyEntity() {
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
			return isIdField() ? ID_KEY : name;
		}

		/**
		 * Returns whether the field references an association in case it refers to a nested field.
		 * 
		 * @return
		 */
		public boolean containsAssociation() {
			return false;
		}

		public Association<MongoPersistentProperty> getAssociation() {
			return null;
		}

		public TypeInformation<?> getTypeHint() {
			return ClassTypeInformation.OBJECT;
		}
	}

	/**
	 * Extension of {@link DocumentField} to be backed with mapping metadata.
	 * 
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	protected static class MetadataBackedField extends Field {

		private static final String INVALID_ASSOCIATION_REFERENCE = "Invalid path reference %s! Associations can only be pointed to directly or via their id property!";

		private final MongoPersistentEntity<?> entity;
		private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
		private final MongoPersistentProperty property;
		private final PersistentPropertyPath<MongoPersistentProperty> path;
		private final Association<MongoPersistentProperty> association;

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
				MongoPersistentProperty property) {

			super(name);

			Assert.notNull(entity, "MongoPersistentEntity must not be null!");

			this.entity = entity;
			this.mappingContext = context;

			this.path = getPath(name);
			this.property = path == null ? property : path.getLeafProperty();
			this.association = findAssociation();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#with(java.lang.String)
		 */
		@Override
		public MetadataBackedField with(String name) {
			return new MetadataBackedField(name, entity, mappingContext, property);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#isIdKey()
		 */
		@Override
		public boolean isIdField() {

			MongoPersistentProperty idProperty = entity.getIdProperty();

			if (idProperty != null) {
				return idProperty.getName().equals(name) || idProperty.getFieldName().equals(name);
			}

			return DEFAULT_ID_NAMES.contains(name);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getProperty()
		 */
		@Override
		public MongoPersistentProperty getProperty() {
			return association == null ? property : association.getInverse();
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getEntity()
		 */
		@Override
		public MongoPersistentEntity<?> getPropertyEntity() {
			MongoPersistentProperty property = getProperty();
			return property == null ? null : mappingContext.getPersistentEntity(property);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#isAssociation()
		 */
		@Override
		public boolean isAssociation() {
			return association != null;
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getAssociation()
		 */
		@Override
		public Association<MongoPersistentProperty> getAssociation() {
			return association;
		}

		/**
		 * Finds the association property in the {@link PersistentPropertyPath}.
		 * 
		 * @return
		 */
		private final Association<MongoPersistentProperty> findAssociation() {

			if (this.path != null) {
				for (MongoPersistentProperty p : this.path) {
					if (p.isAssociation()) {
						return p.getAssociation();
					}
				}
			}

			return null;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getTargetKey()
		 */
		@Override
		public String getMappedKey() {
			return path == null ? name : path.toDotPath(isAssociation() ? getAssociationConverter() : getPropertyConverter());
		}

		protected PersistentPropertyPath<MongoPersistentProperty> getPath() {
			return path;
		}

		/**
		 * Returns the {@link PersistentPropertyPath} for the given <code>pathExpression</code>.
		 * 
		 * @param pathExpression
		 * @return
		 */
		private PersistentPropertyPath<MongoPersistentProperty> getPath(String pathExpression) {

			try {

				PropertyPath path = PropertyPath.from(pathExpression.replaceAll("\\.\\d", ""), entity.getTypeInformation());
				PersistentPropertyPath<MongoPersistentProperty> propertyPath = mappingContext.getPersistentPropertyPath(path);

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
			} catch (PropertyReferenceException e) {
				return null;
			}
		}

		/**
		 * Return the {@link Converter} to be used to created the mapped key. Default implementation will use
		 * {@link PropertyToFieldNameConverter}.
		 * 
		 * @return
		 */
		protected Converter<MongoPersistentProperty, String> getPropertyConverter() {
			return new PositionParameterRetainingPropertyKeyConverter(name);
		}

		/**
		 * Return the {@link Converter} to use for creating the mapped key of an association. Default implementation is
		 * {@link AssociationConverter}.
		 * 
		 * @return
		 * @since 1.7
		 */
		protected Converter<MongoPersistentProperty, String> getAssociationConverter() {
			return new AssociationConverter(getAssociation());
		}

		/**
		 * @author Christoph Strobl
		 * @since 1.8
		 */
		static class PositionParameterRetainingPropertyKeyConverter implements Converter<MongoPersistentProperty, String> {

			private final KeyMapper keyMapper;

			public PositionParameterRetainingPropertyKeyConverter(String rawKey) {
				this.keyMapper = new KeyMapper(rawKey);
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
			 */
			@Override
			public String convert(MongoPersistentProperty source) {
				return keyMapper.mapPropertyName(source);
			}
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getTypeHint()
		 */
		@Override
		public TypeInformation<?> getTypeHint() {

			MongoPersistentProperty property = getProperty();

			if (property == null) {
				return super.getTypeHint();
			}

			if (property.getActualType().isInterface()
					|| java.lang.reflect.Modifier.isAbstract(property.getActualType().getModifiers())) {
				return ClassTypeInformation.OBJECT;
			}

			return NESTED_DOCUMENT;
		}

		/**
		 * @author Christoph Strobl
		 * @since 1.8
		 */
		static class KeyMapper {

			private final Iterator<String> iterator;

			public KeyMapper(String key) {

				this.iterator = Arrays.asList(key.split("\\.")).iterator();
				this.iterator.next();
			}

			/**
			 * Maps the property name while retaining potential positional operator {@literal $}.
			 * 
			 * @param property
			 * @return
			 */
			protected String mapPropertyName(MongoPersistentProperty property) {

				StringBuilder mappedName = new StringBuilder(PropertyToFieldNameConverter.INSTANCE.convert(property));
				boolean inspect = iterator.hasNext();

				while (inspect) {

					String partial = iterator.next();
					boolean isPositional = (isPositionalParameter(partial) && (property.isMap() || property.isCollectionLike()));

					if (isPositional) {
						mappedName.append(".").append(partial);
					}

					inspect = isPositional && iterator.hasNext();
				}

				return mappedName.toString();
			}

			private static boolean isPositionalParameter(String partial) {

				if ("$".equals(partial)) {
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

		private final MongoPersistentProperty property;
		private boolean associationFound;

		/**
		 * Creates a new {@link AssociationConverter} for the given {@link Association}.
		 * 
		 * @param association must not be {@literal null}.
		 */
		public AssociationConverter(Association<MongoPersistentProperty> association) {

			Assert.notNull(association, "Association must not be null!");
			this.property = association.getInverse();
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public String convert(MongoPersistentProperty source) {

			if (associationFound) {
				return null;
			}

			if (property.equals(source)) {
				associationFound = true;
			}

			return source.getFieldName();
		}
	}
}
