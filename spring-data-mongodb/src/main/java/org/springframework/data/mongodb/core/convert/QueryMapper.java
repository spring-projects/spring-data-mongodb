/*
 * Copyright 2011-2013 the original author or authors.
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
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionException;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
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
 */
public class QueryMapper {

	private static final List<String> DEFAULT_ID_NAMES = Arrays.asList("id", "_id");

	private final ConversionService conversionService;
	private final MongoConverter converter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

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
	}

	/**
	 * Replaces the property keys used in the given {@link DBObject} with the appropriate keys by using the
	 * {@link PersistentEntity} metadata.
	 * 
	 * @param query must not be {@literal null}.
	 * @param entity can be {@literal null}.
	 * @return
	 */
	public DBObject getMappedObject(DBObject query, MongoPersistentEntity<?> entity) {

		return getMappedObject(query, entity, null);
	}

	/**
	 * Replaces the property keys used in the given {@link DBObject} with the appropriate keys by using the
	 * {@link PersistentEntity} metadata and considering the parent {@link MongoPersistentProperty}
	 *
	 * @param query
	 * @param entity
	 * @param parent
	 * @return
	 */
	private DBObject getMappedObject(DBObject query, MongoPersistentEntity entity, Field parent) {

		DBObject result = new BasicDBObject();

		for (String key : query.keySet()) {

			if (isKeyword(key)) {
				if (parent != null) {
					result.put(key, getMappedValue(query.get(key), parent));
				} else {
					result.put(key, convertSimpleOrDBObject(query.get(key), entity));
				}
			} else {

				Field field = entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext);
				result.put(field.getMappedKey(), getMappedValue(query.get(key), field));
			}
		}

		return result;
	}


	/**
	 * Returns the mapped value for the given source object assuming it's a value for the given
	 * {@link MongoPersistentProperty}.
	 *
	 * @param field the property the value is a value for
	 * @param value the source object to be mapped
	 * @return
	 */
	private Object getMappedValue(Object value, Field field) {

		if (value instanceof DBObject) {
			return getMappedObject((DBObject) value, field.getPropertyEntity(), field);
		}

		if (field.isAssociation()) {
			return convertAssociation(value, field.getProperty());
		} else if (field.isIdField()) {
			return convertOneOrMoreIds(value);
		} else {
			return convertSimpleOrDBObject(value, field.getPropertyEntity());
		}
	}


	/**
	 * Retriggers mapping if the given source is a {@link DBObject} or simply invokes the
	 * 
	 * @param source
	 * @param entity
	 * @return
	 */
	private Object convertSimpleOrDBObject(Object source, MongoPersistentEntity<?> entity) {

		if (source instanceof Iterable) {
			BasicDBList result = new BasicDBList();
			for (Object element : (Iterable<?>) source) {
				result.add(convertSimpleOrDBObject(element, entity));
			}
			return result;
		}

		if (source instanceof DBObject) {
			return getMappedObject((DBObject) source, entity);
		}

		return converter.convertToMongoType(source);
	}

	/**
	 * Converts the given source assuming it's actually an association to anoter object.
	 * 
	 * @param source
	 * @param property
	 * @return
	 */
	private Object convertAssociation(Object source, MongoPersistentProperty property) {

		if (property == null || !property.isAssociation()) {
			return source;
		}

		if (source instanceof Iterable) {
			BasicDBList result = new BasicDBList();
			for (Object element : (Iterable<?>) source) {
				result.add(element instanceof DBRef ? element : converter.toDBRef(element, property));
			}
			return result;
		}

		if (property.isMap()) {
			BasicDBObject result = new BasicDBObject();
			DBObject dbObject = (DBObject) source;
			for (String key : dbObject.keySet()) {
				Object o = dbObject.get(key);
				result.put(key, o instanceof DBRef ? o : converter.toDBRef(o, property));
			}
			return result;
		}

		return source == null || source instanceof DBRef ? source : converter.toDBRef(source, property);
	}

	/**
	 * Convert collection or one id
	 *
	 * @param source
	 * @return
	 */
	private Object convertOneOrMoreIds(Object source) {

		if (source instanceof Iterable) {

			BasicDBList result = new BasicDBList();
			for (Object id : (Iterable<?>) source) {
				result.add(convertId(id));
			}
			return result;
		} else {
			return convertId(source);
		}
	}

	/**
	 * Converts the given raw id value into either {@link ObjectId} or {@link String}.
	 * 
	 * @param id
	 * @return
	 */
	public Object convertId(Object id) {

		try {
			return conversionService.convert(id, ObjectId.class);
		} catch (ConversionException e) {
			// Ignore
		}

		return converter.convertToMongoType(id);
	}

	/**
	 * Returns whether the given value actually represents a keyword.
	 *
	 * @param key
	 * @return
	 */
	boolean isKeyword(String key) {

		return key.startsWith("$");
	}

	/**
	 * Value object to represent a field and its meta-information.
	 * 
	 * @author Oliver Gierke
	 */
	private static class Field {

		private static final String ID_KEY = "_id";

		protected final String name;

		/**
		 * Creates a new {@link Field} without meta-information but the given name.
		 * 
		 * @param name must not be {@literal null} or empty.
		 */
		public Field(String name) {

			Assert.hasText(name, "Name must not be null!");
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
			return ID_KEY.equals(name);
		}

		/**
		 * Returns the underlying {@link MongoPersistentProperty} backing the field.
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
	}

	/**
	 * Extension of {@link Field} to be backed with mapping metadata.
	 * 
	 * @author Oliver Gierke
	 */
	private static class MetadataBackedField extends Field {

		private final MongoPersistentEntity<?> entity;
		private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
		private final MongoPersistentProperty property;

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

			super(name);

			Assert.notNull(entity, "MongoPersistentEntity must not be null!");

			this.entity = entity;
			this.mappingContext = context;

			PersistentPropertyPath<MongoPersistentProperty> path = getPath(name);
			this.property = path == null ? null : path.getLeafProperty();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#with(java.lang.String)
		 */
		@Override
		public MetadataBackedField with(String name) {
			return new MetadataBackedField(name, entity, mappingContext);
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
			return property;
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

			MongoPersistentProperty property = getProperty();
			return property == null ? false : property.isAssociation();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getTargetKey()
		 */
		@Override
		public String getMappedKey() {

			PersistentPropertyPath<MongoPersistentProperty> path = getPath(name);
			return path == null ? name : path.toDotPath(MongoPersistentProperty.PropertyToFieldNameConverter.INSTANCE);
		}

		private PersistentPropertyPath<MongoPersistentProperty> getPath(String name) {

			try {
				PropertyPath path = PropertyPath.from(name, entity.getTypeInformation());
				return mappingContext.getPersistentPropertyPath(path);
			} catch (PropertyReferenceException e) {
				return null;
			}
		}
	}
}
