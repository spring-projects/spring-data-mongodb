/*
 * Copyright 2013-2017 the original author or authors.
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

import java.util.Collection;
import java.util.Map.Entry;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update.Modifier;
import org.springframework.data.mongodb.core.query.Update.Modifiers;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * A subclass of {@link QueryMapper} that retains type information on the mongo types.
 * 
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class UpdateMapper extends QueryMapper {

	private final MongoConverter converter;

	/**
	 * Creates a new {@link UpdateMapper} using the given {@link MongoConverter}.
	 * 
	 * @param converter must not be {@literal null}.
	 */
	public UpdateMapper(MongoConverter converter) {

		super(converter);
		this.converter = converter;
	}

	/**
	 * Converts the given source object to a mongo type retaining the original type information of the source type on the
	 * mongo type.
	 * 
	 * @see org.springframework.data.mongodb.core.convert.QueryMapper#delegateConvertToMongoType(java.lang.Object,
	 *      org.springframework.data.mongodb.core.mapping.MongoPersistentEntity)
	 */
	@Override
	protected Object delegateConvertToMongoType(Object source, MongoPersistentEntity<?> entity) {
		return converter.convertToMongoType(source,
				entity == null ? ClassTypeInformation.OBJECT : getTypeHintForEntity(source, entity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.QueryMapper#getMappedObjectForField(org.springframework.data.mongodb.core.convert.QueryMapper.Field, java.lang.Object)
	 */
	@Override
	protected Entry<String, Object> getMappedObjectForField(Field field, Object rawValue) {

		if (isDBObject(rawValue)) {
			return createMapEntry(field, convertSimpleOrDBObject(rawValue, field.getPropertyEntity()));
		}

		if (isQuery(rawValue)) {
			return createMapEntry(field,
					super.getMappedObject(((Query) rawValue).getQueryObject(), field.getPropertyEntity()));
		}

		if (isUpdateModifier(rawValue)) {
			return getMappedUpdateModifier(field, rawValue);
		}

		return super.getMappedObjectForField(field, rawValue);
	}

	private Entry<String, Object> getMappedUpdateModifier(Field field, Object rawValue) {
		Object value = null;

		if (rawValue instanceof Modifier) {

			value = getMappedValue(field, (Modifier) rawValue);

		} else if (rawValue instanceof Modifiers) {

			DBObject modificationOperations = new BasicDBObject();

			for (Modifier modifier : ((Modifiers) rawValue).getModifiers()) {
				modificationOperations.putAll(getMappedValue(field, modifier).toMap());
			}

			value = modificationOperations;
		} else {
			throw new IllegalArgumentException(String.format("Unable to map value of type '%s'!", rawValue.getClass()));
		}

		return createMapEntry(field, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.QueryMapper#isAssociationConversionNecessary(org.springframework.data.mongodb.core.convert.QueryMapper.Field, java.lang.Object)
	 */
	@Override
	protected boolean isAssociationConversionNecessary(Field documentField, Object value) {
		return super.isAssociationConversionNecessary(documentField, value) || documentField.containsAssociation();
	}

	private boolean isUpdateModifier(Object value) {
		return value instanceof Modifier || value instanceof Modifiers;
	}

	private boolean isQuery(Object value) {
		return value instanceof Query;
	}

	private DBObject getMappedValue(Field field, Modifier modifier) {
		return new BasicDBObject(modifier.getKey(), getMappedModifier(field, modifier));
	}

	private Object getMappedModifier(Field field, Modifier modifier) {

		Object value = modifier.getValue();

		if (value instanceof Sort) {

			DBObject sortObject = getSortObject((Sort) value);
			return field == null || field.getPropertyEntity() == null ? sortObject
					: getMappedSort(sortObject, field.getPropertyEntity());
		}

		TypeInformation<?> typeHint = field == null ? ClassTypeInformation.OBJECT : field.getTypeHint();

		return converter.convertToMongoType(value, typeHint);
	}

	private TypeInformation<?> getTypeHintForEntity(Object source, MongoPersistentEntity<?> entity) {

		TypeInformation<?> info = entity.getTypeInformation();
		Class<?> type = info.getActualType().getType();

		if (source == null || type.isInterface() || java.lang.reflect.Modifier.isAbstract(type.getModifiers())) {
			return info;
		}

		if (source instanceof Collection) {
			return NESTED_DOCUMENT;
		}

		if (!type.equals(source.getClass())) {
			return info;
		}

		return NESTED_DOCUMENT;
	}

	public DBObject getSortObject(Sort sort) {

		DBObject dbo = new BasicDBObject();

		for (Order order : sort) {
			dbo.put(order.getProperty(), order.isAscending() ? 1 : -1);
		}

		return dbo;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.QueryMapper#createPropertyField(org.springframework.data.mongodb.core.mapping.MongoPersistentEntity, java.lang.String, org.springframework.data.mapping.context.MappingContext)
	 */
	@Override
	protected Field createPropertyField(MongoPersistentEntity<?> entity, String key,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		return entity == null ? super.createPropertyField(entity, key, mappingContext)
				: new MetadataBackedUpdateField(entity, key, mappingContext);
	}

	/**
	 * {@link MetadataBackedField} that handles {@literal $} paths inside a field key. We clean up an update key
	 * containing a {@literal $} before handing it to the super class to make sure property lookups and transformations
	 * continue to work as expected. We provide a custom property converter to re-applied the cleaned up {@literal $}s
	 * when constructing the mapped key.
	 * 
	 * @author Thomas Darimont
	 * @author Oliver Gierke
	 */
	private static class MetadataBackedUpdateField extends MetadataBackedField {

		private final String key;

		/**
		 * Creates a new {@link MetadataBackedField} with the given {@link MongoPersistentEntity}, key and
		 * {@link MappingContext}. We clean up the key before handing it up to the super class to make sure it continues to
		 * work as expected.
		 * 
		 * @param entity must not be {@literal null}.
		 * @param key must not be {@literal null} or empty.
		 * @param mappingContext must not be {@literal null}.
		 */
		public MetadataBackedUpdateField(MongoPersistentEntity<?> entity, String key,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

			super(key.replaceAll("\\.\\$", ""), entity, mappingContext);
			this.key = key;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.MetadataBackedField#getMappedKey()
		 */
		@Override
		public String getMappedKey() {
			return this.getPath() == null ? key : super.getMappedKey();
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.MetadataBackedField#getPropertyConverter()
		 */
		@Override
		protected Converter<MongoPersistentProperty, String> getPropertyConverter() {
			return new PositionParameterRetainingPropertyKeyConverter(key);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.MetadataBackedField#getAssociationConverter()
		 */
		@Override
		protected Converter<MongoPersistentProperty, String> getAssociationConverter() {
			return new UpdateAssociationConverter(getAssociation(), key);
		}

		/**
		 * {@link Converter} retaining positional parameter {@literal $} for {@link Association}s.
		 * 
		 * @author Christoph Strobl
		 */
		protected static class UpdateAssociationConverter extends AssociationConverter {

			private final KeyMapper mapper;

			/**
			 * Creates a new {@link AssociationConverter} for the given {@link Association}.
			 * 
			 * @param association must not be {@literal null}.
			 */
			public UpdateAssociationConverter(Association<MongoPersistentProperty> association, String key) {

				super(association);
				this.mapper = new KeyMapper(key);
			}

			/* 
			 * (non-Javadoc)
			 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
			 */
			@Override
			public String convert(MongoPersistentProperty source) {
				return super.convert(source) == null ? null : mapper.mapPropertyName(source);
			}
		}
	}
}
