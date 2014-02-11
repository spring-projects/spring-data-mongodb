/*
 * Copyright 2013-2014 the original author or authors.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty.PropertyToFieldNameConverter;
import org.springframework.data.mongodb.core.query.Update.Modifier;
import org.springframework.data.mongodb.core.query.Update.Modifiers;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * A subclass of {@link QueryMapper} that retains type information on the mongo types.
 * 
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
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
		return entity == null ? super.delegateConvertToMongoType(source, null) : converter.convertToMongoType(source,
				entity.getTypeInformation());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.QueryMapper#getMappedObjectForField(org.springframework.data.mongodb.core.convert.QueryMapper.Field, java.lang.Object)
	 */
	@Override
	protected Entry<String, Object> getMappedObjectForField(Field field, Object rawValue) {

		if (!isUpdateModifier(rawValue)) {
			return super.getMappedObjectForField(field, rawValue);
		}

		Object value = null;

		if (rawValue instanceof Modifier) {

			value = getMappedValue((Modifier) rawValue);

		} else if (rawValue instanceof Modifiers) {

			DBObject modificationOperations = new BasicDBObject();

			for (Modifier modifier : ((Modifiers) rawValue).getModifiers()) {
				modificationOperations.putAll(getMappedValue(modifier).toMap());
			}

			value = modificationOperations;
		} else {

			throw new IllegalArgumentException(String.format("Unable to map value of type '%s'!", rawValue.getClass()));
		}

		return Collections.singletonMap(field.getMappedKey(), value).entrySet().iterator().next();
	}

	private boolean isUpdateModifier(Object value) {
		return value instanceof Modifier || value instanceof Modifiers;
	}

	private DBObject getMappedValue(Modifier modifier) {

		Object value = converter.convertToMongoType(modifier.getValue(), ClassTypeInformation.OBJECT);
		return new BasicDBObject(modifier.getKey(), value);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.QueryMapper#createPropertyField(org.springframework.data.mongodb.core.mapping.MongoPersistentEntity, java.lang.String, org.springframework.data.mapping.context.MappingContext)
	 */
	@Override
	protected Field createPropertyField(MongoPersistentEntity<?> entity, String key,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		return entity == null ? super.createPropertyField(entity, key, mappingContext) : //
				new MetadataBackedUpdateField(entity, key, mappingContext);
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
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.MetadataBackedField#getPropertyConverter()
		 */
		@Override
		protected Converter<MongoPersistentProperty, String> getPropertyConverter() {
			return new UpdatePropertyConverter(key);
		}

		/**
		 * Special {@link Converter} for {@link MongoPersistentProperty} instances that will concatenate the {@literal $}
		 * contained in the source update key.
		 * 
		 * @author Oliver Gierke
		 */
		private static class UpdatePropertyConverter implements Converter<MongoPersistentProperty, String> {

			private final Iterator<String> iterator;

			/**
			 * Creates a new {@link UpdatePropertyConverter} with the given update key.
			 * 
			 * @param updateKey must not be {@literal null} or empty.
			 */
			public UpdatePropertyConverter(String updateKey) {

				Assert.hasText(updateKey, "Update key must not be null or empty!");

				this.iterator = Arrays.asList(updateKey.split("\\.")).iterator();
				this.iterator.next();
			}

			/* 
			 * (non-Javadoc)
			 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
			 */
			@Override
			public String convert(MongoPersistentProperty property) {

				String mappedName = PropertyToFieldNameConverter.INSTANCE.convert(property);
				return iterator.hasNext() && iterator.next().equals("$") ? String.format("%s.$", mappedName) : mappedName;
			}
		}
	}
}
