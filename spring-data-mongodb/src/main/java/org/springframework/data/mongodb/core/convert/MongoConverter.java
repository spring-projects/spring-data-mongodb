/*
 * Copyright 2010-2023 the original author or authors.
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

import com.mongodb.MongoClientSettings;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionException;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.EntityConverter;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.mongodb.CodecRegistryProvider;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.mongodb.DBRef;

/**
 * Central Mongo specific converter interface which combines {@link MongoWriter} and {@link EntityReader}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Ryan Gibb
 */
public interface MongoConverter
		extends EntityConverter<MongoPersistentEntity<?>, MongoPersistentProperty, Object, Bson>, MongoWriter<Object>,
		EntityReader<Object, Bson>, CodecRegistryProvider {

	/**
	 * Returns the {@link TypeMapper} being used to write type information into {@link Document}s created with that
	 * converter.
	 *
	 * @return will never be {@literal null}.
	 */
	MongoTypeMapper getTypeMapper();

	/**
	 * Returns the {@link ProjectionFactory} for this converter.
	 *
	 * @return will never be {@literal null}.
	 * @since 3.4
	 */
	ProjectionFactory getProjectionFactory();

	/**
	 * Returns the {@link CustomConversions} for this converter.
	 *
	 * @return will never be {@literal null}.
	 * @since 3.4
	 */
	CustomConversions getCustomConversions();

	/**
	 * Apply a projection to {@link Bson} and return the projection return type {@code R}.
	 * {@link EntityProjection#isProjection() Non-projecting} descriptors fall back to {@link #read(Class, Object) regular
	 * object materialization}.
	 *
	 * @param descriptor the projection descriptor, must not be {@literal null}.
	 * @param bson must not be {@literal null}.
	 * @param <R>
	 * @return a new instance of the projection return type {@code R}.
	 * @since 3.4
	 */
	<R> R project(EntityProjection<R, ?> descriptor, Bson bson);

	/**
	 * Mapping function capable of converting values into a desired target type by eg. extracting the actual java type
	 * from a given {@link BsonValue}.
	 *
	 * @param targetType must not be {@literal null}.
	 * @param dbRefResolver must not be {@literal null}.
	 * @param <S>
	 * @param <T>
	 * @return new typed {@link java.util.function.Function}.
	 * @throws IllegalArgumentException if {@literal targetType} is {@literal null}.
	 * @since 2.1
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	default <S, T> T mapValueToTargetType(S source, Class<T> targetType, DbRefResolver dbRefResolver) {

		Assert.notNull(targetType, "TargetType must not be null");
		Assert.notNull(dbRefResolver, "DbRefResolver must not be null");

		if (targetType != Object.class && ClassUtils.isAssignable(targetType, source.getClass())) {
			return (T) source;
		}

		if (source instanceof BsonValue bson) {

			Object value = BsonUtils.toJavaType(bson);

			if (value instanceof Document document) {

				if (document.containsKey("$ref") && document.containsKey("$id")) {

					Object id = document.get("$id");
					String collection = document.getString("$ref");

					MongoPersistentEntity<?> entity = getMappingContext().getPersistentEntity(targetType);
					if (entity != null && entity.hasIdProperty()) {
						id = convertId(id, entity.getIdProperty().getFieldType());
					}

					DBRef ref = document.containsKey("$db") ? new DBRef(document.getString("$db"), collection, id)
							: new DBRef(collection, id);

					document = dbRefResolver.fetch(ref);
					if (document == null) {
						return null;
					}
				}

				return read(targetType, document);
			} else {
				if (!ClassUtils.isAssignable(targetType, value.getClass()) && getConversionService().canConvert(value.getClass(), targetType)) {
					return getConversionService().convert(value, targetType);
				}
			}

			return (T) value;
		}
		return getConversionService().convert(source, targetType);
	}

	/**
	 * Converts the given raw id value into either {@link ObjectId} or {@link String}.
	 *
	 * @param id can be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @return {@literal null} if source {@literal id} is already {@literal null}.
	 * @since 2.2
	 */
	@Nullable
	default Object convertId(@Nullable Object id, Class<?> targetType) {

		if (id == null || ClassUtils.isAssignableValue(targetType, id)) {
			return id;
		}

		if (ClassUtils.isAssignable(ObjectId.class, targetType)) {

			if (id instanceof String) {

				if (ObjectId.isValid(id.toString())) {
					return new ObjectId(id.toString());
				}

				// avoid ConversionException as convertToMongoType will return String anyways.
				return id;
			}
		}

		try {
			return getConversionService().canConvert(id.getClass(), targetType)
					? getConversionService().convert(id, targetType)
					: convertToMongoType(id, (TypeInformation<?>) null);
		} catch (ConversionException o_O) {
			return convertToMongoType(id,(TypeInformation<?>)  null);
		}
	}

	@Override
	default CodecRegistry getCodecRegistry() {
		return MongoClientSettings.getDefaultCodecRegistry();
	}

}
