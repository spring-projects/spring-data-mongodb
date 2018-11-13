/*
 * Copyright 2010-2018 the original author or authors.
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

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionException;
import org.springframework.data.convert.EntityConverter;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
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
 */
public interface MongoConverter
		extends EntityConverter<MongoPersistentEntity<?>, MongoPersistentProperty, Object, Bson>, MongoWriter<Object>,
		EntityReader<Object, Bson> {

	/**
	 * Returns thw {@link TypeMapper} being used to write type information into {@link Document}s created with that
	 * converter.
	 *
	 * @return will never be {@literal null}.
	 */
	MongoTypeMapper getTypeMapper();

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

		Assert.notNull(targetType, "TargetType must not be null!");
		Assert.notNull(dbRefResolver, "DbRefResolver must not be null!");

		if (targetType != Object.class && ClassUtils.isAssignable(targetType, source.getClass())) {
			return (T) source;
		}

		if (source instanceof BsonValue) {

			Object value = BsonUtils.toJavaType((BsonValue) source);

			if (value instanceof Document) {

				Document sourceDocument = (Document) value;

				if (sourceDocument.containsKey("$ref") && sourceDocument.containsKey("$id")) {

					Object id = sourceDocument.get("$id");
					String collection = sourceDocument.getString("$ref");

					MongoPersistentEntity<?> entity = getMappingContext().getPersistentEntity(targetType);
					if (entity != null && entity.hasIdProperty()) {
						id = convertId(id, entity.getIdProperty().getFieldType());
					}

					DBRef ref = sourceDocument.containsKey("$db") ? new DBRef(sourceDocument.getString("$db"), collection, id)
							: new DBRef(collection, id);

					sourceDocument = dbRefResolver.fetch(ref);
					if (sourceDocument == null) {
						return null;
					}
				}

				return read(targetType, sourceDocument);
			} else {
				if (!ClassUtils.isAssignable(targetType, value.getClass())) {
					if (getConversionService().canConvert(value.getClass(), targetType)) {
						return getConversionService().convert(value, targetType);
					}
				}
			}

			return (T) value;
		}
		return getConversionService().convert(source, targetType);
	}

	/**
	 * Converts the given raw id value into either {@link ObjectId} or {@link String}.
	 *
	 * @param id
	 * @param targetType
	 * @return {@literal null} if source {@literal id} is already {@literal null}.
	 * @since 2.2
	 */
	@Nullable
	default Object convertId(@Nullable Object id, Class<?> targetType) {

		if (id == null) {
			return null;
		}

		if (ClassUtils.isAssignable(ObjectId.class, targetType)) {

			if (id instanceof String) {

				if (ObjectId.isValid(id.toString())) {
					return new ObjectId(id.toString());
				}
			}
		}

		try {
			return getConversionService().canConvert(id.getClass(), targetType)
					? getConversionService().convert(id, targetType)
					: convertToMongoType(id, null);
		} catch (ConversionException o_O) {
			return convertToMongoType(id, null);
		}
	}
}
