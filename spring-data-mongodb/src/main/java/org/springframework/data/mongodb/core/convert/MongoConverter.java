/*
 * Copyright 2010-2016 the original author or authors.
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

import java.util.function.Function;

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.convert.EntityConverter;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.mongodb.DBRef;

/**
 * Central Mongo specific converter interface which combines {@link MongoWriter} and {@link MongoReader}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
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
	 * @return new typed {@link com.mongodb.Function}.
	 * @throws IllegalArgumentException if {@literal targetType} is {@literal null}.
	 * @since 2.1
	 */
	default <S, T> Function<S, T> mapValueToTargetType(Class<T> targetType, DbRefResolver dbRefResolver) {

		Assert.notNull(targetType, "TargetType must not be null!");
		Assert.notNull(dbRefResolver, "DbRefResolver must not be null!");

		return (source) -> {

			if (targetType != Object.class && ClassUtils.isAssignable(targetType, source.getClass())) {
				return (T) source;
			}

			if (source instanceof BsonValue) {

				Object value = BsonUtils.toJavaType((BsonValue) source);

				if (value instanceof Document) {

					Document sourceDocument = (Document) value;

					if (sourceDocument.containsKey("$ref") && sourceDocument.containsKey("$id")) {

						sourceDocument = dbRefResolver
								.fetch(new DBRef(sourceDocument.getString("$ref"), sourceDocument.get("$id")));
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
			return (T) getConversionService().convert(source, targetType);
		};
	}
}
