/*
 * Copyright 2022-2023 the original author or authors.
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

import org.bson.conversions.Bson;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * {@link ValueConversionContext} that allows to delegate read/write to an underlying {@link MongoConverter}.
 *
 * @author Christoph Strobl
 * @since 3.4
 */
public class MongoConversionContext implements ValueConversionContext<MongoPersistentProperty> {

	private final MongoPersistentProperty persistentProperty;
	private final MongoConverter mongoConverter;

	public MongoConversionContext(MongoPersistentProperty persistentProperty, MongoConverter mongoConverter) {

		this.persistentProperty = persistentProperty;
		this.mongoConverter = mongoConverter;
	}

	@Override
	public MongoPersistentProperty getProperty() {
		return persistentProperty;
	}

	@Override
	public <T> T write(@Nullable Object value, TypeInformation<T> target) {
		return (T) mongoConverter.convertToMongoType(value, target);
	}

	@Override
	public <T> T read(@Nullable Object value, TypeInformation<T> target) {
		return value instanceof Bson ? mongoConverter.read(target.getType(), (Bson) value)
				: ValueConversionContext.super.read(value, target);
	}
}
