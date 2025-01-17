/*
 * Copyright 2022-2025 the original author or authors.
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
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
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

	private final PropertyValueProvider<MongoPersistentProperty> accessor; // TODO: generics
	private final MongoConverter mongoConverter;

	@Nullable private final MongoPersistentProperty persistentProperty;
	@Nullable private final SpELContext spELContext;
	@Nullable private final String queryFieldPath;

	public MongoConversionContext(PropertyValueProvider<MongoPersistentProperty> accessor,
			@Nullable MongoPersistentProperty persistentProperty, MongoConverter mongoConverter) {
		this(accessor, mongoConverter, persistentProperty, null);
	}

	public MongoConversionContext(PropertyValueProvider<MongoPersistentProperty> accessor,
			@Nullable MongoPersistentProperty persistentProperty, MongoConverter mongoConverter,
			@Nullable SpELContext spELContext) {
		this(accessor, mongoConverter, persistentProperty, spELContext, null);
	}

	public MongoConversionContext(PropertyValueProvider<MongoPersistentProperty> accessor, MongoConverter mongoConverter,
			@Nullable MongoPersistentProperty persistentProperty, @Nullable String queryFieldPath) {
		this(accessor, mongoConverter, persistentProperty, null, queryFieldPath);
	}

	public MongoConversionContext(PropertyValueProvider<MongoPersistentProperty> accessor, MongoConverter mongoConverter,
			@Nullable MongoPersistentProperty persistentProperty, @Nullable SpELContext spELContext,
			@Nullable String queryFieldPath) {

		this.accessor = accessor;
		this.persistentProperty = persistentProperty;
		this.mongoConverter = mongoConverter;
		this.spELContext = spELContext;
		this.queryFieldPath = queryFieldPath;
	}

	@Override
	public MongoPersistentProperty getProperty() {

		if (persistentProperty == null) {
			throw new IllegalStateException("No underlying MongoPersistentProperty available");
		}

		return persistentProperty;
	}

	@Nullable
	public Object getValue(String propertyPath) {
		return accessor.getPropertyValue(getProperty().getOwner().getRequiredPersistentProperty(propertyPath));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T write(@Nullable Object value, TypeInformation<T> target) {
		return (T) mongoConverter.convertToMongoType(value, target);
	}

	@Override
	public <T> T read(@Nullable Object value, TypeInformation<T> target) {
		return value instanceof Bson bson ? mongoConverter.read(target.getType(), bson)
				: ValueConversionContext.super.read(value, target);
	}

	@Nullable
	public SpELContext getSpELContext() {
		return spELContext;
	}

	@Nullable
	public String getQueryFieldPath() {
		return queryFieldPath;
	}
}
