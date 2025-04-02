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
 * @author Ross Lawley
 * @since 3.4
 */
public class MongoConversionContext implements ValueConversionContext<MongoPersistentProperty> {

	private final PropertyValueProvider<MongoPersistentProperty> accessor; // TODO: generics
	private final MongoConverter mongoConverter;

	@Nullable private final MongoPersistentProperty persistentProperty;
	@Nullable private final SpELContext spELContext;
	@Nullable private final ConversionOperation conversionOperation;

	public MongoConversionContext(PropertyValueProvider<MongoPersistentProperty> accessor,
			@Nullable MongoPersistentProperty persistentProperty, MongoConverter mongoConverter) {
		this(accessor, persistentProperty, mongoConverter, null, null);
	}

	public MongoConversionContext(PropertyValueProvider<MongoPersistentProperty> accessor,
			@Nullable MongoPersistentProperty persistentProperty, MongoConverter mongoConverter,
			@Nullable SpELContext spELContext) {
		this(accessor, persistentProperty, mongoConverter, spELContext, null);
	}

	public MongoConversionContext(PropertyValueProvider<MongoPersistentProperty> accessor,
			@Nullable MongoPersistentProperty persistentProperty, MongoConverter mongoConverter,
			@Nullable ConversionOperation conversionOperation) {
		this(accessor, persistentProperty, mongoConverter, null, conversionOperation);
	}

	public MongoConversionContext(PropertyValueProvider<MongoPersistentProperty> accessor,
			@Nullable MongoPersistentProperty persistentProperty, MongoConverter mongoConverter,
			@Nullable SpELContext spELContext, @Nullable ConversionOperation conversionOperation) {

		this.accessor = accessor;
		this.persistentProperty = persistentProperty;
		this.mongoConverter = mongoConverter;
		this.spELContext = spELContext;
		this.conversionOperation = conversionOperation;
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
	public ConversionOperation getConversionOperation() {
		return conversionOperation;
	}

	public interface ConversionOperation {
		 String getOperator();

		 String getPath();
	}

	public static class WriteConversionOperation implements ConversionOperation {

		private final String path;

		public WriteConversionOperation(String path) {
			this.path = path;
		}


		@Override
		public String getOperator() {
			return "write";
		}

		@Override
		public String getPath() {
			return path;
		}
	}

	public static class QueryConversionOperation implements ConversionOperation {

		private final String operator;
		private final String path;

		public QueryConversionOperation(@Nullable String operator, String path) {
			this.operator = operator != null ? operator : "$eq";
			this.path = path;
		}

		public String getOperator() {
			return operator;
		}

		public String getPath() {
			return path;
		}
	}
}
