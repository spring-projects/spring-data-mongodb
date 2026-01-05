/*
 * Copyright 2022-present the original author or authors.
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
import org.jspecify.annotations.Nullable;

import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.CheckReturnValue;

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
	@Nullable private final OperatorContext operatorContext;

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
			@Nullable OperatorContext operatorContext) {
		this(accessor, persistentProperty, mongoConverter, null, operatorContext);
	}

	public MongoConversionContext(PropertyValueProvider<MongoPersistentProperty> accessor,
			@Nullable MongoPersistentProperty persistentProperty, MongoConverter mongoConverter,
			@Nullable SpELContext spELContext, @Nullable OperatorContext operatorContext) {

		this.accessor = accessor;
		this.persistentProperty = persistentProperty;
		this.mongoConverter = mongoConverter;
		this.spELContext = spELContext;
		this.operatorContext = operatorContext;
	}

	@Override
	public MongoPersistentProperty getProperty() {

		if (persistentProperty == null) {
			throw new IllegalStateException("No underlying MongoPersistentProperty available");
		}

		return persistentProperty;
	}

	/**
	 * @param operatorContext
	 * @return new instance of {@link MongoConversionContext}.
	 * @since 4.5
	 */
	@CheckReturnValue
	public MongoConversionContext forOperator(@Nullable OperatorContext operatorContext) {
		return new MongoConversionContext(accessor, persistentProperty, mongoConverter, spELContext, operatorContext);
	}

	@Nullable
	public Object getValue(String propertyPath) {
		return accessor.getPropertyValue(getProperty().getOwner().getRequiredPersistentProperty(propertyPath));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> @Nullable T write(@Nullable Object value, TypeInformation<T> target) {
		return (T) mongoConverter.convertToMongoType(value, target);
	}

	@Override
	public <T> @Nullable T read(@Nullable Object value, TypeInformation<T> target) {
		return value instanceof Bson bson ? mongoConverter.read(target.getType(), bson)
				: ValueConversionContext.super.read(value, target);
	}

	@Nullable
	public SpELContext getSpELContext() {
		return spELContext;
	}

	@Nullable
	public OperatorContext getOperatorContext() {
		return operatorContext;
	}

	/**
	 * The {@link OperatorContext} provides access to the actual conversion intent like a write operation or a query
	 * operator such as {@literal $gte}.
	 *
	 * @since 4.5
	 */
	public interface OperatorContext {

		/**
		 * The operator the conversion is used in.
		 *
		 * @return {@literal write} for simple write operations during save, or a query operator.
		 */
		String operator();

		/**
		 * The context path the operator is used in.
		 *
		 * @return never {@literal null}.
		 */
		String path();

		boolean isWriteOperation();

	}

	record WriteOperatorContext(String path) implements OperatorContext {

		@Override
		public String operator() {
			return "write";
		}

		@Override
		public boolean isWriteOperation() {
			return true;
		}
	}

	record QueryOperatorContext(String operator, String path) implements OperatorContext {

		public QueryOperatorContext(@Nullable String operator, String path) {
			this.operator = operator != null ? operator : "$eq";
			this.path = path;
		}

		@Override
		public boolean isWriteOperation() {
			return false;
		}
	}

}
