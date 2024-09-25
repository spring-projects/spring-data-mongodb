/*
 * Copyright 2013-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.CodecRegistryProvider;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import com.mongodb.MongoClientSettings;

/**
 * The context for an {@link AggregationOperation}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.3
 */
public interface AggregationOperationContext extends CodecRegistryProvider {

	/**
	 * Returns the mapped {@link Document}, potentially converting the source considering mapping metadata etc.
	 *
	 * @param document will never be {@literal null}.
	 * @return must not be {@literal null}.
	 */
	default Document getMappedObject(Document document) {
		return getMappedObject(document, null);
	}

	/**
	 * Returns the mapped {@link Document}, potentially converting the source considering mapping metadata for the given
	 * type.
	 *
	 * @param document will never be {@literal null}.
	 * @param type can be {@literal null}.
	 * @return must not be {@literal null}.
	 * @since 2.2
	 */
	Document getMappedObject(Document document, @Nullable Class<?> type);

	/**
	 * Returns a {@link FieldReference} for the given field.
	 *
	 * @param field must not be {@literal null}.
	 * @return the {@link FieldReference} for the given {@link Field}.
	 * @throws IllegalArgumentException if the context does not expose a field with the given name
	 */
	FieldReference getReference(Field field);

	/**
	 * Returns the {@link FieldReference} for the field with the given name.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return the {@link FieldReference} for the field with given {@literal name}.
	 * @throws IllegalArgumentException if the context does not expose a field with the given name
	 */
	FieldReference getReference(String name);

	/**
	 * Returns the {@link Fields} exposed by the type. May be a {@literal class} or an {@literal interface}. The default
	 * implementation uses {@link BeanUtils#getPropertyDescriptors(Class) property descriptors} discover fields from a
	 * {@link Class}.
	 *
	 * @param type must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.2
	 * @see BeanUtils#getPropertyDescriptor(Class, String)
	 */
	default Fields getFields(Class<?> type) {

		Assert.notNull(type, "Type must not be null");

		return Fields.fields(Arrays.stream(BeanUtils.getPropertyDescriptors(type)) //
				.filter(it -> { // object and default methods
					Method method = it.getReadMethod();
					if (method == null) {
						return false;
					}
					if (ReflectionUtils.isObjectMethod(method)) {
						return false;
					}
					return !method.isDefault();
				}) //
				.map(PropertyDescriptor::getName) //
				.toArray(String[]::new));
	}

	/**
	 * Create a nested {@link AggregationOperationContext} from this context that exposes {@link ExposedFields fields}.
	 * <p>
	 * Implementations of {@link AggregationOperationContext} retain their {@link FieldLookupPolicy}. If no policy is
	 * specified, then lookup defaults to {@link FieldLookupPolicy#strict()}.
	 *
	 * @param fields the fields to expose, must not be {@literal null}.
	 * @return the new {@link AggregationOperationContext} exposing {@code fields}.
	 * @since 4.3.1
	 */
	default AggregationOperationContext expose(ExposedFields fields) {
		return new ExposedFieldsAggregationOperationContext(fields, this, FieldLookupPolicy.strict());
	}

	/**
	 * Create a nested {@link AggregationOperationContext} from this context that inherits exposed fields from this
	 * context and exposes {@link ExposedFields fields}.
	 * <p>
	 * Implementations of {@link AggregationOperationContext} retain their {@link FieldLookupPolicy}. If no policy is
	 * specified, then lookup defaults to {@link FieldLookupPolicy#strict()}.
	 *
	 * @param fields the fields to expose, must not be {@literal null}.
	 * @return the new {@link AggregationOperationContext} exposing {@code fields}.
	 * @since 4.3.1
	 */
	default AggregationOperationContext inheritAndExpose(ExposedFields fields) {
		return new InheritingExposedFieldsAggregationOperationContext(fields, this, FieldLookupPolicy.strict());
	}

	/**
	 * This toggle allows the {@link AggregationOperationContext context} to use any given field name without checking for
	 * its existence. Typically, the {@link AggregationOperationContext} fails when referencing unknown fields, those that
	 * are not present in one of the previous stages or the input source, throughout the pipeline.
	 *
	 * @return a more relaxed {@link AggregationOperationContext}.
	 * @since 3.0
	 * @deprecated since 4.3.1, {@link FieldLookupPolicy} should be specified explicitly when creating the
	 *             AggregationOperationContext.
	 */
	@Deprecated(since = "4.3.1", forRemoval = true)
	default AggregationOperationContext continueOnMissingFieldReference() {
		return this;
	}

	@Override
	default CodecRegistry getCodecRegistry() {
		return MongoClientSettings.getDefaultCodecRegistry();
	}

}
