/*
 * Copyright 2013-2023 the original author or authors.
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

import static org.springframework.data.mongodb.core.aggregation.Fields.*;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.DirectFieldReference;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link AggregationOperationContext} aware of a particular type and a {@link MappingContext} to potentially translate
 * property references into document field names.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.3
 */
public class TypeBasedAggregationOperationContext implements AggregationOperationContext {

	private final Class<?> type;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final QueryMapper mapper;
	private final Lazy<MongoPersistentEntity<?>> entity;

	/**
	 * Creates a new {@link TypeBasedAggregationOperationContext} for the given type, {@link MappingContext} and
	 * {@link QueryMapper}.
	 *
	 * @param type must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 * @param mapper must not be {@literal null}.
	 */
	public TypeBasedAggregationOperationContext(Class<?> type,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext, QueryMapper mapper) {

		Assert.notNull(type, "Type must not be null");
		Assert.notNull(mappingContext, "MappingContext must not be null");
		Assert.notNull(mapper, "QueryMapper must not be null");

		this.type = type;
		this.mappingContext = mappingContext;
		this.mapper = mapper;
		this.entity = Lazy.of(() -> mappingContext.getPersistentEntity(type));
	}

	@Override
	public Document getMappedObject(Document document) {
		return getMappedObject(document, type);
	}

	@Override
	public Document getMappedObject(Document document, @Nullable Class<?> type) {
		return mapper.getMappedObject(document, type != null ? mappingContext.getPersistentEntity(type) : null);
	}

	@Override
	public FieldReference getReference(Field field) {
		return getReferenceFor(field);
	}

	@Override
	public FieldReference getReference(String name) {
		return getReferenceFor(field(name));
	}

	@Override
	public Fields getFields(Class<?> type) {

		Assert.notNull(type, "Type must not be null");

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);

		if (entity == null) {
			return AggregationOperationContext.super.getFields(type);
		}

		List<Field> fields = new ArrayList<>();

		for (MongoPersistentProperty property : entity) {
			fields.add(Fields.field(property.getName(), property.getFieldName()));
		}

		return Fields.from(fields.toArray(new Field[0]));
	}

	@Override
	public AggregationOperationContext continueOnMissingFieldReference() {
		return continueOnMissingFieldReference(type);
	}

	/**
	 * This toggle allows the {@link AggregationOperationContext context} to use any given field name without checking for
	 * its existence. Typically, the {@link AggregationOperationContext} fails when referencing unknown fields, those that
	 * are not present in one of the previous stages or the input source, throughout the pipeline.
	 *
	 * @param type The domain type to map fields to.
	 * @return a more relaxed {@link AggregationOperationContext}.
	 * @since 3.1
	 * @see RelaxedTypeBasedAggregationOperationContext
	 */
	public AggregationOperationContext continueOnMissingFieldReference(Class<?> type) {
		return new RelaxedTypeBasedAggregationOperationContext(type, mappingContext, mapper);
	}

	protected FieldReference getReferenceFor(Field field) {

		if(entity.getNullable() == null || AggregationVariable.isVariable(field)) {
			return new DirectFieldReference(new ExposedField(field, true));
		}

		PersistentPropertyPath<MongoPersistentProperty> propertyPath = mappingContext
					.getPersistentPropertyPath(field.getTarget(), type);
		Field mappedField = field(field.getName(),
					propertyPath.toDotPath(MongoPersistentProperty.PropertyToFieldNameConverter.INSTANCE));

		return new DirectFieldReference(new ExposedField(mappedField, true));
	}

	public Class<?> getType() {
		return type;
	}

	@Override
	public CodecRegistry getCodecRegistry() {
		return this.mapper.getConverter().getCodecRegistry();
	}
}
