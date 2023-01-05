/*
 * Copyright 2020-2023 the original author or authors.
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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation;
import org.springframework.util.Assert;

/**
 * Base class for common tasks required by {@link SetOperation} and {@link AddFieldsOperation}.
 *
 * @author Christoph Strobl
 * @since 3.0
 */
abstract class DocumentEnhancingOperation implements InheritsFieldsAggregationOperation {

	private final Map<Object, Object> valueMap;

	private ExposedFields exposedFields = ExposedFields.empty();

	protected DocumentEnhancingOperation(Map<Object, Object> source) {

		this.valueMap = new LinkedHashMap<>(source);
		for (Object key : source.keySet()) {
			this.exposedFields = add(key);
		}
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		InheritingExposedFieldsAggregationOperationContext operationContext = new InheritingExposedFieldsAggregationOperationContext(
				exposedFields, context);

		if (valueMap.size() == 1) {
			return context.getMappedObject(
					new Document(mongoOperator(), toSetEntry(valueMap.entrySet().iterator().next(), operationContext)));
		}

		Document $set = new Document();
		valueMap.entrySet().stream().map(it -> toSetEntry(it, operationContext)).forEach($set::putAll);
		return context.getMappedObject(new Document(mongoOperator(), $set));
	}

	/**
	 * @return the String representation of the native MongoDB operator.
	 */
	protected abstract String mongoOperator();

	@Override
	public String getOperator() {
		return mongoOperator();
	}

	/**
	 * @return the raw value map
	 */
	protected Map<Object, Object> getValueMap() {
		return this.valueMap;
	}

	@Override
	public ExposedFields getFields() {
		return exposedFields;
	}

	private ExposedFields add(Object fieldValue) {

		if (fieldValue instanceof Field field) {
			return exposedFields.and(new ExposedField(field, true));
		}
		if (fieldValue instanceof String fieldName) {
			return exposedFields.and(new ExposedField(Fields.field(fieldName), true));
		}

		throw new IllegalArgumentException(String.format("Expected %s to be a field/property", fieldValue));
	}

	private static Document toSetEntry(Entry<Object, Object> entry, AggregationOperationContext context) {

		String field = entry.getKey() instanceof String key ? context.getReference(key).getRaw()
				: context.getReference((Field) entry.getKey()).getRaw();

		Object value = computeValue(entry.getValue(), context);

		return new Document(field, value);
	}

	private static Object computeValue(Object value, AggregationOperationContext context) {

		if (value instanceof Field field) {
			return context.getReference(field).toString();
		}

		if (value instanceof ExpressionProjection expressionProjection) {
			return expressionProjection.toExpression(context);
		}

		if (value instanceof AggregationExpression aggregationExpression) {
			return aggregationExpression.toDocument(context);
		}

		if (value instanceof Collection<?> collection) {
			return collection.stream().map(it -> computeValue(it, context)).collect(Collectors.toList());
		}

		return value;
	}

	/**
	 * A {@link AggregationExpression} based on a SpEL expression.
	 *
	 * @author Mark Paluch
	 */
	static class ExpressionProjection {

		private static final SpelExpressionTransformer TRANSFORMER = new SpelExpressionTransformer();

		private final String expression;
		private final Object[] params;

		/**
		 * Creates a new {@link ProjectionOperation.ExpressionProjectionOperationBuilder.ExpressionProjection} for the given
		 * field, SpEL expression and parameters.
		 *
		 * @param expression must not be {@literal null} or empty.
		 * @param parameters must not be {@literal null}.
		 */
		ExpressionProjection(String expression, Object[] parameters) {

			Assert.notNull(expression, "Expression must not be null");
			Assert.notNull(parameters, "Parameters must not be null");

			this.expression = expression;
			this.params = parameters.clone();
		}

		Object toExpression(AggregationOperationContext context) {
			return TRANSFORMER.transform(expression, context, params);
		}
	}

}
