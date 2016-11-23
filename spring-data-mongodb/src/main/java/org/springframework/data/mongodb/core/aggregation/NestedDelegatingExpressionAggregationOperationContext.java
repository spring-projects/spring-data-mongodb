/*
 * Copyright 2016. the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExpressionFieldReference;
import org.springframework.util.Assert;

import com.mongodb.DBObject;

/**
 * {@link AggregationOperationContext} that delegates {@link FieldReference} resolution and mapping to a parent one, but
 * assures {@link FieldReference} get converted into {@link ExpressionFieldReference} using {@code $$} to ref an inner
 * variable.
 *
 * @author Christoph Strobl
 * @since 1.10
 */
class NestedDelegatingExpressionAggregationOperationContext implements AggregationOperationContext {

	private final AggregationOperationContext delegate;

	/**
	 * Creates new {@link NestedDelegatingExpressionAggregationOperationContext}.
	 *
	 * @param referenceContext must not be {@literal null}.
	 */
	public NestedDelegatingExpressionAggregationOperationContext(AggregationOperationContext referenceContext) {

		Assert.notNull(referenceContext, "Reference context must not be null!");
		this.delegate = referenceContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getMappedObject(com.mongodb.DBObject)
	 */
	@Override
	public DBObject getMappedObject(DBObject dbObject) {
		return delegate.getMappedObject(dbObject);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getReference(org.springframework.data.mongodb.core.aggregation.Field)
	 */
	@Override
	public FieldReference getReference(Field field) {
		return new ExpressionFieldReference(delegate.getReference(field));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getReference(java.lang.String)
	 */
	@Override
	public FieldReference getReference(String name) {
		return new ExpressionFieldReference(delegate.getReference(name));
	}
}
