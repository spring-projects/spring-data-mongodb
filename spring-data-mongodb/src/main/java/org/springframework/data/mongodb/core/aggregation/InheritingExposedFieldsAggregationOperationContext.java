/*
 * Copyright 2016-2018 the original author or authors.
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

/**
 * {@link ExposedFieldsAggregationOperationContext} that inherits fields from its parent
 * {@link AggregationOperationContext}.
 *
 * @author Mark Paluch
 * @since 1.9
 */
class InheritingExposedFieldsAggregationOperationContext extends ExposedFieldsAggregationOperationContext {

	private final AggregationOperationContext previousContext;

	/**
	 * Creates a new {@link ExposedFieldsAggregationOperationContext} from the given {@link ExposedFields}. Uses the given
	 * {@link AggregationOperationContext} to perform a mapping to mongo types if necessary.
	 *
	 * @param exposedFields must not be {@literal null}.
	 * @param previousContext must not be {@literal null}.
	 */
	public InheritingExposedFieldsAggregationOperationContext(ExposedFields exposedFields,
			AggregationOperationContext previousContext) {

		super(exposedFields, previousContext);

		this.previousContext = previousContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.ExposedFieldsAggregationOperationContext#resolveExposedField(org.springframework.data.mongodb.core.aggregation.Field, java.lang.String)
	 */
	@Override
	protected FieldReference resolveExposedField(Field field, String name) {

		FieldReference fieldReference = super.resolveExposedField(field, name);
		if (fieldReference != null) {
			return fieldReference;
		}

		if (field != null) {
			return previousContext.getReference(field);
		}

		return previousContext.getReference(name);
	}
}
