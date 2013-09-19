/*
 * Copyright 2013 the original author or authors.
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

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.util.Assert;

import com.mongodb.DBObject;

/**
 * {@link AggregationOperationContext} that combines the available field references from a given
 * {@code AggregationOperationContext} and an {@link FieldsExposingAggregationOperation}.
 * 
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @since 1.4
 */
class ExposedFieldsAggregationOperationContext implements AggregationOperationContext {

	private final ExposedFields exposedFields;

	/**
	 * Creates a new {@link ExposedFieldsAggregationOperationContext} from the given {@link ExposedFields}.
	 * 
	 * @param exposedFields must not be {@literal null}.
	 */
	public ExposedFieldsAggregationOperationContext(ExposedFields exposedFields) {

		Assert.notNull(exposedFields, "ExposedFields must not be null!");
		this.exposedFields = exposedFields;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getMappedObject(com.mongodb.DBObject)
	 */
	@Override
	public DBObject getMappedObject(DBObject dbObject) {
		return dbObject;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getReference(org.springframework.data.mongodb.core.aggregation.ExposedFields.AvailableField)
	 */
	@Override
	public FieldReference getReference(Field field) {
		return getReference(field.getTarget());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getReference(java.lang.String)
	 */
	@Override
	public FieldReference getReference(String name) {

		ExposedField field = exposedFields.getField(name);

		if (field != null) {
			return new FieldReference(field);
		}

		throw new IllegalArgumentException(String.format("Invalid reference '%s'!", name));
	}
}
