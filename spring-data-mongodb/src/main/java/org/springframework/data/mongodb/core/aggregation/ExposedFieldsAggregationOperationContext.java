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

import com.mongodb.DBObject;

/**
 * Support class to implement {@link AggregationOperation}s that will become an {@link AggregationOperationContext} as
 * well defining {@link ExposedFields}.
 * 
 * @author Oliver Gierke
 * @since 1.3
 */
public abstract class ExposedFieldsAggregationOperationContext implements AggregationOperationContext {

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

		ExposedField field = getFields().getField(name);

		if (field != null) {
			return new FieldReference(field);
		}

		throw new IllegalArgumentException(String.format("Invalid reference '%s'!", name));
	}

	protected abstract ExposedFields getFields();

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#hasReferenceFor(java.lang.String)
	 */
	protected boolean hasReferenceFor(String name) {
		return getFields().getField(name) != null;
	}
}
