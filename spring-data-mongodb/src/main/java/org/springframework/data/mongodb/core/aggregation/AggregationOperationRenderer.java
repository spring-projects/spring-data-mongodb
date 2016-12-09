/*
 * Copyright 2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.DirectFieldReference;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.aggregation.Fields.AggregationField;
import org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation;

import com.mongodb.DBObject;

/**
 * Rendering support for {@link AggregationOperation} into a {@link List} of {@link com.mongodb.DBObject}.
 * 
 * @author Mark Paluch
 * @since 1.10
 */
class AggregationOperationRenderer {

	static final AggregationOperationContext DEFAULT_CONTEXT = new NoOpAggregationOperationContext();

	/**
	 * Render a {@link List} of {@link AggregationOperation} given {@link AggregationOperationContext} into their
	 * {@link DBObject} representation.
	 * 
	 * @param operations must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @return the {@link List} of {@link DBObject}.
	 */
	static List<DBObject> toDBObject(List<AggregationOperation> operations, AggregationOperationContext rootContext) {

		List<DBObject> operationDocuments = new ArrayList<DBObject>(operations.size());

		AggregationOperationContext contextToUse = rootContext;

		for (AggregationOperation operation : operations) {

			operationDocuments.add(operation.toDBObject(contextToUse));

			if (operation instanceof FieldsExposingAggregationOperation) {

				FieldsExposingAggregationOperation exposedFieldsOperation = (FieldsExposingAggregationOperation) operation;

				if (operation instanceof InheritsFieldsAggregationOperation) {
					contextToUse = new InheritingExposedFieldsAggregationOperationContext(exposedFieldsOperation.getFields(),
							contextToUse);
				} else {
					contextToUse = new ExposedFieldsAggregationOperationContext(exposedFieldsOperation.getFields(), contextToUse);
				}
			}
		}

		return operationDocuments;
	}

	/**
	 * Simple {@link AggregationOperationContext} that just returns {@link FieldReference}s as is.
	 *
	 * @author Oliver Gierke
	 */
	private static class NoOpAggregationOperationContext implements AggregationOperationContext {

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
			return new DirectFieldReference(new ExposedField(field, true));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getReference(java.lang.String)
		 */
		@Override
		public FieldReference getReference(String name) {
			return new DirectFieldReference(new ExposedField(new AggregationField(name), true));
		}
	}
}
