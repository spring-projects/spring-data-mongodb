/*
 * Copyright 2016-2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.DirectFieldReference;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.aggregation.Fields.AggregationField;
import org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation;
import org.springframework.lang.Nullable;

/**
 * Rendering support for {@link AggregationOperation} into a {@link List} of {@link org.bson.Document}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.10
 */
class AggregationOperationRenderer {

	static final AggregationOperationContext DEFAULT_CONTEXT = new NoOpAggregationOperationContext();

	/**
	 * Render a {@link List} of {@link AggregationOperation} given {@link AggregationOperationContext} into their
	 * {@link Document} representation.
	 *
	 * @param operations must not be {@literal null}.
	 * @param rootContext must not be {@literal null}.
	 * @return the {@link List} of {@link Document}.
	 */
	static List<Document> toDocument(List<AggregationOperation> operations, AggregationOperationContext rootContext) {

		List<Document> operationDocuments = new ArrayList<Document>(operations.size());

		AggregationOperationContext contextToUse = rootContext;

		for (AggregationOperation operation : operations) {

			operationDocuments.addAll(operation.toPipelineStages(contextToUse));

			if (operation instanceof FieldsExposingAggregationOperation exposedFieldsOperation) {

				ExposedFields fields = exposedFieldsOperation.getFields();

				if (operation instanceof InheritsFieldsAggregationOperation || exposedFieldsOperation.inheritsFields()) {
					contextToUse = new InheritingExposedFieldsAggregationOperationContext(fields, contextToUse);
				} else {
					contextToUse = fields.exposesNoFields() ? DEFAULT_CONTEXT
							: new ExposedFieldsAggregationOperationContext(fields, contextToUse);
				}
			}
		}

		return operationDocuments;
	}

	/**
	 * Simple {@link AggregationOperationContext} that just returns {@link FieldReference}s as is.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 */
	private static class NoOpAggregationOperationContext implements AggregationOperationContext {

		@Override
		public Document getMappedObject(Document document, @Nullable Class<?> type) {
			return document;
		}

		@Override
		public FieldReference getReference(Field field) {
			return new DirectFieldReference(new ExposedField(field, true));
		}

		@Override
		public FieldReference getReference(String name) {
			return new DirectFieldReference(new ExposedField(new AggregationField(name), true));
		}
	}
}
