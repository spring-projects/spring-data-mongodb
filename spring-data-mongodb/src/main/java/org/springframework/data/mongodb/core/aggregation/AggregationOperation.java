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

import java.util.List;

import org.bson.Document;
import org.springframework.util.CollectionUtils;

/**
 * Represents one single operation in an aggregation pipeline.
 *
 * @author Sebastian Herold
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @since 1.3
 */
public interface AggregationOperation extends MultiOperationAggregationStage {

	/**
	 * @param context the {@link AggregationOperationContext} to operate within. Must not be {@literal null}.
	 * @return
	 */
	@Override
	Document toDocument(AggregationOperationContext context);

	/**
	 * More the exception than the default.
	 *
	 * @param context the {@link AggregationOperationContext} to operate within. Must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	@Override
	default List<Document> toPipelineStages(AggregationOperationContext context) {
		return List.of(toDocument(context));
	}

	/**
	 * Return the MongoDB operator that is used for this {@link AggregationOperation}. Aggregation operations should
	 * implement this method to avoid document rendering.
	 *
	 * @return the operator used for this {@link AggregationOperation}.
	 * @since 3.0.2
	 */
	default String getOperator() {
		return CollectionUtils.lastElement(toPipelineStages(Aggregation.DEFAULT_CONTEXT)).keySet().iterator().next();
	}
}
