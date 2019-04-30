/*
 * Copyright 2013-2019 the original author or authors.
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

import java.util.Collections;
import java.util.List;

import org.bson.Document;

/**
 * Represents one single operation in an aggregation pipeline.
 *
 * @author Sebastian Herold
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @since 1.3
 */
public interface AggregationOperation {

	/**
	 * Turns the {@link AggregationOperation} into a {@link Document} by using the given
	 * {@link AggregationOperationContext}.
	 *
	 * @param context the {@link AggregationOperationContext} to operate within. Must not be {@literal null}.
	 * @return the Document
	 * @deprecated since 2.2 in favor of {@link #toPipelineStages(AggregationOperationContext)}.
	 */
	@Deprecated
	Document toDocument(AggregationOperationContext context);

	/**
	 * Turns the {@link AggregationOperation} into list of {@link Document stages} by using the given
	 * {@link AggregationOperationContext}. This allows a single {@link AggregationOptions} to add additional stages for
	 * eg. {@code $sort} or {@code $limit}.
	 *
	 * @param context the {@link AggregationOperationContext} to operate within. Must not be {@literal null}.
	 * @return the pipeline stages to run through. Never {@literal null}.
	 * @since 2.2
	 */
	default List<Document> toPipelineStages(AggregationOperationContext context) {
		return Collections.singletonList(toDocument(context));
	}
}
