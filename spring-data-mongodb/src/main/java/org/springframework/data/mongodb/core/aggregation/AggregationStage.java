/*
 * Copyright 2023 the original author or authors.
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

import org.bson.Document;

/**
 * Abstraction for a single
 * <a href="https://www.mongodb.com/docs/manual/reference/operator/aggregation-pipeline/#stages">Aggregation Pipeline
 * Stage</a> to be used within an {@link AggregationPipeline}.
 * <p>
 * An {@link AggregationStage} may operate upon domain specific types but will render to a ready to use store native
 * representation within a given {@link AggregationOperationContext context}. The most straight forward way of writing a
 * custom {@link AggregationStage} is just returning the raw document.
 * 
 * <pre class="code">
 * AggregationStage stage = (ctx) -> Document.parse("{ $sort : { borough : 1 } }");
 * </pre>
 * 
 * @author Christoph Strobl
 * @since 4.1
 */
public interface AggregationStage {

	/**
	 * Turns the {@link AggregationStage} into a {@link Document} by using the given {@link AggregationOperationContext}.
	 *
	 * @param context the {@link AggregationOperationContext} to operate within. Must not be {@literal null}.
	 * @return the ready to use {@link Document} representing the stage.
	 */
	Document toDocument(AggregationOperationContext context);
}
