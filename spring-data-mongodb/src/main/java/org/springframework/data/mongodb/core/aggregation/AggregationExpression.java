/*
 * Copyright 2015-2018 the original author or authors.
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

import org.bson.Document;

/**
 * An {@link AggregationExpression} can be used with field expressions in aggregation pipeline stages like
 * {@code project} and {@code group}.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public interface AggregationExpression {

	/**
	 * Turns the {@link AggregationExpression} into a {@link Document} within the given
	 * {@link AggregationOperationContext}.
	 *
	 * @param context
	 * @return
	 */
	Document toDocument(AggregationOperationContext context);
}
