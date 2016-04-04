/*
 * Copyright 2013-2015 the original author or authors.
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
import org.springframework.util.Assert;

/**
 * Encapsulates the {@code $limit}-operation.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#limit(long)} instead of creating instances of this
 * class directly.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/limit/
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @since 1.3
 */
public class LimitOperation implements AggregationOperation {

	private final long maxElements;

	/**
	 * @param maxElements Number of documents to consider.
	 */
	public LimitOperation(long maxElements) {

		Assert.isTrue(maxElements >= 0, "Maximum number of elements must be greater or equal to zero!");
		this.maxElements = maxElements;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {
		return new Document("$limit", Long.valueOf(maxElements));
	}
}
