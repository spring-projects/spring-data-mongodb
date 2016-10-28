/*
 * Copyright 2013-2016 the original author or authors.
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
 * Encapsulates the aggregation framework {@code $skip}-operation.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#skip(int)} instead of creating instances of this
 * class directly.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/skip/
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @since 1.3
 */
public class SkipOperation implements AggregationOperation {

	private final long skipCount;

	/**
	 * Creates a new {@link SkipOperation} skipping the given number of elements.
	 * 
	 * @param skipCount number of documents to skip.
	 */
	public SkipOperation(long skipCount) {

		Assert.isTrue(skipCount >= 0, "Skip count must not be negative!");
		this.skipCount = skipCount;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {
		return new Document("$skip", skipCount);
	}
}
