/*
 * Copyright 2017-2018 the original author or authors.
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
 * Encapsulates the {@code $sample}-operation.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#sample(long)} instead of creating instances of this
 * class directly.
 *
 * @author Gustavo de Geus
 * @since 2.0
 * @see <a href="https://docs.mongodb.com/master/reference/operator/aggregation/sample/">MongoDB Aggregation Framework:
 *      $sample</a>
 */
public class SampleOperation implements AggregationOperation {

	private final long sampleSize;

	/**
	 * @param sampleSize number of documents to be randomly selected from its input.
	 */
	public SampleOperation(long sampleSize) {

		Assert.isTrue(sampleSize > 0, "Sample size must be greater than zero!");
		this.sampleSize = sampleSize;
	}

	/*
	  (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.BucketOperationSupport#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {
		return new Document("$sample", new Document("size", this.sampleSize));
	}
}
