/*
 * Copyright 2017-2021 the original author or authors.
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
import org.springframework.util.Assert;

/**
 * Encapsulates the {@code $sampleRate}-operation.
 * <p>
 * We recommend using the static factory method {@link Aggregation#sampleRate(double)} instead of creating instances of this
 * class directly.
 *
 * @author James McNee
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/sampleRate/">MongoDB Aggregation Framework:
 * $sampleRate</a>
 * @since 3.2.4
 */
public class SampleRateOperation implements AggregationOperation {

	private final double sampleRate;

	/**
	 * @param sampleRate sample rate to determine number of documents to be randomly selected from the input.
	 */
	public SampleRateOperation(double sampleRate) {
		Assert.isTrue(sampleRate >= 0, "Sample rate must be greater than zero!");
		Assert.isTrue(sampleRate <= 1, "Sample rate must not be greater than one!");
		this.sampleRate = sampleRate;
	}

	/*
	  (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.BucketOperationSupport#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {
		return new Document(getOperator(), this.sampleRate);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#getOperator()
	 */
	@Override
	public String getOperator() {
		return "$sampleRate";
	}
}
