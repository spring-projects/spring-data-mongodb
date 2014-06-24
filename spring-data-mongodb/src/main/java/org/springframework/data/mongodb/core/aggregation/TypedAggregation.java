/*
 * Copyright 2013-2014 the original author or authors.
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

import java.util.List;

import org.springframework.util.Assert;

/**
 * A {@code TypedAggregation} is a special {@link Aggregation} that holds information of the input aggregation type.
 * 
 * @author Thomas Darimont
 * @author Oliver Gierke
 */
public class TypedAggregation<I> extends Aggregation {

	private final Class<I> inputType;

	/**
	 * Creates a new {@link TypedAggregation} from the given {@link AggregationOperation}s.
	 * 
	 * @param inputType must not be {@literal null}.
	 * @param operations must not be {@literal null} or empty.
	 */
	public TypedAggregation(Class<I> inputType, AggregationOperation... operations) {
		this(inputType, asAggregationList(operations));
	}

	/**
	 * Creates a new {@link TypedAggregation} from the given {@link AggregationOperation}s.
	 * 
	 * @param inputType must not be {@literal null}.
	 * @param operations must not be {@literal null} or empty.
	 */
	public TypedAggregation(Class<I> inputType, List<AggregationOperation> operations) {
		this(inputType, operations, DEFAULT_OPTIONS);
	}

	/**
	 * Creates a new {@link TypedAggregation} from the given {@link AggregationOperation}s and the given
	 * {@link AggregationOptions}.
	 * 
	 * @param inputType must not be {@literal null}.
	 * @param operations must not be {@literal null} or empty.
	 * @param options must not be {@literal null}.
	 */
	public TypedAggregation(Class<I> inputType, List<AggregationOperation> operations, AggregationOptions options) {

		super(operations, options);

		Assert.notNull(inputType, "Input type must not be null!");
		this.inputType = inputType;
	}

	/**
	 * Returns the input type for the {@link Aggregation}.
	 * 
	 * @return the inputType will never be {@literal null}.
	 */
	public Class<I> getInputType() {
		return inputType;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.Aggregation#withOptions(org.springframework.data.mongodb.core.aggregation.AggregationOptions)
	 */
	public TypedAggregation<I> withOptions(AggregationOptions options) {

		Assert.notNull(options, "AggregationOptions must not be null.");
		return new TypedAggregation<I>(inputType, operations, options);
	}
}
