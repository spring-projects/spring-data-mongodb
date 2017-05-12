/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.util.CloseableIterator;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ExecutableAggregationOperationBuilder {

	/**
	 * Start creating an aggregation operation that returns results mapped to the given domain type. <br />
	 * Use {@link org.springframework.data.mongodb.core.aggregation.TypedAggregation} to specify a potentially different
	 * input type for he aggregation.
	 *
	 * @param domainType must not be {@literal null}.
	 * @param <T>
	 * @return
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	public <T> AggregationOperationBuilder<T> aggregateAndReturn(Class<T> domainType);

	/**
	 * Collection override (Optional).
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithCollectionBuilder<T> {

		/**
		 * [optional] Explicitly set the name of the collection to perform the query on. <br />
		 * Just skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		WithAggregationBuilder<T> inCollection(String collection);
	}

	/**
	 * Trigger execution by calling one of the terminating methods.
	 *
	 * @param <T>
	 */
	interface AggregateOperationBuilderTerminatingOperations<T> {

		/**
		 * Apply pipeline operations as specified.
		 *
		 * @return
		 */
		AggregationResults<T> get();

		/**
		 * Apply pipeline operations as specified. <br />
		 * Returns a {@link CloseableIterator} that wraps the a Mongo DB {@link com.mongodb.Cursor}
		 *
		 * @return
		 */
		CloseableIterator<T> stream();
	}

	/**
	 * Define the aggregation with pipeline stages.
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithAggregationBuilder<T> {

		/**
		 * [required] Set the aggregation to be used.
		 *
		 * @param aggregation must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if aggregation is {@literal null}.
		 */
		AggregateOperationBuilderTerminatingOperations<T> by(Aggregation aggregation);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface AggregationOperationBuilder<T> extends WithCollectionBuilder<T>, WithAggregationBuilder<T> {

	}
}
