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

import reactor.core.publisher.Flux;

import org.springframework.data.mongodb.core.aggregation.Aggregation;

/**
 * {@link ReactiveAggregationOperation} allows creation and execution of reactive MongoDB aggregation operations in a
 * fluent API style. <br />
 * The starting {@literal domainType} is used for mapping the {@link Aggregation} provided via {@code by} into the
 * MongoDB specific representation, as well as mapping back the resulting {@link org.bson.Document}. An alternative
 * input type for mapping the {@link Aggregation} can be provided by using
 * {@link org.springframework.data.mongodb.core.aggregation.TypedAggregation}.
 *
 * <pre>
 *     <code>
 *         aggregateAndReturn(Jedi.class)
 *             .by(newAggregation(Human.class, project("These are not the droids you are looking for")))
 *             .all();
 *     </code>
 * </pre>
 *
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveAggregationOperation {

	/**
	 * Start creating an aggregation operation that returns results mapped to the given domain type. <br />
	 * Use {@link org.springframework.data.mongodb.core.aggregation.TypedAggregation} to specify a potentially different
	 * input type for he aggregation.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ReactiveAggregation}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ReactiveAggregation<T> aggregateAndReturn(Class<T> domainType);

	/**
	 * Collection override (optional).
	 */
	interface AggregationOperationWithCollection<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link AggregationOperationWithAggregation}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		AggregationOperationWithAggregation<T> inCollection(String collection);
	}

	/**
	 * Trigger execution by calling one of the terminating methods.
	 */
	interface TerminatingAggregationOperation<T> {

		/**
		 * Apply pipeline operations as specified and stream all matching elements. <br />
		 *
		 * @return a {@link Flux} streaming all matching elements. Never {@literal null}.
		 */
		Flux<T> all();
	}

	/**
	 * Define the aggregation with pipeline stages.
	 */
	interface AggregationOperationWithAggregation<T> {

		/**
		 * Set the aggregation to be used.
		 *
		 * @param aggregation must not be {@literal null}.
		 * @return new instance of {@link TerminatingAggregationOperation}.
		 * @throws IllegalArgumentException if aggregation is {@literal null}.
		 */
		TerminatingAggregationOperation<T> by(Aggregation aggregation);
	}

	interface ReactiveAggregation<T>
			extends AggregationOperationWithCollection<T>, AggregationOperationWithAggregation<T> {}
}
