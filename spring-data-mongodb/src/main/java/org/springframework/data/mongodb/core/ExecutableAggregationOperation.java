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
 * {@link ExecutableAggregationOperation} allows creation and execution of MongoDB aggregation operations in a fluent
 * API style. <br />
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
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface ExecutableAggregationOperation {

	/**
	 * Start creating an aggregation operation that returns results mapped to the given domain type. <br />
	 * Use {@link org.springframework.data.mongodb.core.aggregation.TypedAggregation} to specify a potentially different
	 * input type for he aggregation.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ExecutableAggregation}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ExecutableAggregation<T> aggregateAndReturn(Class<T> domainType);

	/**
	 * Collection override (Optional).
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface AggregationWithCollection<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link AggregationWithAggregation}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		AggregationWithAggregation<T> inCollection(String collection);
	}

	/**
	 * Trigger execution by calling one of the terminating methods.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface TerminatingAggregation<T> {

		/**
		 * Apply pipeline operations as specified and get all matching elements.
		 *
		 * @return never {@literal null}.
		 */
		AggregationResults<T> all();

		/**
		 * Apply pipeline operations as specified and stream all matching elements. <br />
		 * Returns a {@link CloseableIterator} that wraps the a Mongo DB {@link com.mongodb.Cursor}
		 *
		 * @return a {@link CloseableIterator} that wraps the a Mongo DB {@link com.mongodb.Cursor} that needs to be closed.
		 *         Never {@literal null}.
		 */
		CloseableIterator<T> stream();
	}

	/**
	 * Define the aggregation with pipeline stages.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface AggregationWithAggregation<T> {

		/**
		 * Set the aggregation to be used.
		 *
		 * @param aggregation must not be {@literal null}.
		 * @return new instance of {@link TerminatingAggregation}.
		 * @throws IllegalArgumentException if aggregation is {@literal null}.
		 */
		TerminatingAggregation<T> by(Aggregation aggregation);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface ExecutableAggregation<T> extends AggregationWithCollection<T>, AggregationWithAggregation<T> {}
}
