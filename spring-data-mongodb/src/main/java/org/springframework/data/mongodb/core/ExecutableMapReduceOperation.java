/*
 * Copyright 2018-2022 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.List;

import org.springframework.data.mongodb.core.ExecutableFindOperation.ExecutableFind;
import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.Query;

/**
 * {@link ExecutableMapReduceOperation} allows creation and execution of MongoDB mapReduce operations in a fluent API
 * style. The starting {@literal domainType} is used for mapping an optional {@link Query} provided via {@code matching}
 * into the MongoDB specific representation. By default, the originating {@literal domainType} is also used for mapping
 * back the results from the {@link org.bson.Document}. However, it is possible to define an different
 * {@literal returnType} via {@code as} to mapping the result.<br />
 * The collection to operate on is by default derived from the initial {@literal domainType} and can be defined there
 * via {@link org.springframework.data.mongodb.core.mapping.Document}. Using {@code inCollection} allows to override the
 * collection name for the execution.
 *
 * <pre>
 *     <code>
 *         mapReduce(Human.class)
 *             .map("function() { emit(this.id, this.firstname) }")
 *             .reduce("function(id, name) { return sum(id, name); }")
 *             .inCollection("star-wars")
 *             .as(Jedi.class)
 *             .matching(query(where("lastname").is("skywalker")))
 *             .all();
 *     </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
public interface ExecutableMapReduceOperation {

	/**
	 * Start creating a mapReduce operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ExecutableFind}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> MapReduceWithMapFunction<T> mapReduce(Class<T> domainType);

	/**
	 * Trigger mapReduce execution by calling one of the terminating methods.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface TerminatingMapReduce<T> {

		/**
		 * Get the mapReduce results.
		 *
		 * @return never {@literal null}.
		 */
		List<T> all();
	}

	/**
	 * Provide the Javascript {@code function()} used to map matching documents.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface MapReduceWithMapFunction<T> {

		/**
		 * Set the Javascript map {@code function()}.
		 *
		 * @param mapFunction must not be {@literal null} nor empty.
		 * @return new instance of {@link MapReduceWithReduceFunction}.
		 * @throws IllegalArgumentException if {@literal mapFunction} is {@literal null} or empty.
		 */
		MapReduceWithReduceFunction<T> map(String mapFunction);

	}

	/**
	 * Provide the Javascript {@code function()} used to reduce matching documents.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface MapReduceWithReduceFunction<T> {

		/**
		 * Set the Javascript map {@code function()}.
		 *
		 * @param reduceFunction must not be {@literal null} nor empty.
		 * @return new instance of {@link ExecutableMapReduce}.
		 * @throws IllegalArgumentException if {@literal reduceFunction} is {@literal null} or empty.
		 */
		ExecutableMapReduce<T> reduce(String reduceFunction);

	}

	/**
	 * Collection override (Optional).
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface MapReduceWithCollection<T> extends MapReduceWithQuery<T> {

		/**
		 * Explicitly set the name of the collection to perform the mapReduce operation on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link MapReduceWithProjection}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		MapReduceWithProjection<T> inCollection(String collection);
	}

	/**
	 * Input document filter query (Optional).
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface MapReduceWithQuery<T> extends TerminatingMapReduce<T> {

		/**
		 * Set the filter query to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link TerminatingMapReduce}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingMapReduce<T> matching(Query query);

		/**
		 * Set the filter {@link CriteriaDefinition criteria} to be used.
		 *
		 * @param criteria must not be {@literal null}.
		 * @return new instance of {@link TerminatingMapReduce}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 * @since 3.0
		 */
		default TerminatingMapReduce<T> matching(CriteriaDefinition criteria) {
			return matching(Query.query(criteria));
		}
	}

	/**
	 * Result type override (Optional).
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface MapReduceWithProjection<T> extends MapReduceWithQuery<T> {

		/**
		 * Define the target type fields should be mapped to. <br />
		 * Skip this step if you are anyway only interested in the original domain type.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 * @return new instance of {@link TerminatingMapReduce}.
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 */
		<R> MapReduceWithQuery<R> as(Class<R> resultType);
	}

	/**
	 * Additional mapReduce options (Optional).
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 * @deprecated since 4.0 in favor of {@link org.springframework.data.mongodb.core.aggregation}.
	 */
	@Deprecated
	interface MapReduceWithOptions<T> {

		/**
		 * Set additional options to apply to the mapReduce operation.
		 *
		 * @param options must not be {@literal null}.
		 * @return new instance of {@link ExecutableMapReduce}.
		 * @throws IllegalArgumentException if options is {@literal null}.
		 */
		ExecutableMapReduce<T> with(MapReduceOptions options);
	}

	/**
	 * {@link ExecutableMapReduce} provides methods for constructing mapReduce operations in a fluent way.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface ExecutableMapReduce<T> extends MapReduceWithMapFunction<T>, MapReduceWithReduceFunction<T>,
			MapReduceWithCollection<T>, MapReduceWithProjection<T>, MapReduceWithOptions<T> {

	}
}
