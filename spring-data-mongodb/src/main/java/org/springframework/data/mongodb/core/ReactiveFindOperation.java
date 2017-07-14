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
import reactor.core.publisher.Mono;

import org.springframework.data.geo.GeoResult;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;

/**
 * {@link ReactiveFindOperation} allows creation and execution of reactive MongoDB find operations in a fluent API
 * style. <br />
 * The starting {@literal domainType} is used for mapping the {@link Query} provided via {@code matching} into the
 * MongoDB specific representation. By default, the originating {@literal domainType} is also used for mapping back the
 * result from the {@link org.bson.Document}. However, it is possible to define an different {@literal returnType} via
 * {@code as} to mapping the result.<br />
 * The collection to operate on is by default derived from the initial {@literal domainType} and can be defined there
 * via {@link org.springframework.data.mongodb.core.mapping.Document}. Using {@code inCollection} allows to override the
 * collection name for the execution.
 *
 * <pre>
 *     <code>
 *         query(Human.class)
 *             .inCollection("star-wars")
 *             .as(Jedi.class)
 *             .matching(query(where("firstname").is("luke")))
 *             .all();
 *     </code>
 * </pre>
 *
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveFindOperation {

	/**
	 * Start creating a find operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ReactiveFind}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ReactiveFind<T> query(Class<T> domainType);

	/**
	 * Compose find execution by calling one of the terminating methods.
	 */
	interface TerminatingFind<T> {

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@link Mono#empty()} if no match found.
		 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
		 */
		Mono<T> one();

		/**
		 * Get the first or no result.
		 *
		 * @return {@link Mono#empty()} if no match found.
		 */
		Mono<T> first();

		/**
		 * Get all matching elements.
		 *
		 * @return never {@literal null}.
		 */
		Flux<T> all();

		/**
		 * Get the number of matching elements.
		 *
		 * @return total number of matching elements.
		 */
		Mono<Long> count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@literal true} if at least one matching element exists.
		 */
		Mono<Boolean> exists();
	}

	/**
	 * Compose geonear execution by calling one of the terminating methods.
	 */
	interface TerminatingFindNear<T> {

		/**
		 * Find all matching elements and return them as {@link org.springframework.data.geo.GeoResult}.
		 *
		 * @return never {@literal null}.
		 */
		Flux<GeoResult<T>> all();
	}

	/**
	 * Provide a {@link Query} override (optional).
	 */
	interface FindWithQuery<T> extends TerminatingFind<T> {

		/**
		 * Set the filter query to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link TerminatingFind}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingFind<T> matching(Query query);

		/**
		 * Set the filter query for the geoNear execution.
		 *
		 * @param nearQuery must not be {@literal null}.
		 * @return new instance of {@link TerminatingFindNear}.
		 * @throws IllegalArgumentException if nearQuery is {@literal null}.
		 */
		TerminatingFindNear<T> near(NearQuery nearQuery);
	}

	/**
	 * Collection override (optional).
	 */
	interface FindWithCollection<T> extends FindWithQuery<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link FindWithProjection}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		FindWithProjection<T> inCollection(String collection);
	}

	/**
	 * Result type override (optional).
	 */
	interface FindWithProjection<T> extends FindWithQuery<T> {

		/**
		 * Define the target type fields should be mapped to. <br />
		 * Skip this step if you are anyway only interested in the original domain type.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 * @return new instance of {@link FindWithProjection}.
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 */
		<R> FindWithQuery<R> as(Class<R> resultType);
	}

	/**
	 * {@link ReactiveFind} provides methods for constructing lookup operations in a fluent way.
	 */
	interface ReactiveFind<T> extends FindWithCollection<T>, FindWithProjection<T> {}
}
