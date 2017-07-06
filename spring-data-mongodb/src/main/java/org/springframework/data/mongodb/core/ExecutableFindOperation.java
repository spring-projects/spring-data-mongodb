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

import java.util.List;
import java.util.Optional;

import org.springframework.data.geo.GeoResults;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;

/**
 * {@link ExecutableFindOperation} allows creation and execution of MongoDB find operations in a fluent API style.
 * <br />
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
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface ExecutableFindOperation {

	/**
	 * Start creating a find operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link FindOperation}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> FindOperation<T> query(Class<T> domainType);

	/**
	 * Trigger find execution by calling one of the terminating methods.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface TerminatingFindOperation<T> {

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
		 */
		default Optional<T> one() {
			return Optional.ofNullable(oneValue());
		}

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@literal null} if no match found.
		 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
		 */
		T oneValue();

		/**
		 * Get the first or no result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 */
		default Optional<T> first() {
			return Optional.ofNullable(firstValue());
		}

		/**
		 * Get the first or no result.
		 *
		 * @return {@literal null} if no match found.
		 */
		T firstValue();

		/**
		 * Get all matching elements.
		 *
		 * @return never {@literal null}.
		 */
		List<T> all();

		/**
		 * Stream all matching elements.
		 *
		 * @return a {@link CloseableIterator} that wraps the a Mongo DB {@link com.mongodb.Cursor} that needs to be closed.
		 *         Never {@literal null}.
		 */
		CloseableIterator<T> stream();

		/**
		 * Get the number of matching elements.
		 *
		 * @return total number of matching elements.
		 */
		long count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@literal true} if at least one matching element exists.
		 */
		boolean exists();
	}

	/**
	 * Trigger geonear execution by calling one of the terminating methods.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface TerminatingFindNearOperation<T> {

		/**
		 * Find all matching elements and return them as {@link org.springframework.data.geo.GeoResult}.
		 *
		 * @return never {@literal null}.
		 */
		GeoResults<T> all();
	}

	/**
	 * Terminating operations invoking the actual query execution.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface FindOperationWithQuery<T> extends TerminatingFindOperation<T> {

		/**
		 * Set the filter query to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link TerminatingFindOperation}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingFindOperation<T> matching(Query query);

		/**
		 * Set the filter query for the geoNear execution.
		 *
		 * @param nearQuery must not be {@literal null}.
		 * @return new instance of {@link TerminatingFindNearOperation}.
		 * @throws IllegalArgumentException if nearQuery is {@literal null}.
		 */
		TerminatingFindNearOperation<T> near(NearQuery nearQuery);
	}

	/**
	 * Collection override (Optional).
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface FindOperationWithCollection<T> extends FindOperationWithQuery<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link FindOperationWithProjection}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		FindOperationWithProjection<T> inCollection(String collection);
	}

	/**
	 * Result type override (Optional).
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface FindOperationWithProjection<T> extends FindOperationWithQuery<T> {

		/**
		 * Define the target type fields should be mapped to. <br />
		 * Skip this step if you are anyway only interested in the original domain type.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 * @return new instance of {@link FindOperationWithProjection}.
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 */
		<R> FindOperationWithQuery<R> as(Class<R> resultType);
	}

	/**
	 * {@link FindOperation} provides methods for constructing lookup operations in a fluent way.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface FindOperation<T> extends FindOperationWithCollection<T>, FindOperationWithProjection<T> {}
}
