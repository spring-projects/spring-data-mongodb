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

import org.springframework.data.geo.GeoResults;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ExecutableFindOperationBuilder {

	/**
	 * Start creating a find operation for the given {@literal domainType}. <br />
	 *
	 * @param domainType must not be {@literal null}.
	 * @param <T>
	 * @return
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	public <T> FindOperationBuilder<T> find(Class<T> domainType);

	/**
	 * Trigger find execution by calling one of the terminating methods.
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface FindOperationBuilderTerminatingOperations<T> {

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@literal null} if no match found.
		 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
		 */
		T one();

		/**
		 * Get the first or no result.
		 *
		 * @return {@literal null} if no match found.
		 */
		T first();

		/**
		 * Get all matching elements.
		 *
		 * @return never {@literal}.
		 */
		List<T> all();

		/**
		 * Stream all matching elements.
		 *
		 * @return a {@link CloseableIterator} that wraps the a Mongo DB {@link com.mongodb.Cursor} that needs to be closed.
		 *         Never {@literal null}.
		 */
		CloseableIterator<T> stream();
	}

	/**
	 * Trigger geonear execution by calling one of the terminating methods.
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface FindOperationBuilderTerminatingNearOperations<T> {

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
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithQueryBuilder<T> extends FindOperationBuilderTerminatingOperations<T> {

		/**
		 * [optional] Set the filter query to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		FindOperationBuilderTerminatingOperations<T> matching(Query query);

		/**
		 * [optional] Set the filter query for the geoNear execution.
		 *
		 * @param nearQuery must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if nearQuery is {@literal null}.
		 */
		FindOperationBuilderTerminatingNearOperations<T> near(NearQuery nearQuery);

	}

	/**
	 * Collection override (Optional).
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithCollectionBuilder<T> extends WithQueryBuilder<T> {

		/**
		 * [optional] Explicitly set the name of the collection to perform the query on. <br />
		 * Just skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		WithProjectionBuilder<T> inCollection(String collection);
	}

	/**
	 * Result type override (Optional).
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithProjectionBuilder<T> extends WithQueryBuilder<T> {

		/**
		 * [optional] Define the target type fields should be mapped to. <br />
		 * Just skip this step if you are anyway only interested in the original domain type.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <T>
		 * @return
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 */
		<T1> WithQueryBuilder<T1> as(Class<T1> resultType);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface FindOperationBuilder<T> extends WithCollectionBuilder<T>, WithProjectionBuilder<T> {

	}
}
