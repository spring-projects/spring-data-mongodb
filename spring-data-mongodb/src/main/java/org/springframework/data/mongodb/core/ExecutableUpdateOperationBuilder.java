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

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.client.result.UpdateResult;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ExecutableUpdateOperationBuilder {

	/**
	 * Start creating an update operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> UpdateOperationBuilder<T> update(Class<T> domainType);

	/**
	 * Trigger update execution by calling one of the terminating methods.
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface UpdateOperationBuilderTerminatingOperations<T>
			extends UpdateOperationBuilderTerminatingFindAndModifyOperations<T> {

		/**
		 * Update the first document in the collection.
		 *
		 * @return
		 */
		UpdateResult first();

		/**
		 * Creates a new document if no documents match the filter query or updates the matching ones.
		 *
		 * @return
		 */
		UpdateResult upsert();

		/**
		 * Update all matching documents in the collection.
		 *
		 * @return
		 */
		UpdateResult all();
	}

	/**
	 * Trigger findAndModify execution by calling one of the terminating methods.
	 *
	 * @param <T>
	 */
	interface UpdateOperationBuilderTerminatingFindAndModifyOperations<T> {

		/**
		 * Find, modify and return the first matching document.
		 *
		 * @return
		 */
		T findAndModify();
	}

	/**
	 * Define a filter query for the {@link Update}.
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithQueryBuilder<T> extends WithFindAndModifyBuilder<T>, UpdateOperationBuilderTerminatingOperations<T> {

		/**
		 * Filter documents by given {@literal query}.
		 *
		 * @param filter must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if filter is {@literal null}.
		 */
		UpdateOperationBuilderTerminatingOperations<T> matching(Query filter);

	}

	/**
	 * Define a filter query for the {@link Update} used for {@literal findAndModify}.
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithFindAndModifyBuilder<T> {

		/**
		 * Find, modify and return the first matching document.
		 *
		 * @param filter must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if filter is {@literal null}.
		 */
		UpdateOperationBuilderTerminatingFindAndModifyOperations<T> matching(Query filter);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface UpdateOperationBuilder<T> {

		/**
		 * Set the {@link Update} to be applied.
		 *
		 * @param update must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if update is {@literal null}.
		 */
		WithOptionsBuilder<T> apply(Update update);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithCollectionBuilder<T> extends WithFindAndModifyOptionsBuilder<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		WithQueryBuilder<T> inCollection(String collection);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithOptionsBuilder<T> extends WithCollectionBuilder<T>, WithQueryBuilder<T> {}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithFindAndModifyOptionsBuilder<T> {

		/**
		 * Explicitly define {@link FindAndModifyOptions} for the {@link Update}.
		 *
		 * @param options must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if options is {@literal null}.
		 */
		WithFindAndModifyBuilder<T> withOptions(FindAndModifyOptions options);
	}
}
