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

import java.util.Optional;

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.client.result.UpdateResult;

/**
 * {@link ExecutableUpdateOperation} allows creation and execution of MongoDB update / findAndModify operations in a
 * fluent API style. <br />
 * The starting {@literal domainType} is used for mapping the {@link Query} provided via {@code matching}, as well as
 * the {@link Update} via {@code apply} into the MongoDB specific representations. The collection to operate on is by
 * default derived from the initial {@literal domainType} and can be defined there via
 * {@link org.springframework.data.mongodb.core.mapping.Document}. Using {@code inCollection} allows to override the
 * collection name for the execution.
 *
 * <pre>
 *     <code>
 *         update(Jedi.class)
 *             .inCollection("star-wars")
 *             .matching(query(where("firstname").is("luke")))
 *             .apply(new Update().set("lastname", "skywalker"))
 *             .upsert();
 *     </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ExecutableUpdateOperation {

	/**
	 * Start creating an update operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link UpdateOperation}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> UpdateOperation<T> update(Class<T> domainType);

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface UpdateOperation<T>
			extends UpdateOperationWithCollection<T>, UpdateOperationWithQuery<T>, UpdateOperationWithUpdate<T> {}

	/**
	 * Declare the {@link Update} to apply.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface UpdateOperationWithUpdate<T> {

		/**
		 * Set the {@link Update} to be applied.
		 *
		 * @param update must not be {@literal null}.
		 * @return new instance of {@link TerminatingUpdateOperation}.
		 * @throws IllegalArgumentException if update is {@literal null}.
		 */
		TerminatingUpdateOperation<T> apply(Update update);
	}

	/**
	 * Explicitly define the name of the collection to perform operation in.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface UpdateOperationWithCollection<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link UpdateOperationWithCollection}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		UpdateOperationWithQuery<T> inCollection(String collection);
	}

	/**
	 * Define a filter query for the {@link Update}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface UpdateOperationWithQuery<T> extends UpdateOperationWithUpdate<T> {

		/**
		 * Filter documents by given {@literal query}.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link UpdateOperationWithQuery}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		UpdateOperationWithUpdate<T> matching(Query query);
	}

	/**
	 * Define {@link FindAndModifyOptions}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface FindAndModifyWithOptions<T> {

		/**
		 * Explicitly define {@link FindAndModifyOptions} for the {@link Update}.
		 *
		 * @param options must not be {@literal null}.
		 * @return new instance of {@link FindAndModifyWithOptions}.
		 * @throws IllegalArgumentException if options is {@literal null}.
		 */
		TerminatingFindAndModifyOperation<T> withOptions(FindAndModifyOptions options);
	}

	/**
	 * Trigger findAndModify execution by calling one of the terminating methods.
	 */
	interface TerminatingFindAndModifyOperation<T> {

		/**
		 * Find, modify and return the first matching document.
		 *
		 * @return {@link Optional#empty()} if nothing found.
		 */
		Optional<T> findAndModify();
	}

	/**
	 * Trigger update execution by calling one of the terminating methods.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface TerminatingUpdateOperation<T> extends TerminatingFindAndModifyOperation<T>, FindAndModifyWithOptions<T> {

		/**
		 * Update all matching documents in the collection.
		 *
		 * @return never {@literal null}.
		 */
		UpdateResult all();

		/**
		 * Update the first document in the collection.
		 *
		 * @return never {@literal null}.
		 */
		UpdateResult first();

		/**
		 * Creates a new document if no documents match the filter query or updates the matching ones.
		 *
		 * @return never {@literal null}.
		 */
		UpdateResult upsert();
	}
}
