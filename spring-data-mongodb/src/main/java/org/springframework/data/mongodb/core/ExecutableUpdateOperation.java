/*
 * Copyright 2017-2019 the original author or authors.
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

import java.util.Optional;

import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.lang.Nullable;

import com.mongodb.client.result.UpdateResult;

/**
 * {@link ExecutableUpdateOperation} allows creation and execution of MongoDB update / findAndModify / findAndReplace
 * operations in a fluent API style. <br />
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
 * @author Mark Paluch
 * @since 2.0
 */
public interface ExecutableUpdateOperation {

	/**
	 * Start creating an update operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ExecutableUpdate}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ExecutableUpdate<T> update(Class<T> domainType);

	/**
	 * Trigger findAndModify execution by calling one of the terminating methods.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.0
	 */
	interface TerminatingFindAndModify<T> {

		/**
		 * Find, modify and return the first matching document.
		 *
		 * @return {@link Optional#empty()} if nothing found.
		 */
		default Optional<T> findAndModify() {
			return Optional.ofNullable(findAndModifyValue());
		}

		/**
		 * Find, modify and return the first matching document.
		 *
		 * @return {@literal null} if nothing found.
		 */
		@Nullable
		T findAndModifyValue();
	}

	/**
	 * Trigger
	 * <a href="https://docs.mongodb.com/manual/reference/method/db.collection.findOneAndReplace/">findOneAndReplace<a/>
	 * execution by calling one of the terminating methods.
	 *
	 * @author Mark Paluch
	 * @since 2.1
	 */
	interface TerminatingFindAndReplace<T> {

		/**
		 * Find, replace and return the first matching document.
		 *
		 * @return {@link Optional#empty()} if nothing found.
		 */
		default Optional<T> findAndReplace() {
			return Optional.ofNullable(findAndReplaceValue());
		}

		/**
		 * Find, replace and return the first matching document.
		 *
		 * @return {@literal null} if nothing found.
		 */
		@Nullable
		T findAndReplaceValue();
	}

	/**
	 * Trigger update execution by calling one of the terminating methods.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface TerminatingUpdate<T> extends TerminatingFindAndModify<T>, FindAndModifyWithOptions<T> {

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

	/**
	 * Declare the {@link Update} to apply.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface UpdateWithUpdate<T> {

		/**
		 * Set the {@link UpdateDefinition} to be applied.
		 *
		 * @param update must not be {@literal null}.
		 * @return new instance of {@link TerminatingUpdate}.
		 * @throws IllegalArgumentException if update is {@literal null}.
		 * @since 3.0
		 * @see Update
		 * @see AggregationUpdate
		 */
		TerminatingUpdate<T> apply(UpdateDefinition update);

		/**
		 * Specify {@code replacement} object.
		 *
		 * @param replacement must not be {@literal null}.
		 * @return new instance of {@link FindAndReplaceOptions}.
		 * @throws IllegalArgumentException if options is {@literal null}.
		 * @since 2.1
		 */
		FindAndReplaceWithProjection<T> replaceWith(T replacement);
	}

	/**
	 * Explicitly define the name of the collection to perform operation in.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface UpdateWithCollection<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link UpdateWithCollection}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		UpdateWithQuery<T> inCollection(String collection);
	}

	/**
	 * Define a filter query for the {@link Update}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface UpdateWithQuery<T> extends UpdateWithUpdate<T> {

		/**
		 * Filter documents by given {@literal query}.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link UpdateWithQuery}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		UpdateWithUpdate<T> matching(Query query);
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
		TerminatingFindAndModify<T> withOptions(FindAndModifyOptions options);
	}

	/**
	 * Define {@link FindAndReplaceOptions}.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface FindAndReplaceWithOptions<T> extends TerminatingFindAndReplace<T> {

		/**
		 * Explicitly define {@link FindAndReplaceOptions} for the {@link Update}.
		 *
		 * @param options must not be {@literal null}.
		 * @return new instance of {@link FindAndReplaceOptions}.
		 * @throws IllegalArgumentException if options is {@literal null}.
		 */
		FindAndReplaceWithProjection<T> withOptions(FindAndReplaceOptions options);
	}

	/**
	 * Result type override (Optional).
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface FindAndReplaceWithProjection<T> extends FindAndReplaceWithOptions<T> {

		/**
		 * Define the target type fields should be mapped to. <br />
		 * Skip this step if you are anyway only interested in the original domain type.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 * @return new instance of {@link FindAndReplaceWithProjection}.
		 * @throws IllegalArgumentException if resultType is {@literal null}.
		 */
		<R> FindAndReplaceWithOptions<R> as(Class<R> resultType);

	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface ExecutableUpdate<T> extends UpdateWithCollection<T>, UpdateWithQuery<T>, UpdateWithUpdate<T> {}
}
