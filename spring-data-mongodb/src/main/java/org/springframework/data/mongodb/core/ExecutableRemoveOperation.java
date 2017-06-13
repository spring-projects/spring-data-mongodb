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

import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.client.result.DeleteResult;

/**
 * {@link ExecutableRemoveOperation} allows creation and execution of MongoDB remove / findAndRemove operations in a
 * fluent API style. <br />
 * The starting {@literal domainType} is used for mapping the {@link Query} provided via {@code matching} into the
 * MongoDB specific representation. The collection to operate on is by default derived from the initial
 * {@literal domainType} and can be defined there via {@link org.springframework.data.mongodb.core.mapping.Document}.
 * Using {@code inCollection} allows to override the collection name for the execution.
 *
 * <pre>
 *     <code>
 *         remove(Jedi.class)
 *             .inCollection("star-wars")
 *             .matching(query(where("firstname").is("luke")))
 *             .all();
 *     </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ExecutableRemoveOperation {

	/**
	 * Start creating a remove operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link RemoveOperation}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> RemoveOperation<T> remove(Class<T> domainType);

	/**
	 * Collection override (Optional).
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface RemoveOperationWithCollection<T> extends RemoveOperationWithQuery<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link RemoveOperationWithCollection}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		RemoveOperationWithQuery<T> inCollection(String collection);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface TerminatingRemoveOperation<T> {

		/**
		 * Remove all documents matching.
		 *
		 * @return the {@link DeleteResult}. Never {@literal null}.
		 */
		DeleteResult all();

		/**
		 * Remove and return all matching documents. <br/>
		 * <strong>NOTE</strong> The entire list of documents will be fetched before sending the actual delete commands.
		 * Also, {@link org.springframework.context.ApplicationEvent}s will be published for each and every delete
		 * operation.
		 *
		 * @return empty {@link List} if no match found. Never {@literal null}.
		 */
		List<T> findAndRemove();
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface RemoveOperationWithQuery<T> extends TerminatingRemoveOperation<T> {

		/**
		 * Define the query filtering elements.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link TerminatingRemoveOperation}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingRemoveOperation<T> matching(Query query);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface RemoveOperation<T> extends RemoveOperationWithCollection<T> {}
}
