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
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ExecutableRemoveOperationBuilder {

	/**
	 * Start creating a remove operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> RemoveOperationBuilder<T> remove(Class<T> domainType);

	/**
	 * Collection override (Optional).
	 *
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithCollectionBuilder<T> extends WithQueryBuilder<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return
		 * @throws IllegalArgumentException if domainType is {@literal null}.
		 */
		WithQueryBuilder<T> inCollection(String collection);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface RemoveOperationBuilderTerminatingOperations<T> {

		/**
		 * Remove all documents matching.
		 *
		 * @return
		 */
		DeleteResult remove();

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
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface WithQueryBuilder<T> extends RemoveOperationBuilderTerminatingOperations<T> {

		/**
		 * Define the query filtering elements.
		 *
		 * @param query must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if domainType is {@literal null}.
		 */
		RemoveOperationBuilderTerminatingOperations<T> matching(Query query);
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface RemoveOperationBuilder<T> extends WithCollectionBuilder<T> {}
}
