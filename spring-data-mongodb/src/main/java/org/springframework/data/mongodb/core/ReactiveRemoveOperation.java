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

import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.client.result.DeleteResult;

/**
 * {@link ReactiveRemoveOperation} allows creation and execution of reactive MongoDB remove / findAndRemove operations
 * in a fluent API style. <br />
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
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveRemoveOperation {

	/**
	 * Start creating a remove operation for the given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ReactiveRemove}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ReactiveRemove<T> remove(Class<T> domainType);

	/**
	 * Compose remove execution by calling one of the terminating methods.
	 */
	interface TerminatingRemove<T> {

		/**
		 * Remove all documents matching.
		 *
		 * @return the {@link DeleteResult}. Never {@literal null}.
		 */
		Mono<DeleteResult> all();

		/**
		 * Remove and return all matching documents. <br/>
		 * <strong>NOTE</strong> The entire list of documents will be fetched before sending the actual delete commands.
		 * Also, {@link org.springframework.context.ApplicationEvent}s will be published for each and every delete
		 * operation.
		 *
		 * @return empty {@link Flux} if no match found. Never {@literal null}.
		 */
		Flux<T> findAndRemove();
	}

	/**
	 * Collection override (optional).
	 */
	interface RemoveWithCollection<T> extends RemoveWithQuery<T> {

		/**
		 * Explicitly set the name of the collection to perform the query on. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link RemoveWithCollection}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		RemoveWithQuery<T> inCollection(String collection);
	}

	/**
	 * Provide a {@link Query} override (optional).
	 */
	interface RemoveWithQuery<T> extends TerminatingRemove<T> {

		/**
		 * Define the query filtering elements.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link TerminatingRemove}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingRemove<T> matching(Query query);
	}

	interface ReactiveRemove<T> extends RemoveWithCollection<T> {}
}
