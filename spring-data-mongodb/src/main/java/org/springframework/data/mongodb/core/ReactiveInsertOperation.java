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

import java.util.Collection;

/**
 * {@link ReactiveInsertOperation} allows creation and execution of reactive MongoDB insert and bulk insert operations
 * in a fluent API style. <br />
 * The collection to operate on is by default derived from the initial {@literal domainType} and can be defined there
 * via {@link org.springframework.data.mongodb.core.mapping.Document}. Using {@code inCollection} allows to override the
 * collection name for the execution.
 *
 * <pre>
 *     <code>
 *         insert(Jedi.class)
 *             .inCollection("star-wars")
 *             .one(luke);
 *     </code>
 * </pre>
 *
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveInsertOperation {

	/**
	 * Start creating an insert operation for given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ReactiveInsert}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ReactiveInsert<T> insert(Class<T> domainType);

	/**
	 * Compose insert execution by calling one of the terminating methods.
	 */
	interface TerminatingInsert<T> {

		/**
		 * Insert exactly one object.
		 *
		 * @param object must not be {@literal null}.
		 * @throws IllegalArgumentException if object is {@literal null}.
		 */
		Mono<T> one(T object);

		/**
		 * Insert a collection of objects.
		 *
		 * @param objects must not be {@literal null}.
		 * @throws IllegalArgumentException if objects is {@literal null}.
		 */
		Flux<T> all(Collection<? extends T> objects);
	}

	interface ReactiveInsert<T> extends TerminatingInsert<T>, InsertWithCollection<T> {}

	/**
	 * Collection override (optional).
	 */
	interface InsertWithCollection<T> {

		/**
		 * Explicitly set the name of the collection. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link TerminatingInsert}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		TerminatingInsert<T> inCollection(String collection);
	}
}
