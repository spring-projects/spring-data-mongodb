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

import java.util.Collection;

import org.springframework.data.mongodb.core.BulkOperations.BulkMode;

import com.mongodb.bulk.BulkWriteResult;

/**
 * {@link ExecutableInsertOperation} allows creation and execution of MongoDB insert and bulk insert operations in a
 * fluent API style. <br />
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
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface ExecutableInsertOperation {

	/**
	 * Start creating an insert operation for given {@literal domainType}.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return new instance of {@link ExecutableInsert}.
	 * @throws IllegalArgumentException if domainType is {@literal null}.
	 */
	<T> ExecutableInsert<T> insert(Class<T> domainType);

	/**
	 * Trigger insert execution by calling one of the terminating methods.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface TerminatingInsert<T> extends TerminatingBulkInsert<T> {

		/**
		 * Insert exactly one object.
		 *
		 * @param object must not be {@literal null}.
		 * @throws IllegalArgumentException if object is {@literal null}.
		 */
		void one(T object);

		/**
		 * Insert a collection of objects.
		 *
		 * @param objects must not be {@literal null}.
		 * @throws IllegalArgumentException if objects is {@literal null}.
		 */
		void all(Collection<? extends T> objects);
	}

	/**
	 * Trigger bulk insert execution by calling one of the terminating methods.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface TerminatingBulkInsert<T> {

		/**
		 * Bulk write collection of objects.
		 *
		 * @param objects must not be {@literal null}.
		 * @return resulting {@link BulkWriteResult}.
		 * @throws IllegalArgumentException if objects is {@literal null}.
		 */
		BulkWriteResult bulk(Collection<? extends T> objects);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface ExecutableInsert<T> extends TerminatingInsert<T>, InsertWithCollection<T>, InsertWithBulkMode<T> {}

	/**
	 * Collection override (optional).
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface InsertWithCollection<T> {

		/**
		 * Explicitly set the name of the collection. <br />
		 * Skip this step to use the default collection derived from the domain type.
		 *
		 * @param collection must not be {@literal null} nor {@literal empty}.
		 * @return new instance of {@link InsertWithBulkMode}.
		 * @throws IllegalArgumentException if collection is {@literal null}.
		 */
		InsertWithBulkMode<T> inCollection(String collection);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface InsertWithBulkMode<T> extends TerminatingInsert<T> {

		/**
		 * Define the {@link BulkMode} to use for bulk insert operation.
		 *
		 * @param bulkMode must not be {@literal null}.
		 * @return new instance of {@link TerminatingBulkInsert}.
		 * @throws IllegalArgumentException if bulkMode is {@literal null}.
		 */
		TerminatingBulkInsert<T> withBulkMode(BulkMode bulkMode);
	}
}
