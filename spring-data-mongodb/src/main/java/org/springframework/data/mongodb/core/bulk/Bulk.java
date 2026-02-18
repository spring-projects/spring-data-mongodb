/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb.core.bulk;

import java.util.List;
import java.util.function.Consumer;

import org.springframework.data.mongodb.core.bulk.BulkWriteOptions.Order;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;

/**
 * Container for an ordered list of {@link BulkOperation bulk operations} that modify documents in one or more
 * collections within a single request. Execution can be {@link Order#ORDERED ordered} (serial, stop on first error) or
 * {@link Order#UNORDERED unordered} (possibly parallel, continue on errors).
 *
 * @author Christoph Strobl
 * @since 5.1
 */
public interface Bulk {

	/**
	 * Returns the ordered list of bulk operations to execute.
	 *
	 * @return the ordered list of {@link BulkOperation operations}; never {@literal null}.
	 */
	List<BulkOperation> operations();

	/**
	 * Creates a new {@link Bulk} by applying the given consumer to a fresh {@link BulkBuilder}.
	 *
	 * @param consumer the consumer that configures the builder with operations; must not be {@literal null}.
	 * @return a new {@link Bulk} instance; never {@literal null}.
	 */
	static Bulk create(Consumer<BulkBuilder> consumer) {
		BulkBuilder bulkBuilder = Bulk.builder();
		consumer.accept(bulkBuilder);
		return bulkBuilder.build();
	}

	/**
	 * Returns a new {@link BulkBuilder} to define and build a {@link Bulk}.
	 *
	 * <pre class="code">
	 * Bulk bulk = Bulk.builder()
	 *     .inCollection(Person.class).insert(p1).upsert(where(....
	 *     .inCollection("user").update(where(...
	 *     .build();
	 * </pre>
	 *
	 * @return a new {@link BulkBuilder}; never {@literal null}.
	 */
	static BulkBuilder builder() {
		return new NamespaceAwareBulkBuilder<>();
	}

	/**
	 * Builder for defining {@link BulkOperation bulk operations} across one or more collections.
	 */
	interface BulkBuilder {

		/**
		 * Adds operations for the given collection name within a scoped consumer.
		 *
		 * @param collectionName the target collection name; must not be {@literal null} or empty.
		 * @param scoped the consumer that defines operations for that collection; must not be {@literal null}.
		 * @return this.
		 */
		BulkBuilder inCollection(String collectionName, Consumer<BulkBuilderBase<Object>> scoped);

		/**
		 * Adds operations for the collection mapped to the given domain type within a scoped consumer.
		 *
		 * @param type the domain type used to resolve the collection; must not be {@literal null}.
		 * @param scoped the consumer that defines operations for that collection; must not be {@literal null}.
		 * @param <T> the domain type.
		 * @return this.
		 */
		<T> BulkBuilder inCollection(Class<T> type, Consumer<BulkBuilderBase<T>> scoped);

		/**
		 * Switches the target to the given collection by name. Subsequent operations apply to this collection until another
		 * collection is selected.
		 *
		 * @param collectionName the target collection name; must not be {@literal null} or empty.
		 * @return a collection bound builder; never {@literal null}.
		 */
		NamespaceBoundBulkBuilder<Object> inCollection(String collectionName);

		/**
		 * Switches the target to the collection mapped to the given domain type.
		 *
		 * @param type the domain type used to resolve the collection; must not be {@literal null}.
		 * @param <T> the domain type.
		 * @return a collection bound builder; never {@literal null}.
		 */
		<T> NamespaceBoundBulkBuilder<T> inCollection(Class<T> type);

		/**
		 * Switches the target to the given collection name, using the given type for mapping.
		 *
		 * @param collectionName the target collection name; must not be {@literal null} or empty.
		 * @param type the domain type used for mapping; must not be {@literal null}.
		 * @param <T> the domain type.
		 * @return a collection bound builder; never {@literal null}.
		 */
		<T> NamespaceBoundBulkBuilder<T> inCollection(String collectionName, Class<T> type);

		/**
		 * Builds the {@link Bulk} with all operations added so far.
		 *
		 * @return the built {@link Bulk}; never {@literal null}.
		 */
		Bulk build();
	}

	/**
	 * Builder for adding bulk operations (insert, update, replace, remove) to a single collection.
	 *
	 * @param <T> the domain type for the target collection.
	 */
	interface BulkBuilderBase<T> {

		/**
		 * Adds an insert of the given document.
		 *
		 * @param object the document to insert; must not be {@literal null}.
		 * @return this.
		 */
		BulkBuilderBase<T> insert(T object);

		/**
		 * Adds inserts for all given documents.
		 *
		 * @param objects the documents to insert; must not be {@literal null}.
		 * @return this.
		 */
		BulkBuilderBase<T> insertAll(Iterable<? extends T> objects);

		/** Adds an update-one operation (update at most one document matching the criteria). */
		default BulkBuilderBase<T> updateOne(CriteriaDefinition where, UpdateDefinition update) {
			return updateOne(Query.query(where), update);
		}

		/**
		 * Adds an update-one operation (update at most one document matching the filter).
		 *
		 * @param filter the query to select the document; must not be {@literal null}.
		 * @param update the update to apply; must not be {@literal null}.
		 * @return this.
		 */
		BulkBuilderBase<T> updateOne(Query filter, UpdateDefinition update);

		/** Adds an update-many operation (update all documents matching the criteria). */
		default BulkBuilderBase<T> updateMulti(CriteriaDefinition where, UpdateDefinition update) {
			return updateMulti(Query.query(where), update);
		}

		/**
		 * Adds an update-many operation (update all documents matching the filter).
		 *
		 * @param filter the query to select documents; must not be {@literal null}.
		 * @param update the update to apply; must not be {@literal null}.
		 * @return this.
		 */
		BulkBuilderBase<T> updateMulti(Query filter, UpdateDefinition update);

		/** Adds an upsert operation (update if a document matches, otherwise insert). */
		default BulkBuilderBase<T> upsert(CriteriaDefinition where, UpdateDefinition update) {
			return upsert(Query.query(where), update);
		}

		/**
		 * Adds an upsert operation (update if a document matches the filter, otherwise insert).
		 *
		 * @param filter the query to find an existing document; must not be {@literal null}.
		 * @param update the update to apply or use for the new document; must not be {@literal null}.
		 * @return this.
		 */
		BulkBuilderBase<T> upsert(Query filter, UpdateDefinition update);

		/** Adds a remove operation (delete all documents matching the criteria). */
		default BulkBuilderBase<T> remove(CriteriaDefinition where) {
			return remove(Query.query(where));
		}

		/**
		 * Adds a remove operation (delete all documents matching the filter).
		 *
		 * @param filter the query to select documents to delete; must not be {@literal null}.
		 * @return this.
		 */
		BulkBuilderBase<T> remove(Query filter);

		/** Adds a replace-one operation (replace at most one document matching the criteria). */
		default BulkBuilderBase<T> replaceOne(CriteriaDefinition where, Object replacement) {
			return replaceOne(Query.query(where), replacement);
		}

		/**
		 * Adds a replace-one operation (replace at most one document matching the filter).
		 *
		 * @param filter the query to select the document; must not be {@literal null}.
		 * @param replacement the replacement document; must not be {@literal null}.
		 * @return this.
		 */
		BulkBuilderBase<T> replaceOne(Query filter, Object replacement);

		/** Adds a replace-one-if-exists operation (replace only if a document matches the criteria). */
		default BulkBuilderBase<T> replaceIfExists(CriteriaDefinition where, Object replacement) {
			return replaceIfExists(Query.query(where), replacement);
		}

		/**
		 * Adds a replace-one-if-exists operation (replace only if a document matches the filter).
		 *
		 * @param filter the query to select the document; must not be {@literal null}.
		 * @param replacement the replacement document; must not be {@literal null}.
		 * @return this.
		 */
		BulkBuilderBase<T> replaceIfExists(Query filter, Object replacement);
	}

	/**
	 * Builder for bulk operations that is bound to a specific collection (namespace). Extends both {@link BulkBuilder}
	 * (to switch collection or build) and {@link BulkBuilderBase} (to add operations in the current collection).
	 *
	 * @param <T> the domain type for the bound collection.
	 */
	interface NamespaceBoundBulkBuilder<T> extends BulkBuilderBase<T>, BulkBuilder {

		@Override
		NamespaceBoundBulkBuilder<T> insert(T object);

		@Override
		NamespaceBoundBulkBuilder<T> insertAll(Iterable<? extends T> objects);

		default NamespaceBoundBulkBuilder<T> updateOne(CriteriaDefinition where, UpdateDefinition update) {
			return updateOne(Query.query(where), update);
		}

		@Override
		NamespaceBoundBulkBuilder<T> updateOne(Query filter, UpdateDefinition update);

		default NamespaceBoundBulkBuilder<T> updateMulti(CriteriaDefinition where, UpdateDefinition update) {
			return updateMulti(Query.query(where), update);
		}

		@Override
		NamespaceBoundBulkBuilder<T> updateMulti(Query filter, UpdateDefinition update);

		default NamespaceBoundBulkBuilder<T> upsert(CriteriaDefinition where, UpdateDefinition update) {
			return upsert(Query.query(where), update);
		}

		@Override
		NamespaceBoundBulkBuilder<T> upsert(Query filter, UpdateDefinition update);

		default NamespaceBoundBulkBuilder<T> remove(CriteriaDefinition where) {
			return remove(Query.query(where));
		}

		@Override
		NamespaceBoundBulkBuilder<T> remove(Query filter);

		default NamespaceBoundBulkBuilder<T> replaceOne(CriteriaDefinition where, Object replacement) {
			return replaceOne(Query.query(where), replacement);
		}

		@Override
		NamespaceBoundBulkBuilder<T> replaceOne(Query filter, Object replacement);

		default NamespaceBoundBulkBuilder<T> replaceIfExists(CriteriaDefinition where, Object replacement) {
			return replaceIfExists(Query.query(where), replacement);
		}

		@Override
		NamespaceBoundBulkBuilder<T> replaceIfExists(Query filter, Object replacement);
	}

}
