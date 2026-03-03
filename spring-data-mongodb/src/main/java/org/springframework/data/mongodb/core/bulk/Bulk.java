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
import org.springframework.util.Assert;

/**
 * Container for an ordered list of {@link BulkOperation bulk operations} that modify documents in one or more
 * collections within a single request. Execution can be {@link Order#ORDERED ordered} (serial, stop on first error) or
 * {@link Order#UNORDERED unordered} (possibly parallel, continue on errors).
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 5.1
 */
public interface Bulk {

	/**
	 * Returns the ordered list of bulk operations to execute.
	 *
	 * @return the ordered list of {@link BulkOperation operations}.
	 */
	List<BulkOperation> operations();

	/**
	 * Creates a new {@link Bulk} by applying the given builder customizer to a fresh {@link BulkBuilder}.
	 *
	 * @param builderCustomizer the customizer that configures the builder with operations.
	 * @return a new {@link Bulk} instance.
	 */
	static Bulk create(Consumer<BulkBuilder> builderCustomizer) {

		Assert.notNull(builderCustomizer, "Bulk builderCustomizer must not be null");

		BulkBuilder bulkBuilder = Bulk.builder();
		builderCustomizer.accept(bulkBuilder);
		return bulkBuilder.build();
	}

	/**
	 * Returns a new {@link BulkBuilder} to define and build a {@link Bulk}.
	 *
	 * <pre class="code">
	 * Bulk bulk = Bulk.builder()
	 *     .inCollection(Person.class, it -&gt; it..insert(p1).upsert(where(...)))
	 *     .inCollection("user", it -&gt; it.update(where(...), ...)
	 *     .build();
	 * </pre>
	 *
	 * @return a new {@link BulkBuilder}.
	 */
	static BulkBuilder builder() {
		return new DefaultBulkBuilder();
	}

	/**
	 * Builder for defining {@link BulkOperation bulk operations} across one or more collections.
	 */
	interface BulkBuilder {

		/**
		 * Adds operations for the given collection name within a scoped consumer.
		 *
		 * @param collectionName the target collection name; must not be {@literal null} or empty.
		 * @param builderCustomizer the consumer that defines operations for that collection.
		 * @return this builder.
		 */
		BulkBuilder inCollection(String collectionName, Consumer<BulkSpec> builderCustomizer);

		/**
		 * Adds operations for the collection mapped to the given domain type within a scoped consumer.
		 *
		 * @param type the domain type used to resolve the collection.
		 * @param builderCustomizer the consumer that defines operations for that collection.
		 * @param <T> the domain type.
		 * @return this builder.
		 */
		<T> BulkBuilder inCollection(Class<T> type, Consumer<BulkSpec> builderCustomizer);

		/**
		 * Adds operations for the collection mapped to the given domain type within a scoped consumer.
		 *
		 * @param type the domain type used to map domain objects.
		 * @param collectionName the target collection name; must not be {@literal null} or empty.
		 * @param builderCustomizer the consumer that defines operations for that collection.
		 * @param <T> the domain type.
		 * @return this builder.
		 */
		<T> BulkBuilder inCollection(Class<T> type, String collectionName, Consumer<BulkSpec> builderCustomizer);

		/**
		 * Builds the {@link Bulk} with all operations added so far.
		 *
		 * @return the built {@link Bulk}.
		 */
		Bulk build();

	}

	/**
	 * Builder for adding bulk operations (insert, update, replace, remove) to a single collection.
	 */
	interface BulkSpec {

		/**
		 * Adds an insert of the given document.
		 *
		 * @param object the document to insert.
		 * @return this builder.
		 */
		BulkSpec insert(Object object);

		/**
		 * Adds inserts for all given documents.
		 *
		 * @param objects the documents to insert.
		 * @return this builder.
		 */
		BulkSpec insertAll(Iterable<? extends Object> objects);

		/**
		 * Adds an update-one operation (update at most one document matching the criteria).
		 *
		 * @param where criteria to select the document.
		 * @param update the update to apply.
		 * @return this builder.
		 */
		default BulkSpec updateOne(CriteriaDefinition where, UpdateDefinition update) {
			return updateOne(Query.query(where), update);
		}

		/**
		 * Adds an update-one operation (update at most one document matching the filter).
		 *
		 * @param filter the query to select the document.
		 * @param update the update to apply.
		 * @return this builder.
		 */
		BulkSpec updateOne(Query filter, UpdateDefinition update);

		/**
		 * Adds an update-many operation (update all documents matching the criteria).
		 *
		 * @param where criteria to select the document.
		 * @param update the update to apply.
		 * @return this builder.
		 */
		default BulkSpec updateMulti(CriteriaDefinition where, UpdateDefinition update) {
			return updateMulti(Query.query(where), update);
		}

		/**
		 * Adds an update-many operation (update all documents matching the filter).
		 *
		 * @param filter the query to select documents.
		 * @param update the update to apply.
		 * @return this builder.
		 */
		BulkSpec updateMulti(Query filter, UpdateDefinition update);

		/**
		 * Adds an upsert operation (update if a document matches the criteria, otherwise insert).
		 *
		 * @param criteria the match criteria to find an existing document.
		 * @param update the update to apply or use for the new dozcument.
		 * @return this builder.
		 */
		default BulkSpec upsert(CriteriaDefinition criteria, UpdateDefinition update) {
			return upsert(Query.query(criteria), update);
		}

		/**
		 * Adds an upsert operation (update if a document matches the filter, otherwise insert).
		 *
		 * @param filter the query to find an existing document.
		 * @param update the update to apply or use for the new document.
		 * @return this builder.
		 */
		BulkSpec upsert(Query filter, UpdateDefinition update);

		/**
		 * Adds a remove operation (delete all documents matching the criteria).
		 *
		 * @param criteria the match criteria for documents to delete.
		 * @return this builder.
		 */
		default BulkSpec remove(CriteriaDefinition criteria) {
			return remove(Query.query(criteria));
		}

		/**
		 * Adds a remove operation (delete all documents matching the filter).
		 *
		 * @param filter the query to select documents to delete.
		 * @return this builder.
		 */
		BulkSpec remove(Query filter);

		/**
		 * Adds a replace-one operation (replace at most one document matching the criteria).
		 *
		 * @param where the criteria to select the document.
		 * @param replacement the replacement document.
		 * @return this builder.
		 */
		default BulkSpec replaceOne(CriteriaDefinition where, Object replacement) {
			return replaceOne(Query.query(where), replacement);
		}

		/**
		 * Adds a replace-one operation (replace at most one document matching the filter).
		 *
		 * @param filter the query to select the document.
		 * @param replacement the replacement document.
		 * @return this builder.
		 */
		BulkSpec replaceOne(Query filter, Object replacement);

		/**
		 * Adds a replace-one-if-exists operation (replace only if a document matches the criteria).
		 *
		 * @param where the criteria to select the document.
		 * @param replacement the replacement document.
		 * @return this builder.
		 */
		default BulkSpec replaceIfExists(CriteriaDefinition where, Object replacement) {
			return replaceIfExists(Query.query(where), replacement);
		}

		/**
		 * Adds a replace-one-if-exists operation (replace only if a document matches the filter).
		 *
		 * @param filter the query to select the document.
		 * @param replacement the replacement document.
		 * @return this builder.
		 */
		BulkSpec replaceIfExists(Query filter, Object replacement);

	}

}
