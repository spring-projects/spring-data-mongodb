/*
 * Copyright 2015-2023 the original author or authors.
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

import java.util.List;

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.util.Pair;

import com.mongodb.bulk.BulkWriteResult;

/**
 * Bulk operations for insert/update/remove actions on a collection. Bulk operations are available since MongoDB 2.6 and
 * make use of low level bulk commands on the protocol level. This interface defines a fluent API to add multiple single
 * operations or list of similar operations in sequence which can then eventually be executed by calling
 * {@link #execute()}.
 *
 * <pre class="code">
 * MongoOperations ops = …;
 *
 * ops.bulkOps(BulkMode.UNORDERED, Person.class)
 * 				.insert(newPerson)
 * 				.updateOne(where("firstname").is("Joe"), Update.update("lastname", "Doe"))
 * 				.execute();
 * </pre>
 * <p>
 * Bulk operations are issued as one batch that pulls together all insert, update, and delete operations. Operations
 * that require individual operation results such as optimistic locking (using {@code @Version}) are not supported and
 * the version field remains not populated.
 *
 * @author Tobias Trelle
 * @author Oliver Gierke
 * @author Minsu Kim
 * @since 1.9
 */
public interface BulkOperations {

	/**
	 * Mode for bulk operation.
	 **/
	enum BulkMode {

		/** Perform bulk operations in sequence. The first error will cancel processing. */
		ORDERED,

		/** Perform bulk operations in parallel. Processing will continue on errors. */
		UNORDERED
	}

	/**
	 * Add a single insert to the bulk operation.
	 *
	 * @param documents the document to insert, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the insert added, will never be {@literal null}.
	 */
	BulkOperations insert(Object documents);

	/**
	 * Add a list of inserts to the bulk operation.
	 *
	 * @param documents List of documents to insert, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the insert added, will never be {@literal null}.
	 */
	BulkOperations insert(List<? extends Object> documents);

	/**
	 * Add a single update to the bulk operation. For the update request, only the first matching document is updated.
	 *
	 * @param query update criteria, must not be {@literal null}.
	 * @param update {@link Update} operation to perform, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	default BulkOperations updateOne(Query query, Update update) {
		return updateOne(query, (UpdateDefinition) update);
	}

	/**
	 * Add a single update to the bulk operation. For the update request, only the first matching document is updated.
	 *
	 * @param query update criteria, must not be {@literal null}.
	 * @param update {@link Update} operation to perform, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 * @since 4.1
	 */
	BulkOperations updateOne(Query query, UpdateDefinition update);

	/**
	 * Add a list of updates to the bulk operation. For each update request, only the first matching document is updated.
	 *
	 * @param updates Update operations to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	BulkOperations updateOne(List<Pair<Query, UpdateDefinition>> updates);

	/**
	 * Add a single update to the bulk operation. For the update request, all matching documents are updated.
	 *
	 * @param query Update criteria.
	 * @param update Update operation to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	default BulkOperations updateMulti(Query query, Update update) {
		return updateMulti(query, (UpdateDefinition) update);
	}

	/**
	 * Add a single update to the bulk operation. For the update request, all matching documents are updated.
	 *
	 * @param query Update criteria.
	 * @param update Update operation to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 * @since 4.1
	 */
	BulkOperations updateMulti(Query query, UpdateDefinition update);

	/**
	 * Add a list of updates to the bulk operation. For each update request, all matching documents are updated.
	 *
	 * @param updates Update operations to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	BulkOperations updateMulti(List<Pair<Query, UpdateDefinition>> updates);

	/**
	 * Add a single upsert to the bulk operation. An upsert is an update if the set of matching documents is not empty,
	 * else an insert.
	 *
	 * @param query Update criteria.
	 * @param update Update operation to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	default BulkOperations upsert(Query query, Update update) {
		return upsert(query, (UpdateDefinition) update);
	}

	/**
	 * Add a single upsert to the bulk operation. An upsert is an update if the set of matching documents is not empty,
	 * else an insert.
	 *
	 * @param query Update criteria.
	 * @param update Update operation to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 * @since 4.1
	 */
	BulkOperations upsert(Query query, UpdateDefinition update);

	/**
	 * Add a list of upserts to the bulk operation. An upsert is an update if the set of matching documents is not empty,
	 * else an insert.
	 *
	 * @param updates Updates/insert operations to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	BulkOperations upsert(List<Pair<Query, Update>> updates);

	/**
	 * Add a single remove operation to the bulk operation.
	 *
	 * @param remove the {@link Query} to select the documents to be removed, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the removal added, will never be {@literal null}.
	 */
	BulkOperations remove(Query remove);

	/**
	 * Add a list of remove operations to the bulk operation.
	 *
	 * @param removes the remove operations to perform, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the removal added, will never be {@literal null}.
	 */
	BulkOperations remove(List<Query> removes);

	/**
	 * Add a single replace operation to the bulk operation.
	 *
	 * @param query Update criteria.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the replacement added, will never be {@literal null}.
	 * @since 2.2
	 */
	default BulkOperations replaceOne(Query query, Object replacement) {
		return replaceOne(query, replacement, FindAndReplaceOptions.empty());
	}

	/**
	 * Add a single replace operation to the bulk operation.
	 *
	 * @param query Update criteria.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the replacement added, will never be {@literal null}.
	 * @since 2.2
	 */
	BulkOperations replaceOne(Query query, Object replacement, FindAndReplaceOptions options);

	/**
	 * Execute all bulk operations using the default write concern.
	 *
	 * @return Result of the bulk operation providing counters for inserts/updates etc.
	 * @throws org.springframework.data.mongodb.BulkOperationException if an error occurred during bulk processing.
	 */
	BulkWriteResult execute();
}
