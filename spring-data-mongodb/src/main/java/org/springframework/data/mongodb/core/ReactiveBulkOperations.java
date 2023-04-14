/*
 * Copyright 2023 the original author or authors.
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

import reactor.core.publisher.Mono;

import java.util.List;

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;

import com.mongodb.bulk.BulkWriteResult;

/**
 * Bulk operations for insert/update/remove actions on a collection. Bulk operations are available since MongoDB 2.6 and
 * make use of low level bulk commands on the protocol level. This interface defines a fluent API to add multiple single
 * operations or list of similar operations in sequence which can then eventually be executed by calling
 * {@link #execute()}.
 *
 * <pre class="code">
 * ReactiveMongoOperations ops = …;
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
 * @author Christoph Strobl
 * @since 4.1
 */
public interface ReactiveBulkOperations {

	/**
	 * Add a single insert to the bulk operation.
	 *
	 * @param documents the document to insert, must not be {@literal null}.
	 * @return the current {@link ReactiveBulkOperations} instance with the insert added, will never be {@literal null}.
	 */
	ReactiveBulkOperations insert(Object documents);

	/**
	 * Add a list of inserts to the bulk operation.
	 *
	 * @param documents List of documents to insert, must not be {@literal null}.
	 * @return the current {@link ReactiveBulkOperations} instance with the insert added, will never be {@literal null}.
	 */
	ReactiveBulkOperations insert(List<? extends Object> documents);

	/**
	 * Add a single update to the bulk operation. For the update request, only the first matching document is updated.
	 *
	 * @param query update criteria, must not be {@literal null}.
	 * @param update {@link UpdateDefinition} operation to perform, must not be {@literal null}.
	 * @return the current {@link ReactiveBulkOperations} instance with the update added, will never be {@literal null}.
	 */
	ReactiveBulkOperations updateOne(Query query, UpdateDefinition update);

	/**
	 * Add a single update to the bulk operation. For the update request, all matching documents are updated.
	 *
	 * @param query Update criteria.
	 * @param update Update operation to perform.
	 * @return the current {@link ReactiveBulkOperations} instance with the update added, will never be {@literal null}.
	 */
	ReactiveBulkOperations updateMulti(Query query, UpdateDefinition update);

	/**
	 * Add a single upsert to the bulk operation. An upsert is an update if the set of matching documents is not empty,
	 * else an insert.
	 *
	 * @param query Update criteria.
	 * @param update Update operation to perform.
	 * @return the current {@link ReactiveBulkOperations} instance with the update added, will never be {@literal null}.
	 */
	ReactiveBulkOperations upsert(Query query, UpdateDefinition update);

	/**
	 * Add a single remove operation to the bulk operation.
	 *
	 * @param remove the {@link Query} to select the documents to be removed, must not be {@literal null}.
	 * @return the current {@link ReactiveBulkOperations} instance with the removal added, will never be {@literal null}.
	 */
	ReactiveBulkOperations remove(Query remove);

	/**
	 * Add a list of remove operations to the bulk operation.
	 *
	 * @param removes the remove operations to perform, must not be {@literal null}.
	 * @return the current {@link ReactiveBulkOperations} instance with the removal added, will never be {@literal null}.
	 */
	ReactiveBulkOperations remove(List<Query> removes);

	/**
	 * Add a single replace operation to the bulk operation.
	 *
	 * @param query Update criteria.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @return the current {@link ReactiveBulkOperations} instance with the replace added, will never be {@literal null}.
	 */
	default ReactiveBulkOperations replaceOne(Query query, Object replacement) {
		return replaceOne(query, replacement, FindAndReplaceOptions.empty());
	}

	/**
	 * Add a single replace operation to the bulk operation.
	 *
	 * @param query Update criteria.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @return the current {@link ReactiveBulkOperations} instance with the replace added, will never be {@literal null}.
	 */
	ReactiveBulkOperations replaceOne(Query query, Object replacement, FindAndReplaceOptions options);

	/**
	 * Execute all bulk operations using the default write concern.
	 *
	 * @return a {@link Mono} emitting the result of the bulk operation providing counters for inserts/updates etc.
	 */
	Mono<BulkWriteResult> execute();
}
