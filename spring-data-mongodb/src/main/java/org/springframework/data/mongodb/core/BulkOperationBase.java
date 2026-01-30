/*
 * Copyright 2026. the original author or authors.
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

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.util.Pair;

/**
 * @author Christoph Strobl
 * @since 2026/01
 */
public interface BulkOperationBase {

	/**
	 * Add a single insert to the bulk operation.
	 *
	 * @param documents the document to insert, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the insert added, will never be {@literal null}.
	 */
	BulkOperationBase insert(Object documents);

	/**
	 * Add a list of inserts to the bulk operation.
	 *
	 * @param documents List of documents to insert, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the insert added, will never be {@literal null}.
	 */
	BulkOperationBase insert(List<? extends Object> documents);

	/**
	 * Add a single update to the bulk operation. For the update request, only the first matching document is updated.
	 *
	 * @param query update criteria, must not be {@literal null}. The {@link Query} may define a {@link Query#with(Sort)
	 *          sort order} to influence which document to update when potentially matching multiple candidates.
	 * @param update {@link Update} operation to perform, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	default BulkOperationBase updateOne(Query query, Update update) {
		return updateOne(query, (UpdateDefinition) update);
	}

	/**
	 * Add a single update to the bulk operation. For the update request, only the first matching document is updated.
	 *
	 * @param query update criteria, must not be {@literal null}. The {@link Query} may define a {@link Query#with(Sort)
	 *          sort order} to influence which document to update when potentially matching multiple candidates.
	 * @param update {@link Update} operation to perform, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 * @since 4.1
	 */
	BulkOperationBase updateOne(Query query, UpdateDefinition update);

	/**
	 * Add a list of updates to the bulk operation. For each update request, only the first matching document is updated.
	 *
	 * @param updates Update operations to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	BulkOperationBase updateOne(List<Pair<Query, UpdateDefinition>> updates);

	/**
	 * Add a single update to the bulk operation. For the update request, all matching documents are updated.
	 *
	 * @param query Update criteria.
	 * @param update Update operation to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	default BulkOperationBase updateMulti(Query query, Update update) {
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
	BulkOperationBase updateMulti(Query query, UpdateDefinition update);

	/**
	 * Add a list of updates to the bulk operation. For each update request, all matching documents are updated.
	 *
	 * @param updates Update operations to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	BulkOperationBase updateMulti(List<Pair<Query, UpdateDefinition>> updates);

	/**
	 * Add a single upsert to the bulk operation. An upsert is an update if the set of matching documents is not empty,
	 * else an insert.
	 *
	 * @param query Update criteria.
	 * @param update Update operation to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	default BulkOperationBase upsert(Query query, Update update) {
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
	BulkOperationBase upsert(Query query, UpdateDefinition update);

	/**
	 * Add a list of upserts to the bulk operation. An upsert is an update if the set of matching documents is not empty,
	 * else an insert.
	 *
	 * @param updates Updates/insert operations to perform.
	 * @return the current {@link BulkOperations} instance with the update added, will never be {@literal null}.
	 */
	BulkOperationBase upsert(List<Pair<Query, Update>> updates);

	/**
	 * Add a single remove operation to the bulk operation.
	 *
	 * @param remove the {@link Query} to select the documents to be removed, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the removal added, will never be {@literal null}.
	 */
	BulkOperationBase remove(Query remove);

	/**
	 * Add a list of remove operations to the bulk operation.
	 *
	 * @param removes the remove operations to perform, must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the removal added, will never be {@literal null}.
	 */
	BulkOperationBase remove(List<Query> removes);

	/**
	 * Add a single replace operation to the bulk operation.
	 *
	 * @param query Replace criteria. The {@link Query} may define a {@link Query#with(Sort) sort order} to influence
	 *          which document to replace when potentially matching multiple candidates.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the replacement added, will never be {@literal null}.
	 * @since 2.2
	 */
	default BulkOperationBase replaceOne(Query query, Object replacement) {
		return replaceOne(query, replacement, FindAndReplaceOptions.empty());
	}

	/**
	 * Add a single replace operation to the bulk operation.
	 *
	 * @param query Replace criteria. The {@link Query} may define a {@link Query#with(Sort) sort order} to influence
	 *          which document to replace when potentially matching multiple candidates.
	 * @param replacement the replacement document. Must not be {@literal null}.
	 * @param options the {@link FindAndModifyOptions} holding additional information. Must not be {@literal null}.
	 * @return the current {@link BulkOperations} instance with the replacement added, will never be {@literal null}.
	 * @since 2.2
	 */
	BulkOperationBase replaceOne(Query query, Object replacement, FindAndReplaceOptions options);

}
