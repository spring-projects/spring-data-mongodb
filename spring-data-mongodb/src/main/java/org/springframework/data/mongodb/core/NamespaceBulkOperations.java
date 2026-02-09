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
package org.springframework.data.mongodb.core;

import java.util.List;
import java.util.function.Consumer;

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.util.Pair;

import com.mongodb.client.model.bulk.ClientBulkWriteResult;

/**
 * @author Christoph Strobl
 * @since 2026/01
 */
public interface NamespaceBulkOperations {

	<S> NamespaceAwareBulkOperations<S> inCollection(Class<S> type);

	<S> NamespaceAwareBulkOperations<S> inCollection(Class<S> type, Consumer<BulkOperationBase<S>> bulkActions);

	NamespaceAwareBulkOperations<Object> inCollection(String collection);

	<S> NamespaceAwareBulkOperations<S> inCollection(String collection, Class<S> type);

	<S> NamespaceAwareBulkOperations<S> inCollection(String collection, Class<S> type,
			Consumer<BulkOperationBase<S>> bulkActions);

	NamespaceAwareBulkOperations<Object> inCollection(String collection, Consumer<BulkOperationBase<Object>> bulkActions);

	NamespaceBulkOperations switchDatabase(String databaseName);

	ClientBulkWriteResult execute();

	interface NamespaceAwareBulkOperations<S> extends BulkOperationBase<S>, NamespaceBulkOperations {

		NamespaceAwareBulkOperations<S> insert(S document);

		@Override
		NamespaceAwareBulkOperations<S> insert(List<? extends S> documents);

		@Override
		default NamespaceAwareBulkOperations<S> updateOne(Query query, Update update) {
			return updateOne(query, (UpdateDefinition) update);
		}

		@Override
		NamespaceAwareBulkOperations<S> updateOne(Query query, UpdateDefinition update);

		@Override
		NamespaceAwareBulkOperations<S> updateOne(List<Pair<Query, UpdateDefinition>> updates);

		@Override
		default NamespaceAwareBulkOperations<S> updateMulti(Query query, Update update) {
			return updateMulti(query, (UpdateDefinition) update);
		}

		@Override
		NamespaceAwareBulkOperations<S> updateMulti(Query query, UpdateDefinition update);

		@Override
		NamespaceAwareBulkOperations<S> updateMulti(List<Pair<Query, UpdateDefinition>> updates);

		@Override
		NamespaceAwareBulkOperations<S> upsert(Query query, UpdateDefinition update);

		@Override
		NamespaceAwareBulkOperations<S> upsert(List<Pair<Query, Update>> updates);

		@Override
		NamespaceAwareBulkOperations<S> remove(Query remove);

		@Override
		NamespaceAwareBulkOperations<S> remove(List<Query> removes);

		@Override
		default NamespaceAwareBulkOperations<S> replaceOne(Query query, Object replacement) {
			return replaceOne(query, replacement, FindAndReplaceOptions.empty());
		}

		@Override
		NamespaceAwareBulkOperations<S> replaceOne(Query query, Object replacement, FindAndReplaceOptions options);

		@Override
		default NamespaceAwareBulkOperations<S> upsert(Query query, Update update) {
			upsert(query, (UpdateDefinition) update);
			return this;
		}
	}
}
