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

import com.mongodb.client.model.bulk.ClientBulkWriteResult;

/**
 * Result of a {@link Bulk} write execution. Exposes counts for inserted, modified, deleted, and upserted documents, and
 * whether the operation was acknowledged by the server.
 *
 * @param <T> the type of the raw driver result (e.g. {@link ClientBulkWriteResult}).
 * @author Christoph Strobl
 * @since 5.1
 */
public interface BulkOperationResult<T> {

	/**
	 * Creates a {@link BulkOperationResult} from a MongoDB driver {@link ClientBulkWriteResult}.
	 *
	 * @param result the driver result; must not be {@literal null}.
	 * @return a new {@link BulkOperationResult} wrapping the given result; never {@literal null}.
	 */
	static BulkOperationResult<ClientBulkWriteResult> from(ClientBulkWriteResult result) {

		return new BulkOperationResult<>() {
			@Override
			public long insertCount() {
				return result.getInsertedCount();
			}

			@Override
			public long modifiedCount() {
				return result.getModifiedCount();
			}

			@Override
			public long deleteCount() {
				return result.getDeletedCount();
			}

			@Override
			public long upsertCount() {
				return result.getUpsertedCount();
			}

			@Override
			public boolean acknowledged() {
				return result.isAcknowledged();
			}

			@Override
			public long matchedCount() {
				return result.getMatchedCount();
			}

			@Override
			public ClientBulkWriteResult rawResult() {
				return result;
			}
		};
	}

	/**
	 * Returns the number of documents inserted.
	 *
	 * @return the insert count.
	 */
	long insertCount();

	/**
	 * Returns the number of documents modified by update operations.
	 *
	 * @return the modified count.
	 */
	long modifiedCount();

	/**
	 * Returns the number of documents deleted.
	 *
	 * @return the delete count.
	 */
	long deleteCount();

	/**
	 * Returns the number of documents upserted.
	 *
	 * @return the upsert count.
	 */
	long upsertCount();

	/**
	 * Returns whether the bulk write was acknowledged by the server.
	 *
	 * @return {@literal true} if acknowledged.
	 */
	boolean acknowledged();

	/**
	 * Returns the number of documents that matched the query criteria in update, replace, or remove operations.
	 *
	 * @return the matched count.
	 */
	long matchedCount();

	/**
	 * Returns the raw result from the MongoDB driver.
	 *
	 * @return the raw result.
	 */
	T rawResult();

}
