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
 * whether the operation was acknowledged by the server. Abstracts over both single-collection
 * ({@link com.mongodb.bulk.BulkWriteResult}) and multi-collection ({@link ClientBulkWriteResult}) driver results.
 *
 * @author Christoph Strobl
 * @since 5.1
 */
public interface BulkWriteResult {

	/**
	 * Creates a {@link BulkWriteResult} from a MongoDB driver {@link ClientBulkWriteResult}.
	 *
	 * @param result the driver result; must not be {@literal null}.
	 * @return a new {@link BulkWriteResult} wrapping the given result; never {@literal null}.
	 */
	static BulkWriteResult from(ClientBulkWriteResult result) {

		return new BulkWriteResult() {
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
		};
	}

	/**
	 * Creates a {@link BulkWriteResult} from a MongoDB driver {@link com.mongodb.bulk.BulkWriteResult}.
	 *
	 * @param result the driver result; must not be {@literal null}.
	 * @return a new {@link BulkWriteResult} wrapping the given result; never {@literal null}.
	 */
	static BulkWriteResult from(com.mongodb.bulk.BulkWriteResult result) {
		return new BulkWriteResult() {
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
				return result.getUpserts().size();
			}

			@Override
			public boolean acknowledged() {
				return result.wasAcknowledged();
			}

			@Override
			public long matchedCount() {
				return result.getMatchedCount();
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

}
