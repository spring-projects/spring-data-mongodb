/*
 * Copyright 2010-2025 the original author or authors.
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

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;

/**
 * Internal helper for MongoDB-backed atomic counters using findAndModify.
 * <p>
 * <strong>This class is for internal use only and subject to change.</strong>
 * It is not part of the public API and may be refactored in future releases.
 * <p>
 * This class provides support for server-side counter management using MongoDB's
 * atomic findAndModify operation with $inc, upsert, and returnNew options.
 * Counter values start at 1 on first access.
 *
 * @author Jeongkyun An
 * @since 4.5
 * @see <a href="https://github.com/spring-projects/spring-data-mongodb/issues/4823">GH-4823</a>
 */
class MongoCounterSupport {

	private final MongoOperations mongoOperations;

	/**
	 * Creates a new {@link MongoCounterSupport} instance.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 */
	MongoCounterSupport(MongoOperations mongoOperations) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null");
		this.mongoOperations = mongoOperations;
	}

	/**
	 * Get the next counter value atomically. Counter starts at 1 on first call.
	 *
	 * @param counterName the name of the counter, must not be {@literal null}.
	 * @param collectionName the collection to store counter documents, must not be {@literal null}.
	 * @return the next counter value.
	 */
	long getNextSequenceValue(String counterName, String collectionName) {

		Assert.notNull(counterName, "Counter name must not be null");
		Assert.notNull(collectionName, "Collection name must not be null");

		Update update = new Update().inc("count", 1L);
		FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true).upsert(true);

		CounterDocument result = mongoOperations.findAndModify(query(where("_id").is(counterName)), update, options,
				CounterDocument.class, collectionName);

		return result != null ? result.getCount() : 1L;
	}

	/**
	 * Internal document structure for counter storage.
	 * <p>
	 * Document format: {@code {_id: "counterName", count: 123}}
	 */
	static class CounterDocument {

		@Id
		private String id;

		private Long count;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Long getCount() {
			return count;
		}

		public void setCount(Long count) {
			this.count = count;
		}
	}
}
