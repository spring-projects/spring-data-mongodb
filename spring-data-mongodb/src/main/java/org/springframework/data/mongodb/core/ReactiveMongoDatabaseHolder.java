/*
 * Copyright 2016 the original author or authors.
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


import com.mongodb.reactivestreams.client.MongoDatabase;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.transaction.support.ResourceHolderSupport;
import org.springframework.util.Assert;


/**
 * @author Mark Paluch
 */
class ReactiveMongoDatabaseHolder extends ResourceHolderSupport {
	private static final Object DEFAULT_KEY = new Object();

	private final Map<Object, MongoDatabase> dbMap = new ConcurrentHashMap<Object, MongoDatabase>();

	public ReactiveMongoDatabaseHolder(MongoDatabase db) {
		addMongoDatabase(db);
	}

	public ReactiveMongoDatabaseHolder(Object key, MongoDatabase db) {
		addMongoDatabase(key, db);
	}

	public MongoDatabase getMongoDatabase() {
		return getMongoDatabase(DEFAULT_KEY);
	}

	public MongoDatabase getMongoDatabase(Object key) {
		return this.dbMap.get(key);
	}

	public MongoDatabase getAnyMongoDatabase() {
		if (!this.dbMap.isEmpty()) {
			return this.dbMap.values().iterator().next();
		}
		return null;
	}

	public void addMongoDatabase(MongoDatabase session) {
		addMongoDatabase(DEFAULT_KEY, session);
	}

	public void addMongoDatabase(Object key, MongoDatabase session) {
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(session, "DB must not be null");
		this.dbMap.put(key, session);
	}

	public MongoDatabase removeMongoDatabase(Object key) {
		return this.dbMap.remove(key);
	}

	public boolean containsMongoDatabase(MongoDatabase session) {
		return this.dbMap.containsValue(session);
	}

	public boolean isEmpty() {
		return this.dbMap.isEmpty();
	}

	public boolean doesNotHoldNonDefaultMongoDatabase() {
		synchronized (this.dbMap) {
			return this.dbMap.isEmpty() || (this.dbMap.size() == 1 && this.dbMap.containsKey(DEFAULT_KEY));
		}
	}

}
