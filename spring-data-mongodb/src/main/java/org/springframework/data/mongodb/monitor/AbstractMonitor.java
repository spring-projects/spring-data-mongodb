/*
 * Copyright 2002-2018 the original author or authors.
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
package org.springframework.data.mongodb.monitor;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * Base class to encapsulate common configuration settings when connecting to a database
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public abstract class AbstractMonitor {

	private final MongoClient mongoClient;

	protected AbstractMonitor(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	public Document getServerStatus() {
		return getDb("admin").runCommand(new Document("serverStatus", 1).append("rangeDeleter", 1).append("repl", 1));
	}

	public MongoDatabase getDb(String databaseName) {
		return mongoClient.getDatabase(databaseName);
	}

	protected MongoClient getMongoClient() {
		return mongoClient;
	}
}
