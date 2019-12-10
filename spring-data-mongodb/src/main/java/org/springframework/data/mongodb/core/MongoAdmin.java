/*
 * Copyright 2011-2019 the original author or authors.
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

import org.bson.Document;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.util.Assert;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * Mongo server administration exposed via JMX annotations
 *
 * @author Mark Pollack
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ManagedResource(description = "Mongo Admin Operations")
public class MongoAdmin implements MongoAdminOperations {

	private final MongoClient mongoClient;

	/**
	 * @param client the underlying {@link com.mongodb.client.MongoClient} used for data access.
	 * @since 2.2
	 */
	public MongoAdmin(MongoClient client) {

		Assert.notNull(client, "Client must not be null!");
		this.mongoClient = client;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.core.MongoAdminOperations#dropDatabase(java.lang.String)
	 */
	@ManagedOperation
	public void dropDatabase(String databaseName) {
		getDB(databaseName).drop();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.core.MongoAdminOperations#createDatabase(java.lang.String)
	 */
	@ManagedOperation
	public void createDatabase(String databaseName) {
		getDB(databaseName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.core.MongoAdminOperations#getDatabaseStats(java.lang.String)
	 */
	@ManagedOperation
	public String getDatabaseStats(String databaseName) {
		return getDB(databaseName).runCommand(new Document("dbStats", 1).append("scale", 1024)).toJson();
	}

	@ManagedOperation
	public String getServerStatus() {
		return getDB("admin").runCommand(new Document("serverStatus", 1).append("rangeDeleter", 1).append("repl", 1))
				.toJson();
	}

	MongoDatabase getDB(String databaseName) {
		return mongoClient.getDatabase(databaseName);
	}
}
