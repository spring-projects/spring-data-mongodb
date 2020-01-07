/*
 * Copyright 2002-2020 the original author or authors.
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
package org.springframework.data.mongodb.monitor;

import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ServerDescription;

/**
 * Base class to encapsulate common configuration settings when connecting to a database
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public abstract class AbstractMonitor {

	private final Object mongoClient;

	/**
	 * @param mongoClient
	 * @deprecated since 2.2 in favor of {@link #AbstractMonitor(com.mongodb.client.MongoClient)}
	 */
	@Deprecated
	protected AbstractMonitor(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	/**
	 * @param mongoClient
	 * @since 2.2
	 */
	protected AbstractMonitor(com.mongodb.client.MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	public Document getServerStatus() {
		return getDb("admin").runCommand(new Document("serverStatus", 1).append("rangeDeleter", 1).append("repl", 1));
	}

	public MongoDatabase getDb(String databaseName) {

		if (mongoClient instanceof MongoClient) {
			return ((MongoClient) mongoClient).getDatabase(databaseName);
		}

		return ((com.mongodb.client.MongoClient) mongoClient).getDatabase(databaseName);
	}

	protected MongoClient getMongoClient() {

		if (mongoClient instanceof MongoClient) {
			return (MongoClient) mongoClient;
		}

		throw new IllegalStateException("A com.mongodb.MongoClient is required but was com.mongodb.client.MongoClient");
	}

	protected List<ServerAddress> hosts() {

		if (mongoClient instanceof MongoClient) {
			return ((MongoClient) mongoClient).getServerAddressList();
		}

		return ((com.mongodb.client.MongoClient) mongoClient).getClusterDescription().getServerDescriptions().stream()
				.map(ServerDescription::getAddress).collect(Collectors.toList());
	}
}
