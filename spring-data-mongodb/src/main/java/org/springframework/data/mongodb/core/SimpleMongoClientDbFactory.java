/*
 * Copyright 2018-2022 the original author or authors.
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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

/**
 * Factory to create {@link MongoDatabase} instances from a {@link MongoClient} instance.
 *
 * @author Christoph Strobl
 * @since 2.1
 * @deprecated since 3.0, use {@link SimpleMongoClientDatabaseFactory} instead.
 */
@Deprecated
public class SimpleMongoClientDbFactory extends SimpleMongoClientDatabaseFactory {

	/**
	 * Creates a new {@link SimpleMongoClientDbFactory} instance for the given {@code connectionString}.
	 *
	 * @param connectionString connection coordinates for a database connection. Must contain a database name and must not
	 *          be {@literal null} or empty.
	 * @see <a href="https://docs.mongodb.com/manual/reference/connection-string/">MongoDB Connection String reference</a>
	 */
	public SimpleMongoClientDbFactory(String connectionString) {
		this(new ConnectionString(connectionString));
	}

	/**
	 * Creates a new {@link SimpleMongoClientDbFactory} instance from the given {@link MongoClient}.
	 *
	 * @param connectionString connection coordinates for a database connection. Must contain also a database name and not
	 *          be {@literal null}.
	 */
	public SimpleMongoClientDbFactory(ConnectionString connectionString) {
		this(MongoClients.create(connectionString), connectionString.getDatabase(), true);
	}

	/**
	 * Creates a new {@link SimpleMongoClientDbFactory} instance from the given {@link MongoClient}.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 */
	public SimpleMongoClientDbFactory(MongoClient mongoClient, String databaseName) {
		this(mongoClient, databaseName, false);
	}

	/**
	 * Creates a new {@link SimpleMongoClientDbFactory} instance from the given {@link MongoClient}.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 * @param mongoInstanceCreated
	 */
	private SimpleMongoClientDbFactory(MongoClient mongoClient, String databaseName, boolean mongoInstanceCreated) {
		super(mongoClient, databaseName, mongoInstanceCreated);
	}
}
