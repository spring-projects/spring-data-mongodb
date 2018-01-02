/*
 * Copyright 2016-2018 the original author or authors.
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

import java.net.UnknownHostException;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.ConnectionString;
import com.mongodb.WriteConcern;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Factory to create {@link MongoDatabase} instances from a {@link MongoClient} instance.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public class SimpleReactiveMongoDatabaseFactory implements DisposableBean, ReactiveMongoDatabaseFactory {

	private final MongoClient mongo;
	private final String databaseName;
	private final boolean mongoInstanceCreated;

	private final PersistenceExceptionTranslator exceptionTranslator;

	private @Nullable WriteConcern writeConcern;

	/**
	 * Creates a new {@link SimpleReactiveMongoDatabaseFactory} instance from the given {@link ConnectionString}.
	 *
	 * @param connectionString must not be {@literal null}.
	 * @throws UnknownHostException
	 */
	public SimpleReactiveMongoDatabaseFactory(ConnectionString connectionString) throws UnknownHostException {
		this(MongoClients.create(connectionString), connectionString.getDatabase(), true);
	}

	/**
	 * Creates a new {@link SimpleReactiveMongoDatabaseFactory} instance from the given {@link MongoClient}.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null}.
	 * @since 1.7
	 */
	public SimpleReactiveMongoDatabaseFactory(MongoClient mongoClient, String databaseName) {
		this(mongoClient, databaseName, false);
	}

	private SimpleReactiveMongoDatabaseFactory(MongoClient client, String databaseName, boolean mongoInstanceCreated) {

		Assert.notNull(client, "MongoClient must not be null!");
		Assert.hasText(databaseName, "Database name must not be empty!");
		Assert.isTrue(databaseName.matches("[\\w-]+"),
				"Database name must only contain letters, numbers, underscores and dashes!");

		this.mongo = client;
		this.databaseName = databaseName;
		this.mongoInstanceCreated = mongoInstanceCreated;
		this.exceptionTranslator = new MongoExceptionTranslator();
	}

	/**
	 * Configures the {@link WriteConcern} to be used on the {@link MongoDatabase} instance being created.
	 *
	 * @param writeConcern the writeConcern to set
	 */
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.ReactiveMongoDbFactory#getMongoDatabase()
	 */
	public MongoDatabase getMongoDatabase() throws DataAccessException {
		return getMongoDatabase(databaseName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.ReactiveMongoDbFactory#getMongoDatabase(java.lang.String)
	 */
	public MongoDatabase getMongoDatabase(String dbName) throws DataAccessException {

		Assert.hasText(dbName, "Database name must not be empty.");

		MongoDatabase db = mongo.getDatabase(dbName);
		return writeConcern != null ? db.withWriteConcern(writeConcern) : db;
	}

	/**
	 * Clean up the Mongo instance if it was created by the factory itself.
	 *
	 * @see DisposableBean#destroy()
	 */
	public void destroy() throws Exception {

		if (mongoInstanceCreated) {
			mongo.close();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.ReactiveMongoDbFactory#getExceptionTranslator()
	 */
	public PersistenceExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}
}
