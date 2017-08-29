/*
 * Copyright 2011-2017 the original author or authors.
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

import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.CodecRegistryProvider;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;

/**
 * Factory to create {@link DB} instances from a {@link MongoClient} instance.
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class SimpleMongoDbFactory implements DisposableBean, MongoDbFactory {

	private final MongoClient mongoClient;
	private final String databaseName;
	private final boolean mongoInstanceCreated;
	private final PersistenceExceptionTranslator exceptionTranslator;

	private @Nullable WriteConcern writeConcern;

	/**
	 * Creates a new {@link SimpleMongoDbFactory} instance from the given {@link MongoClientURI}.
	 *
	 * @param uri must not be {@literal null}.
	 * @throws UnknownHostException
	 * @since 1.7
	 */
	public SimpleMongoDbFactory(MongoClientURI uri) {
		this(new MongoClient(uri), uri.getDatabase(), true);
	}

	/**
	 * Creates a new {@link SimpleMongoDbFactory} instance from the given {@link MongoClient}.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null}.
	 * @since 1.7
	 */
	public SimpleMongoDbFactory(MongoClient mongoClient, String databaseName) {
		this(mongoClient, databaseName, false);
	}

	/**
	 * @param client
	 * @param databaseName
	 * @param mongoInstanceCreated
	 * @since 1.7
	 */
	private SimpleMongoDbFactory(MongoClient mongoClient, String databaseName, boolean mongoInstanceCreated) {

		Assert.notNull(mongoClient, "MongoClient must not be null!");
		Assert.hasText(databaseName, "Database name must not be empty!");
		Assert.isTrue(databaseName.matches("[\\w-]+"),
				"Database name must only contain letters, numbers, underscores and dashes!");

		this.mongoClient = mongoClient;
		this.databaseName = databaseName;
		this.mongoInstanceCreated = mongoInstanceCreated;
		this.exceptionTranslator = new MongoExceptionTranslator();
	}

	/**
	 * Configures the {@link WriteConcern} to be used on the {@link DB} instance being created.
	 *
	 * @param writeConcern the writeConcern to set
	 */
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getDb()
	 */
	public MongoDatabase getDb() throws DataAccessException {
		return getDb(databaseName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getDb(java.lang.String)
	 */
	public MongoDatabase getDb(String dbName) throws DataAccessException {

		Assert.hasText(dbName, "Database name must not be empty.");

		MongoDatabase db = mongoClient.getDatabase(dbName);

		if (writeConcern == null) {
			return db;
		}

		return db.withWriteConcern(writeConcern);
	}

	/**
	 * Clean up the Mongo instance if it was created by the factory itself.
	 *
	 * @see DisposableBean#destroy()
	 */
	public void destroy() throws Exception {
		if (mongoInstanceCreated) {
			mongoClient.close();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getExceptionTranslator()
	 */
	@Override
	public PersistenceExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	@SuppressWarnings("deprecation")
	@Override
	public DB getLegacyDb() {
		return mongoClient.getDB(databaseName);
	}

	@Override
	public CodecRegistry getCodecRegistry() {
		return mongoClient.getMongoClientOptions().getCodecRegistry();
	}
}
