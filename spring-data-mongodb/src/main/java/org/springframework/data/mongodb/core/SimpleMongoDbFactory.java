/*
 * Copyright 2011-2018 the original author or authors.
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

import org.springframework.beans.factory.DisposableBean;

import com.mongodb.ClientSessionOptions;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Factory to create {@link MongoDatabase} instances from a {@link MongoClient} instance.
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author George Moraitis
 * @author Mark Paluch
 */
public class SimpleMongoDbFactory extends MongoDbFactorySupport<MongoClient> implements DisposableBean {

	/**
	 * Creates a new {@link SimpleMongoDbFactory} instance from the given {@link MongoClientURI}.
	 *
	 * @param uri coordinates for a database connection. Must contain a database name and must not be {@literal null}.
	 * @since 1.7
	 */
	public SimpleMongoDbFactory(MongoClientURI uri) {
		this(new MongoClient(uri), uri.getDatabase(), true);
	}

	/**
	 * Creates a new {@link SimpleMongoDbFactory} instance from the given {@link MongoClient}.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 * @since 1.7
	 */
	public SimpleMongoDbFactory(MongoClient mongoClient, String databaseName) {
		this(mongoClient, databaseName, false);
	}

	/**
	 * @param mongoClient
	 * @param databaseName
	 * @param mongoInstanceCreated
	 * @since 1.7
	 */
	private SimpleMongoDbFactory(MongoClient mongoClient, String databaseName, boolean mongoInstanceCreated) {
		super(mongoClient, databaseName, mongoInstanceCreated, new MongoExceptionTranslator());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getLegacyDb()
	 */
	@Override
	public DB getLegacyDb() {
		return getMongoClient().getDB(getDefaultDatabaseName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getSession(com.mongodb.ClientSessionOptions)
	 */
	@Override
	public ClientSession getSession(ClientSessionOptions options) {
		return getMongoClient().startSession(options);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoDbFactoryBase#closeClient()
	 */
	@Override
	protected void closeClient() {
		getMongoClient().close();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.MongoDbFactoryBase#doGetMongoDatabase(java.lang.String)
	 */
	@Override
	protected MongoDatabase doGetMongoDatabase(String dbName) {
		return getMongoClient().getDatabase(dbName);
	}
}
