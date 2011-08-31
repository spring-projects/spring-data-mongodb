/*
 * Copyright 2011 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.util.Assert;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

/**
 * Factory to create {@link DB} instances from a {@link Mongo} instance.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class SimpleMongoDbFactory implements DisposableBean, MongoDbFactory {

	protected final Log logger = LogFactory.getLog(getClass());

	private final Mongo mongo;
	private final String databaseName;
	private String username;
	private String password;
	private WriteConcern writeConcern;

	/**
	 * Create an instance of {@link SimpleMongoDbFactory} given the {@link Mongo} instance and database name.
	 * 
	 * @param mongo Mongo instance, must not be {@literal null}.
	 * @param databaseName database name, not be {@literal null}.
	 */
	public SimpleMongoDbFactory(Mongo mongo, String databaseName) {
		Assert.notNull(mongo, "Mongo must not be null");
		Assert.hasText(databaseName, "Database name must not be empty");
		Assert.isTrue(databaseName.matches("[\\w-]+"), "Database name must only contain letters, numbers, underscores and dashes!");
		this.mongo = mongo;
		this.databaseName = databaseName;
	}

	/**
	 * Create an instance of SimpleMongoDbFactory given the Mongo instance, database name, and username/password
	 * 
	 * @param mongo Mongo instance, must not be {@literal null}.
	 * @param databaseName Database name, must not be {@literal null}.
	 * @param userCredentials username and password must not be {@literal null}.
	 */
	public SimpleMongoDbFactory(Mongo mongo, String databaseName, UserCredentials userCredentials) {
		this(mongo, databaseName);
		this.username = userCredentials.getUsername();
		this.password = userCredentials.getPassword();
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
	public DB getDb() throws DataAccessException {
		return getDb(databaseName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getDb(java.lang.String)
	 */
	public DB getDb(String dbName) throws DataAccessException {

		Assert.hasText(dbName, "Database name must not be empty.");

		DB db = MongoDbUtils.getDB(mongo, dbName, username, password == null ? null : password.toCharArray());

		if (writeConcern != null) {
			db.setWriteConcern(writeConcern);
		}

		return db;
	}

	/**
	 * Clean up the Mongo instance.
	 */
	public void destroy() throws Exception {
		mongo.close();
	}
}
