/*
 * Copyright 2010-2011 the original author or authors.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.CannotGetMongoDbConnectionException;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * Helper class featuring helper methods for internal MongoDb classes.
 * <p/>
 * <p>
 * Mainly intended for internal use within the framework.
 * 
 * @author Thomas Risberg
 * @author Graeme Rocher
 * @author Oliver Gierke
 * @since 1.0
 */
public abstract class MongoDbUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbUtils.class);

	/**
	 * Private constructor to prevent instantiation.
	 */
	private MongoDbUtils() {

	}

	/**
	 * Obtains a {@link DB} connection for the given {@link Mongo} instance and database name
	 * 
	 * @param mongo the {@link Mongo} instance, must not be {@literal null}.
	 * @param databaseName the database name, must not be {@literal null} or empty.
	 * @return the {@link DB} connection
	 */
	public static DB getDB(Mongo mongo, String databaseName) {
		return doGetDB(mongo, databaseName, UserCredentials.NO_CREDENTIALS, true);
	}

	/**
	 * Obtains a {@link DB} connection for the given {@link Mongo} instance and database name
	 * 
	 * @param mongo the {@link Mongo} instance, must not be {@literal null}.
	 * @param databaseName the database name, must not be {@literal null} or empty.
	 * @param credentials the credentials to use, must not be {@literal null}.
	 * @return the {@link DB} connection
	 */
	public static DB getDB(Mongo mongo, String databaseName, UserCredentials credentials) {

		Assert.notNull(mongo, "No Mongo instance specified!");
		Assert.hasText(databaseName, "Database name must be given!");
		Assert.notNull(credentials, "Credentials must not be null, use UserCredentials.NO_CREDENTIALS!");

		return doGetDB(mongo, databaseName, credentials, true);
	}

	private static DB doGetDB(Mongo mongo, String databaseName, UserCredentials credentials, boolean allowCreate) {

		DbHolder dbHolder = (DbHolder) TransactionSynchronizationManager.getResource(mongo);

		// Do we have a populated holder and TX sync active?
		if (dbHolder != null && !dbHolder.isEmpty() && TransactionSynchronizationManager.isSynchronizationActive()) {

			DB db = dbHolder.getDB(databaseName);

			// DB found but not yet synchronized
			if (db != null && !dbHolder.isSynchronizedWithTransaction()) {

				LOGGER.debug("Registering Spring transaction synchronization for existing MongoDB {}.", databaseName);

				TransactionSynchronizationManager.registerSynchronization(new MongoSynchronization(dbHolder, mongo));
				dbHolder.setSynchronizedWithTransaction(true);
			}

			if (db != null) {
				return db;
			}
		}

		// Lookup fresh database instance
		LOGGER.debug("Getting Mongo Database name=[{}]", databaseName);

		DB db = mongo.getDB(databaseName);
		boolean credentialsGiven = credentials.hasUsername() && credentials.hasPassword();

		if (credentialsGiven && !db.isAuthenticated()) {

			String username = credentials.getUsername();
			String password = credentials.hasPassword() ? credentials.getPassword() : null;

			synchronized (db) {
				if (!db.authenticate(username, password == null ? null : password.toCharArray())) {
					throw new CannotGetMongoDbConnectionException("Failed to authenticate to database [" + databaseName
							+ "], username = [" + username + "], password = [" + password + "]", databaseName, credentials);
				}
			}
		}

		// TX sync active, bind new database to thread
		if (TransactionSynchronizationManager.isSynchronizationActive()) {

			LOGGER.debug("Registering Spring transaction synchronization for MongoDB instance {}.", databaseName);

			DbHolder holderToUse = dbHolder;

			if (holderToUse == null) {
				holderToUse = new DbHolder(databaseName, db);
			} else {
				holderToUse.addDB(databaseName, db);
			}

			TransactionSynchronizationManager.registerSynchronization(new MongoSynchronization(holderToUse, mongo));
			holderToUse.setSynchronizedWithTransaction(true);

			if (holderToUse != dbHolder) {
				TransactionSynchronizationManager.bindResource(mongo, holderToUse);
			}
		}

		// Check whether we are allowed to return the DB.
		if (!allowCreate && !isDBTransactional(db, mongo)) {
			throw new IllegalStateException("No Mongo DB bound to thread, "
					+ "and configuration does not allow creation of non-transactional one here");
		}

		return db;
	}

	/**
	 * Return whether the given DB instance is transactional, that is, bound to the current thread by Spring's transaction
	 * facilities.
	 * 
	 * @param db the DB to check
	 * @param mongo the Mongo instance that the DB was created with (may be <code>null</code>)
	 * @return whether the DB is transactional
	 */
	public static boolean isDBTransactional(DB db, Mongo mongo) {

		if (mongo == null) {
			return false;
		}
		DbHolder dbHolder = (DbHolder) TransactionSynchronizationManager.getResource(mongo);
		return dbHolder != null && dbHolder.containsDB(db);
	}

	/**
	 * Perform actual closing of the Mongo DB object, catching and logging any cleanup exceptions thrown.
	 * 
	 * @param db the DB to close (may be <code>null</code>)
	 */
	public static void closeDB(DB db) {

		if (db != null) {
			LOGGER.debug("Closing Mongo DB object");
			try {
				db.requestDone();
			} catch (Throwable ex) {
				LOGGER.debug("Unexpected exception on closing Mongo DB object", ex);
			}
		}
	}
}
