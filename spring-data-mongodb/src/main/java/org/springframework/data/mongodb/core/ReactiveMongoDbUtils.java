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

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.util.MongoClientVersion;
import org.springframework.transaction.support.TransactionSynchronizationManager;


/**
 * Helper class featuring helper methods for internal MongoDb classes. Mainly intended for internal use within the
 * framework.
 * 
 * @author Mark Paluch
 */
public abstract class ReactiveMongoDbUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveMongoDbUtils.class);

	/**
	 * Private constructor to prevent instantiation.
	 */
	private ReactiveMongoDbUtils() {}

	/**
	 * Obtains a {@link MongoDatabase} connection for the given {@link MongoClient} instance and database name
	 * 
	 * @param mongo the {@link MongoClient} instance, must not be {@literal null}.
	 * @param databaseName the database name, must not be {@literal null} or empty.
	 * @return the {@link MongoDatabase} connection
	 */
	public static MongoDatabase getMongoDatabase(MongoClient mongo, String databaseName) {
		return doGetMongoDatabase(mongo, databaseName, UserCredentials.NO_CREDENTIALS, true, databaseName);
	}

	private static MongoDatabase doGetMongoDatabase(MongoClient mongo, String databaseName, UserCredentials credentials,
			boolean allowCreate, String authenticationDatabaseName) {

		ReactiveMongoDatabaseHolder dbHolder = (ReactiveMongoDatabaseHolder) TransactionSynchronizationManager
				.getResource(mongo);

		// Do we have a populated holder and TX sync active?
		if (dbHolder != null && !dbHolder.isEmpty() && TransactionSynchronizationManager.isSynchronizationActive()) {

			MongoDatabase db = dbHolder.getMongoDatabase(databaseName);

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

		MongoDatabase db = mongo.getDatabase(databaseName);

		// TX sync active, bind new database to thread
		if (TransactionSynchronizationManager.isSynchronizationActive()) {

			LOGGER.debug("Registering Spring transaction synchronization for MongoDB instance {}.", databaseName);

			ReactiveMongoDatabaseHolder holderToUse = dbHolder;

			if (holderToUse == null) {
				holderToUse = new ReactiveMongoDatabaseHolder(databaseName, db);
			} else {
				holderToUse.addMongoDatabase(databaseName, db);
			}

			// synchronize holder only if not yet synchronized
			if (!holderToUse.isSynchronizedWithTransaction()) {
				TransactionSynchronizationManager.registerSynchronization(new MongoSynchronization(holderToUse, mongo));
				holderToUse.setSynchronizedWithTransaction(true);
			}

			if (holderToUse != dbHolder) {
				TransactionSynchronizationManager.bindResource(mongo, holderToUse);
			}
		}

		// Check whether we are allowed to return the DB.
		if (!allowCreate && !isDBTransactional(db, mongo)) {
			throw new IllegalStateException(
					"No Mongo DB bound to thread, " + "and configuration does not allow creation of non-transactional one here");
		}

		return db;
	}

	/**
	 * Return whether the given DB instance is transactional, that is, bound to the current thread by Spring's transaction
	 * facilities.
	 * 
	 * @param db the DB to check
	 * @param mongoClient the Mongo instance that the DB was created with (may be <code>null</code>)
	 * @return whether the DB is transactional
	 */
	public static boolean isDBTransactional(MongoDatabase db, MongoClient mongoClient) {

		if (mongoClient == null) {
			return false;
		}
		ReactiveMongoDatabaseHolder dbHolder = (ReactiveMongoDatabaseHolder) TransactionSynchronizationManager
				.getResource(mongoClient);
		return dbHolder != null && dbHolder.containsMongoDatabase(db);
	}

	/**
	 * Check if credentials present. In case we're using a mongo-java-driver version 3 or above we do not have the need
	 * for authentication as the auth data has to be provided within the MongoClient
	 * 
	 * @param credentials
	 * @return
	 */
	private static boolean requiresAuthDbAuthentication(UserCredentials credentials) {

		if (credentials == null || !credentials.hasUsername()) {
			return false;
		}

		return !MongoClientVersion.isMongo3Driver();
	}
}
