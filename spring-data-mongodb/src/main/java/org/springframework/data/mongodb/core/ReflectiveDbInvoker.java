/*
 * Copyright 2015 the original author or authors.
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

import static org.springframework.data.mongodb.util.MongoClientVersion.*;
import static org.springframework.util.ReflectionUtils.*;

import java.lang.reflect.Method;

import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.CannotGetMongoDbConnectionException;
import org.springframework.data.mongodb.util.MongoClientVersion;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * {@link ReflectiveDbInvoker} provides reflective access to {@link DB} API that is not consistently available for
 * various driver versions.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
final class ReflectiveDbInvoker {

	private static final Method DB_IS_AUTHENTICATED_METHOD;
	private static final Method DB_AUTHENTICATE_METHOD;
	private static final Method DB_REQUEST_DONE_METHOD;
	private static final Method DB_ADD_USER_METHOD;
	private static final Method DB_REQUEST_START_METHOD;

	static {

		DB_IS_AUTHENTICATED_METHOD = findMethod(DB.class, "isAuthenticated");
		DB_AUTHENTICATE_METHOD = findMethod(DB.class, "authenticate", String.class, char[].class);
		DB_REQUEST_DONE_METHOD = findMethod(DB.class, "requestDone");
		DB_ADD_USER_METHOD = findMethod(DB.class, "addUser", String.class, char[].class);
		DB_REQUEST_START_METHOD = findMethod(DB.class, "requestStart");
	}

	private ReflectiveDbInvoker() {}

	/**
	 * Authenticate against database using provided credentials in case of a MongoDB Java driver version 2.
	 *
	 * @param mongo must not be {@literal null}.
	 * @param db must not be {@literal null}.
	 * @param credentials must not be {@literal null}.
	 * @param authenticationDatabaseName
	 */
	public static void authenticate(Mongo mongo, DB db, UserCredentials credentials, String authenticationDatabaseName) {

		String databaseName = db.getName();

		DB authDb = databaseName.equals(authenticationDatabaseName) ? db : mongo.getDB(authenticationDatabaseName);

		synchronized (authDb) {

			Boolean isAuthenticated = (Boolean) invokeMethod(DB_IS_AUTHENTICATED_METHOD, authDb);
			if (!isAuthenticated) {

				String username = credentials.getUsername();
				String password = credentials.hasPassword() ? credentials.getPassword() : null;

				Boolean authenticated = (Boolean) invokeMethod(DB_AUTHENTICATE_METHOD, authDb, username,
						password == null ? null : password.toCharArray());
				if (!authenticated) {
					throw new CannotGetMongoDbConnectionException("Failed to authenticate to database [" + databaseName + "], "
							+ credentials.toString(), databaseName, credentials);
				}
			}
		}
	}

	/**
	 * Starts a new 'consistent request' in case of MongoDB Java driver version 2. Will do nothing for MongoDB Java driver
	 * version 3 since the operation is no longer available.
	 *
	 * @param db
	 */
	public static void requestStart(DB db) {

		if (isMongo3Driver()) {
			return;
		}

		invokeMethod(DB_REQUEST_START_METHOD, db);
	}

	/**
	 * Ends the current 'consistent request'. a new 'consistent request' in case of MongoDB Java driver version 2. Will do
	 * nothing for MongoDB Java driver version 3 since the operation is no longer available
	 *
	 * @param db
	 */
	public static void requestDone(DB db) {

		if (MongoClientVersion.isMongo3Driver()) {
			return;
		}

		invokeMethod(DB_REQUEST_DONE_METHOD, db);
	}

	/**
	 * @param db
	 * @param username
	 * @param password
	 * @throws UnsupportedOperationException
	 */
	public static void addUser(DB db, String username, char[] password) {

		if (isMongo3Driver()) {
			throw new UnsupportedOperationException(
					"Please use DB.command(…) to call either the createUser or updateUser command!");
		}

		invokeMethod(DB_ADD_USER_METHOD, db, username, password);
	}
}
