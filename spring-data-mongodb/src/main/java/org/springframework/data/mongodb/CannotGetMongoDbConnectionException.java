/*
 * Copyright 2010-2017 the original author or authors.
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
package org.springframework.data.mongodb;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.lang.Nullable;

/**
 * Exception being thrown in case we cannot connect to a MongoDB instance.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class CannotGetMongoDbConnectionException extends DataAccessResourceFailureException {

	private final UserCredentials credentials;
	private final @Nullable String database;

	private static final long serialVersionUID = 1172099106475265589L;

	public CannotGetMongoDbConnectionException(String msg, Throwable cause) {
		super(msg, cause);
		this.database = null;
		this.credentials = UserCredentials.NO_CREDENTIALS;
	}

	public CannotGetMongoDbConnectionException(String msg) {
		this(msg, null, UserCredentials.NO_CREDENTIALS);
	}

	public CannotGetMongoDbConnectionException(String msg, @Nullable String database, UserCredentials credentials) {
		super(msg);
		this.database = database;
		this.credentials = credentials;
	}

	/**
	 * Returns the {@link UserCredentials} that were used when trying to connect to the MongoDB instance.
	 *
	 * @return
	 */
	public UserCredentials getCredentials() {
		return this.credentials;
	}

	/**
	 * Returns the name of the database trying to be accessed.
	 *
	 * @return
	 */
	@Nullable
	public String getDatabase() {
		return database;
	}
}
