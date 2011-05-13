/*
 * Copyright 2002-2011 the original author or authors.
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
package org.springframework.data.document.mongodb;

import com.mongodb.DB;
import com.mongodb.Mongo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

/**
 * Mongo server administration exposed via JMX annotations
 * 
 * @author Mark Pollack
 */

@ManagedResource(description = "Mongo Admin Operations")
public class MongoAdmin implements MongoAdminOperations {

	/**
	 * Logger available to subclasses
	 */
	protected final Log logger = LogFactory.getLog(getClass());

	private Mongo mongo;
	private String username;
	private String password;

	public MongoAdmin(Mongo mongo) {
		this.mongo = mongo;
	}

	/* (non-Javadoc)
	  * @see org.springframework.data.document.mongodb.MongoAdminOperations#dropDatabase(java.lang.String)
	  */
	@ManagedOperation
	public void dropDatabase(String databaseName) {
		getDB(databaseName).dropDatabase();
	}

	/* (non-Javadoc)
	  * @see org.springframework.data.document.mongodb.MongoAdminOperations#createDatabase(java.lang.String)
	  */
	@ManagedOperation
	public void createDatabase(String databaseName) {
		getDB(databaseName);
	}

	/* (non-Javadoc)
	  * @see org.springframework.data.document.mongodb.MongoAdminOperations#getDatabaseStats(java.lang.String)
	  */
	@ManagedOperation
	public String getDatabaseStats(String databaseName) {
		return getDB(databaseName).getStats().toString();
	}

	/**
	 * Sets the username to use to connect to the Mongo database
	 * 
	 * @param username
	 *          The username to use
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Sets the password to use to authenticate with the Mongo database.
	 * 
	 * @param password
	 *          The password to use
	 */
	public void setPassword(String password) {

		this.password = password;
	}

	DB getDB(String databaseName) {
		return MongoDbUtils.getDB(mongo, databaseName, username, password == null ? null : password.toCharArray());
	}
}
