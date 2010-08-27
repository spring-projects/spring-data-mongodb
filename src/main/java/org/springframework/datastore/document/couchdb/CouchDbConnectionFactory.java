/*
 * Copyright 2010 the original author or authors.
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

/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.datastore.document.couchdb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jcouchdb.db.Database;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.datastore.document.DocumentStoreConnectionFactory;


/**
 * Convenient factory for configuring CouchDB.
 *
 * @author Thomas Risberg
 * @since 1.0
 */
public class CouchDbConnectionFactory implements DocumentStoreConnectionFactory<Database>, InitializingBean {

	/**
	 * Logger, available to subclasses.
	 */
	protected final Log logger = LogFactory.getLog(getClass());

	private Database database;
	private String host;
	private String databaseName;

	public CouchDbConnectionFactory() {
		super();
	}
	
	public CouchDbConnectionFactory(String host, String databaseName) {
		super();
		this.host = host;
		this.databaseName = databaseName;
	}
	
	public CouchDbConnectionFactory(Database database) {
		super();
		this.database = database;
	}

	public void setDatabase(Database database) {
		this.database = database;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public boolean isSingleton() {
		return false;
	}

	public void afterPropertiesSet() throws Exception {
		// apply defaults - convenient when used to configure for tests 
		// in an application context
		if (database == null) {
			if (databaseName == null) {
				logger.warn("Property databaseName not specified. Using default name 'test'");
				databaseName = "test";
			}
			if (host == null) {
				logger.warn("Property host not specified. Using default 'localhost'");
				database = new Database(host, databaseName);
			}
			database = new Database(host, databaseName);
		}
		else {
			logger.info("Using provided database configuration");
		}
	}

	public Database getConnection() {
		synchronized (this){
			if (database == null) {
				try {
					afterPropertiesSet();
				} catch (Exception e) {
					throw new CannotGetCouchDbConnectionException("Unable to connect to CouchDB", e);
				}
			}
		}
		return database;
	}

}
