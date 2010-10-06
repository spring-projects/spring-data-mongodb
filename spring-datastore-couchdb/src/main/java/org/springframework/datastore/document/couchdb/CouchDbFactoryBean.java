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

import org.jcouchdb.db.Database;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Convenient factory for configuring MongoDB.
 *
 * @author Thomas Risberg
 * @since 1.0
 */
public class CouchDbFactoryBean implements FactoryBean<Database>, InitializingBean, 
		PersistenceExceptionTranslator {

	/**
	 * Logger, available to subclasses.
	 */
	protected final Log logger = LogFactory.getLog(getClass());

	private String host;
	private Integer port;
	private String databaseName;
	
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public Database getObject() throws Exception {
		Assert.hasText(host, "Host must not be empty");
		Assert.hasText(databaseName, "Database name must not be empty");
		if (port == null) {
			return new Database(host, databaseName);
		}
		else {
			return new Database(host, port, databaseName);
		}
	}

	public Class<? extends Database> getObjectType() {
		return Database.class;
	}

	public boolean isSingleton() {
		return false;
	}

	public void afterPropertiesSet() throws Exception {
		// apply defaults - convenient when used to configure for tests 
		// in an application context
		if (host == null) {
			logger.warn("Property host not specified. Using default 'localhost'");
			databaseName = "localhost";
		}
		if (databaseName == null) {
			logger.warn("Property databaseName not specified. Using default name 'test'");
			databaseName = "test";
		}
	}

	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		logger.debug("Translating " + ex);
		return CouchDbUtils.translateCouchExceptionIfPossible(ex);
	}

}
