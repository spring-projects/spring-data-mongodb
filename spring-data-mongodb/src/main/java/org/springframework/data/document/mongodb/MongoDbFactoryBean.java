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

package org.springframework.data.document.mongodb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * Convenient factory for configuring MongoDB.
 *
 * @author Thomas Risberg
 * @since 1.0
 */
public class MongoDbFactoryBean implements FactoryBean<DB>, InitializingBean {
	//ToDo: add	PersistenceExceptionTranslator ???

	/**
	 * Logger, available to subclasses.
	 */
	protected final Log logger = LogFactory.getLog(getClass());

	private Mongo mongo;
	private String host;
	private Integer port;
	private String databaseName;
	private String username;
	private String password;
	
	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public DB getDb() throws DataAccessException {
		Assert.notNull(mongo, "Mongo must not be null");
		Assert.hasText(databaseName, "Database name must not be empty");
		return MongoDbUtils.getDB(mongo, databaseName, username, password == null ? null : password.toCharArray());
	}

	public DB getObject() throws Exception {
		return getDb();
	}

	public Class<? extends DB> getObjectType() {
		return DB.class;
	}

	public boolean isSingleton() {
		return false;
	}

	public void afterPropertiesSet() throws Exception {
		// apply defaults - convenient when used to configure for tests 
		// in an application context
		//ToDo: do we need a default or should we require database name?
		if (databaseName == null) {
			logger.warn("Property databaseName not specified. Using default name 'test'");
			databaseName = "test";
		}
		if (mongo == null) {
			logger.warn("Property mongo not specified. Using default configuration");
			if (host == null) {
				mongo =  new Mongo();
			}
			else {
				if (port == null) {
					mongo = new Mongo(host);
				}
				else {
					mongo = new Mongo(host, port);					
				}
			}
		}
	}

}