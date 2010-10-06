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

package org.springframework.datastore.document.mongodb;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.Assert;

import com.mongodb.DB;
import com.mongodb.Mongo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Convenient factory for configuring MongoDB.
 *
 * @author Thomas Risberg
 * @since 1.0
 */
public class MongoDbFactoryBean implements FactoryBean<DB>, InitializingBean, 
		PersistenceExceptionTranslator {

	/**
	 * Logger, available to subclasses.
	 */
	protected final Log logger = LogFactory.getLog(getClass());

	private Mongo mongo;
	private String host;
	private Integer port;
	private String databaseName;
	
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

	public DB getObject() throws Exception {
		Assert.notNull(mongo, "Mongo must not be null");
		Assert.hasText(databaseName, "Database name must not be empty");
		return mongo.getDB(databaseName);
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

	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		logger.debug("Translating " + ex);
		return MongoDbUtils.translateMongoExceptionIfPossible(ex);
	}

}
