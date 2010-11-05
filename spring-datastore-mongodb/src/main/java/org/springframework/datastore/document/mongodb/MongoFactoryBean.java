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
import com.mongodb.MongoOptions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Convenient factory for configuring MongoDB.
 *
 * @author Thomas Risberg
 * @author Graeme Rocher
 * 
 * @since 1.0
 */
public class MongoFactoryBean implements FactoryBean<Mongo>, InitializingBean, 
		PersistenceExceptionTranslator {

	/**
	 * Logger, available to subclasses.
	 */
	protected final Log logger = LogFactory.getLog(getClass());

	private Mongo mongo;
	private MongoOptions mongoOptions;
	private String host;
	private Integer port;
	private String databaseName;
	
	
	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}

	public void setMongoOptions(MongoOptions mongoOptions) {
		this.mongoOptions = mongoOptions;
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

	public Mongo getObject() throws Exception {
		Assert.notNull(mongo, "Mongo must not be null");
		return mongo;
	}

	public Class<? extends Mongo> getObjectType() {
		return Mongo.class;
	}

	public boolean isSingleton() {
		return false;
	}

	public void afterPropertiesSet() throws Exception {
		// apply defaults - convenient when used to configure for tests 
		// in an application context
		if (mongo == null) {
			
			if (host == null) {
				logger.warn("Property host not specified. Using default configuration");
				mongo =  new Mongo();
			}
			else {
				if(mongoOptions != null) {
					mongo = new Mongo(host != null ? host : "localhost", mongoOptions);
				}
				else if (port == null) {
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
