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

package org.springframework.datastore.document.mongodb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.datastore.document.DocumentStoreConnectionFactory;
import org.springframework.util.Assert;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * Convenient factory for configuring MongoDB.
 *
 * @author Thomas Risberg
 * @since 1.0
 */
public class MongoDbConnectionFactory implements DocumentStoreConnectionFactory<DB>, InitializingBean {

	/**
	 * Logger, available to subclasses.
	 */
	protected final Log logger = LogFactory.getLog(getClass());

	private Mongo mongo;
	private String databaseName;

	public MongoDbConnectionFactory() {
		super();
	}
	
	public MongoDbConnectionFactory(String databaseName) {
		super();
		this.databaseName = databaseName;
	}
	
	public MongoDbConnectionFactory(Mongo mongo, String databaseName) {
		super();
		this.mongo = mongo;
		this.databaseName = databaseName;
	}

	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
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
			mongo = new Mongo();
		}
	}

	public DB getConnection() {
		Assert.notNull(mongo, "Mongo must not be null");
		Assert.hasText(databaseName, "Database name must not be empty");
		return mongo.getDB(databaseName);
	}

}
