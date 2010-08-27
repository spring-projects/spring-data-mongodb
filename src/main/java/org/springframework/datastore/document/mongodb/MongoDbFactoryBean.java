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
public class MongoDbFactoryBean implements FactoryBean<DB>, InitializingBean {

	/**
	 * Logger, available to subclasses.
	 */
	protected final Log logger = LogFactory.getLog(getClass());

	private MongoDbConnectionFactory mcf = new MongoDbConnectionFactory();
	
	public void setMongo(Mongo mongo) {
		this.mcf.setMongo(mongo);
	}

	public void setDatabaseName(String databaseName) {
		this.mcf.setDatabaseName(databaseName);
	}

	public DB getObject() throws Exception {
		return mcf.getConnection();
	}

	public Class<? extends DB> getObjectType() {
		return DB.class;
	}

	public boolean isSingleton() {
		return false;
	}

	public void afterPropertiesSet() throws Exception {
		this.mcf.afterPropertiesSet();
	}

}
