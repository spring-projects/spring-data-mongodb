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

import java.util.List;

import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.Assert;

/**
 * Convenient factory for configuring MongoDB.
 * 
 * @author Thomas Risberg
 * @author Graeme Rocher
 * @since 1.0
 */
public class MongoFactoryBean implements FactoryBean<Mongo>, InitializingBean, PersistenceExceptionTranslator {

	/**
	 * Logger, available to subclasses.
	 */
	protected final Log logger = LogFactory.getLog(getClass());

	private Mongo mongo;
	private MongoOptions mongoOptions;
	private String host;
	private Integer port;
	private List<ServerAddress> replicaSetSeeds;
	private List<ServerAddress> replicaPair;

	private PersistenceExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();

	public void setMongoOptions(MongoOptions mongoOptions) {
		this.mongoOptions = mongoOptions;
	}

	public void setReplicaSetSeeds(List<ServerAddress> replicaSetSeeds) {
		this.replicaSetSeeds = replicaSetSeeds;
	}

	public void setReplicaPair(List<ServerAddress> replicaPair) {
		this.replicaPair = replicaPair;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public PersistenceExceptionTranslator getExceptionTranslator() {
		return exceptionTranslator;
	}

	public void setExceptionTranslator(PersistenceExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator;
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
				mongo = new Mongo();
			} else {
				ServerAddress defaultOptions = new ServerAddress();
				if (mongoOptions == null)
					mongoOptions = new MongoOptions();
				if (replicaPair != null) {
					if (replicaPair.size() < 2) {
						throw new CannotGetMongoDbConnectionException("A replica pair must have two server entries");
					}
					mongo = new Mongo(replicaPair.get(0), replicaPair.get(1), mongoOptions);
				} else if (replicaSetSeeds != null) {
					mongo = new Mongo(replicaSetSeeds, mongoOptions);
				} else {
					String mongoHost = host != null ? host : defaultOptions.getHost();
					if (port != null) {
						mongo = new Mongo(new ServerAddress(mongoHost, port), mongoOptions);
					} else {
						mongo = new Mongo(mongoHost, mongoOptions);
					}
				}
			}
		}
	}

	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}
}
