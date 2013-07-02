/*
 * Copyright 2010-2012 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.CannotGetMongoDbConnectionException;
import org.springframework.util.StringUtils;

import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

/**
 * Convenient factory for configuring MongoDB.
 * 
 * @author Thomas Risberg
 * @author Graeme Rocher
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @since 1.0
 */
public class MongoFactoryBean implements FactoryBean<Mongo>, InitializingBean, DisposableBean,
		PersistenceExceptionTranslator {

	private Mongo mongo;

	private MongoOptions mongoOptions;
	private String host;
	private Integer port;
	private WriteConcern writeConcern;
	private List<ServerAddress> replicaSetSeeds;
	private List<ServerAddress> replicaPair;

	private PersistenceExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();

	public void setMongoOptions(MongoOptions mongoOptions) {
		this.mongoOptions = mongoOptions;
	}

	public void setReplicaSetSeeds(ServerAddress[] replicaSetSeeds) {
		this.replicaSetSeeds = filterNonNullElementsAsList(replicaSetSeeds);
	}

	/**
	 * @deprecated use {@link #setReplicaSetSeeds(ServerAddress[])} instead
	 * 
	 * @param replicaPair
	 */
	@Deprecated
	public void setReplicaPair(ServerAddress[] replicaPair) {
		this.replicaPair = filterNonNullElementsAsList(replicaPair);
	}

	/**
	 * @param elements the elements to filter <T>
	 * @return a new unmodifiable {@link List#} from the given elements without nulls
	 */
	private <T> List<T> filterNonNullElementsAsList(T[] elements) {

		if (elements == null) {
			return Collections.emptyList();
		}

		List<T> candidateElements = new ArrayList<T>();

		for (T element : elements) {
			if (element != null) {
				candidateElements.add(element);
			}
		}

		return Collections.unmodifiableList(candidateElements);
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Sets the {@link WriteConcern} to be configured for the {@link Mongo} instance to be created.
	 * 
	 * @param writeConcern
	 */
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	public void setExceptionTranslator(PersistenceExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator;
	}

	public Mongo getObject() throws Exception {
		return mongo;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<? extends Mongo> getObjectType() {
		return Mongo.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	public boolean isSingleton() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {

		Mongo mongo;
		ServerAddress defaultOptions = new ServerAddress();

		if (mongoOptions == null) {
			mongoOptions = new MongoOptions();
		}

		if (!isNullOrEmpty(replicaPair)) {
			if (replicaPair.size() < 2) {
				throw new CannotGetMongoDbConnectionException("A replica pair must have two server entries");
			}
			mongo = new Mongo(replicaPair.get(0), replicaPair.get(1), mongoOptions);
		} else if (!isNullOrEmpty(replicaSetSeeds)) {
			mongo = new Mongo(replicaSetSeeds, mongoOptions);
		} else {
			String mongoHost = StringUtils.hasText(host) ? host : defaultOptions.getHost();
			mongo = port != null ? new Mongo(new ServerAddress(mongoHost, port), mongoOptions) : new Mongo(mongoHost,
					mongoOptions);
		}

		if (writeConcern != null) {
			mongo.setWriteConcern(writeConcern);
		}

		this.mongo = mongo;
	}

	private boolean isNullOrEmpty(Collection<?> elements) {
		return elements == null || elements.isEmpty();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() throws Exception {
		this.mongo.close();
	}
}
