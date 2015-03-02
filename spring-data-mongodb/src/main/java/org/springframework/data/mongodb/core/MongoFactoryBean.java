/*
 * Copyright 2010-2015 the original author or authors.
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

import org.springframework.beans.factory.config.AbstractFactoryBean;
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
 * @author Christoph Strobl
 * @since 1.0
 * @deprecated since 1.7. Please use {@link MongoClientFactoryBean} instead.
 */
@Deprecated
public class MongoFactoryBean extends AbstractFactoryBean<Mongo> implements PersistenceExceptionTranslator {

	private static final PersistenceExceptionTranslator DEFAULT_EXCEPTION_TRANSLATOR = new MongoExceptionTranslator();

	private MongoOptions mongoOptions;
	private String host;
	private Integer port;
	private WriteConcern writeConcern;
	private List<ServerAddress> replicaSetSeeds;
	private List<ServerAddress> replicaPair;
	private PersistenceExceptionTranslator exceptionTranslator = DEFAULT_EXCEPTION_TRANSLATOR;

	/**
	 * @param mongoOptions
	 */
	public void setMongoOptions(MongoOptions mongoOptions) {
		this.mongoOptions = mongoOptions;
	}

	public void setReplicaSetSeeds(ServerAddress[] replicaSetSeeds) {
		this.replicaSetSeeds = filterNonNullElementsAsList(replicaSetSeeds);
	}

	/**
	 * @deprecated use {@link #setReplicaSetSeeds(ServerAddress[])} instead
	 * @param replicaPair
	 */
	@Deprecated
	public void setReplicaPair(ServerAddress[] replicaPair) {
		this.replicaPair = filterNonNullElementsAsList(replicaPair);
	}

	/**
	 * Configures the host to connect to.
	 * 
	 * @param host
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Configures the port to connect to.
	 * 
	 * @param port
	 */
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

	/**
	 * Configures the {@link PersistenceExceptionTranslator} to use.
	 * 
	 * @param exceptionTranslator can be {@literal null}.
	 */
	public void setExceptionTranslator(PersistenceExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator == null ? DEFAULT_EXCEPTION_TRANSLATOR : exceptionTranslator;
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
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.config.AbstractFactoryBean#createInstance()
	 */
	@Override
	protected Mongo createInstance() throws Exception {

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

		return mongo;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.config.AbstractFactoryBean#destroyInstance(java.lang.Object)
	 */
	@Override
	protected void destroyInstance(Mongo mongo) throws Exception {
		mongo.close();
	}

	private static boolean isNullOrEmpty(Collection<?> elements) {
		return elements == null || elements.isEmpty();
	}

	/**
	 * Returns the given array as {@link List} with all {@literal null} elements removed.
	 * 
	 * @param elements the elements to filter <T>
	 * @return a new unmodifiable {@link List#} from the given elements without nulls
	 */
	private static <T> List<T> filterNonNullElementsAsList(T[] elements) {

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
}
