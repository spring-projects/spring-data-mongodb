/*
 * Copyright 2016-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.mongodb.core;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * Convenient factory for configuring a reactive streams {@link MongoClient}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public class ReactiveMongoClientFactoryBean extends AbstractFactoryBean<MongoClient>
		implements PersistenceExceptionTranslator {

	private static final PersistenceExceptionTranslator DEFAULT_EXCEPTION_TRANSLATOR = new MongoExceptionTranslator();

	private @Nullable String connectionString;
	private @Nullable String host;
	private @Nullable Integer port;
	private @Nullable MongoClientSettings mongoClientSettings;
	private PersistenceExceptionTranslator exceptionTranslator = DEFAULT_EXCEPTION_TRANSLATOR;

	/**
	 * Configures the host to connect to.
	 *
	 * @param host
	 */
	public void setHost(@Nullable String host) {
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
	 * Configures the connection string.
	 *
	 * @param connectionString
	 */
	public void setConnectionString(@Nullable String connectionString) {
		this.connectionString = connectionString;
	}

	/**
	 * Configures the mongo client settings.
	 *
	 * @param mongoClientSettings
	 */
	public void setMongoClientSettings(@Nullable MongoClientSettings mongoClientSettings) {
		this.mongoClientSettings = mongoClientSettings;
	}

	/**
	 * Configures the {@link PersistenceExceptionTranslator} to use.
	 *
	 * @param exceptionTranslator
	 */
	public void setExceptionTranslator(@Nullable PersistenceExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator == null ? DEFAULT_EXCEPTION_TRANSLATOR : exceptionTranslator;
	}

	@Override
	public Class<?> getObjectType() {
		return MongoClient.class;
	}

	@Override
	protected MongoClient createInstance() throws Exception {

		if (mongoClientSettings != null) {
			return MongoClients.create(mongoClientSettings);
		}

		if (StringUtils.hasText(connectionString)) {
			return MongoClients.create(connectionString);
		}

		if (StringUtils.hasText(host)) {

			if (port != null) {
				return MongoClients.create(String.format("mongodb://%s:%d", host, port));
			}

			return MongoClients.create(String.format("mongodb://%s", host));
		}

		throw new IllegalStateException(
				"Cannot create MongoClients. One of the following is required: mongoClientSettings, connectionString or host/port");
	}

	@Override
	protected void destroyInstance(@Nullable MongoClient instance) throws Exception {
		instance.close();
	}

	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}
}
