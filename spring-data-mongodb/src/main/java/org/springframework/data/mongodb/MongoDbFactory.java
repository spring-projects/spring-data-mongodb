/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.mongodb;

import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;

import com.mongodb.ClientSessionOptions;
import com.mongodb.DB;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;

/**
 * Interface for factories creating {@link MongoDatabase} instances.
 *
 * @author Mark Pollack
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public interface MongoDbFactory extends CodecRegistryProvider, MongoSessionProvider {

	/**
	 * Creates a default {@link MongoDatabase} instance.
	 *
	 * @return
	 * @throws DataAccessException
	 */
	MongoDatabase getDb() throws DataAccessException;

	/**
	 * Creates a {@link DB} instance to access the database with the given name.
	 *
	 * @param dbName must not be {@literal null} or empty.
	 * @return
	 * @throws DataAccessException
	 */
	MongoDatabase getDb(String dbName) throws DataAccessException;

	/**
	 * Exposes a shared {@link MongoExceptionTranslator}.
	 *
	 * @return will never be {@literal null}.
	 */
	PersistenceExceptionTranslator getExceptionTranslator();

	/**
	 * Get the legacy database entry point. Please consider {@link #getDb()} instead.
	 *
	 * @return
	 * @deprecated since 2.1, use {@link #getDb()}. This method will be removed with a future version as it works only
	 *             with the legacy MongoDB driver.
	 */
	@Deprecated
	DB getLegacyDb();

	/**
	 * Get the underlying {@link CodecRegistry} used by the MongoDB Java driver.
	 *
	 * @return never {@literal null}.
	 */
	@Override
	default CodecRegistry getCodecRegistry() {
		return getDb().getCodecRegistry();
	}

	/**
	 * Obtain a {@link ClientSession} for given ClientSessionOptions.
	 *
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	ClientSession getSession(ClientSessionOptions options);

	/**
	 * Obtain a {@link ClientSession} bound instance of {@link MongoDbFactory} returning {@link MongoDatabase} instances
	 * that are aware and bound to a new session with given {@link ClientSessionOptions options}.
	 *
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	default MongoDbFactory withSession(ClientSessionOptions options) {
		return withSession(getSession(options));
	}

	/**
	 * Obtain a {@link ClientSession} bound instance of {@link MongoDbFactory} returning {@link MongoDatabase} instances
	 * that are aware and bound to the given session.
	 *
	 * @param session must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	MongoDbFactory withSession(ClientSession session);
}
