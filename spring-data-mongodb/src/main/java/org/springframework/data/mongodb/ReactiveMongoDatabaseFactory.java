/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.mongodb;

import reactor.core.publisher.Mono;

import org.bson.codecs.configuration.CodecRegistry;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.util.Assert;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Interface for factories creating reactive {@link MongoDatabase} instances.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Mathieu Ouellet
 * @since 2.0
 */
public interface ReactiveMongoDatabaseFactory extends CodecRegistryProvider {

	/**
	 * Creates a new {@link SimpleReactiveMongoDatabaseFactory} instance for the given {@code connectionString}. Using
	 * this factory method will create a new {@link MongoClient} instance that will be closed when calling
	 * {@link SimpleReactiveMongoDatabaseFactory#destroy()}.
	 *
	 * @param connectionString connection coordinates for a database connection. Must contain a database name and must not
	 *          be {@literal null} or empty.
	 * @since 4.5.2
	 * @see <a href="https://docs.mongodb.com/manual/reference/connection-string/">MongoDB Connection String reference</a>
	 */
	static SimpleReactiveMongoDatabaseFactory create(String connectionString) {

		Assert.notNull(connectionString, "ConnectionString must not be null");

		return create(new ConnectionString(connectionString));
	}

	/**
	 * Creates a new {@link SimpleReactiveMongoDatabaseFactory} instance from the given {@link MongoClient}. Using this
	 * factory will create a new {@link MongoClient} instance that will be closed when calling
	 * {@link SimpleReactiveMongoDatabaseFactory#destroy()}.
	 *
	 * @param connectionString connection coordinates for a database connection. Must contain also a database name and not
	 *          be {@literal null}.
	 * @since 4.5.2
	 */
	static SimpleReactiveMongoDatabaseFactory create(ConnectionString connectionString) {

		Assert.notNull(connectionString, "ConnectionString must not be null");

		return new SimpleReactiveMongoDatabaseFactory(connectionString);
	}

	/**
	 * Creates a new {@link MongoDatabaseFactory} instance from the given {@link MongoClient}. We assume a managed client
	 * instance that will be disposed by you (or the application container) once the client is no longer required for use.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 * @since 4.5.2
	 */
	static ReactiveMongoDatabaseFactory create(MongoClient mongoClient, String databaseName) {
		return new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName);
	}

	/**
	 * Creates a default {@link MongoDatabase} instance.
	 *
	 * @return never {@literal null}.
	 * @throws DataAccessException
	 */
	Mono<MongoDatabase> getMongoDatabase() throws DataAccessException;

	/**
	 * Obtain a {@link MongoDatabase} instance to access the database with the given name.
	 *
	 * @param dbName must not be {@literal null} or empty.
	 * @return never {@literal null}.
	 * @throws DataAccessException
	 */
	Mono<MongoDatabase> getMongoDatabase(String dbName) throws DataAccessException;

	/**
	 * Exposes a shared {@link MongoExceptionTranslator}.
	 *
	 * @return will never be {@literal null}.
	 */
	PersistenceExceptionTranslator getExceptionTranslator();

	/**
	 * Get the underlying {@link CodecRegistry} used by the reactive MongoDB Java driver.
	 *
	 * @return never {@literal null}.
	 */
	CodecRegistry getCodecRegistry();

	/**
	 * Obtain a {@link Mono} emitting a {@link ClientSession} for given {@link ClientSessionOptions options}.
	 *
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	Mono<ClientSession> getSession(ClientSessionOptions options);

	/**
	 * Obtain a {@link ClientSession} bound instance of {@link ReactiveMongoDatabaseFactory} returning
	 * {@link MongoDatabase} instances that are aware and bound to the given session.
	 *
	 * @param session must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	ReactiveMongoDatabaseFactory withSession(ClientSession session);

	/**
	 * Returns if the given {@link ReactiveMongoDatabaseFactory} is bound to a
	 * {@link com.mongodb.reactivestreams.client.ClientSession} that has an
	 * {@link com.mongodb.reactivestreams.client.ClientSession#hasActiveTransaction() active transaction}.
	 *
	 * @return {@literal true} if there's an active transaction, {@literal false} otherwise.
	 * @since 2.2
	 */
	default boolean isTransactionActive() {
		return false;
	}
}
