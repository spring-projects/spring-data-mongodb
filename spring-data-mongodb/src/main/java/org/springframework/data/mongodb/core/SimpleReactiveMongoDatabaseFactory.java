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
package org.springframework.data.mongodb.core;

import reactor.core.publisher.Mono;

import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.SessionAwareMethodInterceptor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ConnectionString;
import com.mongodb.WriteConcern;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Factory to create {@link MongoDatabase} instances from a {@link MongoClient} instance.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Mathieu Ouellet
 * @since 2.0
 */
public class SimpleReactiveMongoDatabaseFactory implements DisposableBean, ReactiveMongoDatabaseFactory {

	private final MongoClient mongo;
	private final String databaseName;
	private final boolean mongoInstanceCreated;

	private PersistenceExceptionTranslator exceptionTranslator = MongoExceptionTranslator.DEFAULT_EXCEPTION_TRANSLATOR;
	private @Nullable WriteConcern writeConcern;

	/**
	 * Creates a new {@link SimpleReactiveMongoDatabaseFactory} instance from the given {@link ConnectionString}. Using
	 * this constructor will create a new {@link MongoClient} instance that will be closed when calling
	 * {@link #destroy()}.
	 *
	 * @param connectionString must not be {@literal null}.
	 */
	public SimpleReactiveMongoDatabaseFactory(ConnectionString connectionString) {
		this(MongoClients.create(connectionString), connectionString.getDatabase(), true);
	}

	/**
	 * Creates a new {@link SimpleReactiveMongoDatabaseFactory} instance from the given {@link MongoClient}. Note that the
	 * client will not be closed when calling {@link #destroy()} as we assume a managed client instance that we do not
	 * want to close on {@link #destroy()} meaning that you (or the application container) must dispose the client
	 * instance once it is no longer required for use.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null}.
	 * @since 1.7
	 */
	public SimpleReactiveMongoDatabaseFactory(MongoClient mongoClient, String databaseName) {
		this(mongoClient, databaseName, false);
	}

	private SimpleReactiveMongoDatabaseFactory(MongoClient client, String databaseName, boolean mongoInstanceCreated) {

		Assert.notNull(client, "MongoClient must not be null");
		Assert.hasText(databaseName, "Database name must not be empty");
		Assert.isTrue(databaseName.matches("[^/\\\\.$\"\\s]+"),
				"Database name must not contain slashes, dots, spaces, quotes, or dollar signs");

		this.mongo = client;
		this.databaseName = databaseName;
		this.mongoInstanceCreated = mongoInstanceCreated;
	}

	/**
	 * Configures the {@link PersistenceExceptionTranslator} to be used.
	 *
	 * @param exceptionTranslator the exception translator to set.
	 * @since 4.4
	 */
	public void setExceptionTranslator(PersistenceExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator;
	}

	@Override
	public PersistenceExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	/**
	 * Configures the {@link WriteConcern} to be used on the {@link MongoDatabase} instance being created.
	 *
	 * @param writeConcern the writeConcern to set
	 */
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	@Override
	public Mono<MongoDatabase> getMongoDatabase() throws DataAccessException {
		return getMongoDatabase(databaseName);
	}

	@Override
	public Mono<MongoDatabase> getMongoDatabase(String dbName) throws DataAccessException {

		Assert.hasText(dbName, "Database name must not be empty");

		return Mono.fromSupplier(() -> {

			MongoDatabase db = mongo.getDatabase(dbName);

			return writeConcern != null ? db.withWriteConcern(writeConcern) : db;
		});
	}

	/**
	 * Clean up the Mongo instance if it was created by the factory itself.
	 *
	 * @see DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {

		if (mongoInstanceCreated) {
			mongo.close();
		}
	}

	@Override
	public CodecRegistry getCodecRegistry() {
		return this.mongo.getDatabase(databaseName).getCodecRegistry();
	}

	@Override
	public Mono<ClientSession> getSession(ClientSessionOptions options) {
		return Mono.from(mongo.startSession(options));
	}

	@Override
	public ReactiveMongoDatabaseFactory withSession(ClientSession session) {
		return new ClientSessionBoundMongoDbFactory(session, this);
	}

	/**
	 * {@link ClientSession} bound {@link ReactiveMongoDatabaseFactory} decorating the database with a
	 * {@link SessionAwareMethodInterceptor}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	record ClientSessionBoundMongoDbFactory(ClientSession session,
			ReactiveMongoDatabaseFactory delegate) implements ReactiveMongoDatabaseFactory {

		@Override
		public Mono<MongoDatabase> getMongoDatabase() throws DataAccessException {
			return delegate.getMongoDatabase().map(this::decorateDatabase);
		}

		@Override
		public Mono<MongoDatabase> getMongoDatabase(String dbName) throws DataAccessException {
			return delegate.getMongoDatabase(dbName).map(this::decorateDatabase);
		}

		@Override
		public PersistenceExceptionTranslator getExceptionTranslator() {
			return delegate.getExceptionTranslator();
		}

		@Override
		public CodecRegistry getCodecRegistry() {
			return delegate.getCodecRegistry();
		}

		@Override
		public Mono<ClientSession> getSession(ClientSessionOptions options) {
			return delegate.getSession(options);
		}

		@Override
		public ReactiveMongoDatabaseFactory withSession(ClientSession session) {
			return delegate.withSession(session);
		}

		@Override
		public boolean isTransactionActive() {
			return session.hasActiveTransaction();
		}

		private MongoDatabase decorateDatabase(MongoDatabase database) {
			return createProxyInstance(session, database, MongoDatabase.class);
		}

		private MongoDatabase proxyDatabase(com.mongodb.session.ClientSession session, MongoDatabase database) {
			return createProxyInstance(session, database, MongoDatabase.class);
		}

		private MongoCollection<?> proxyCollection(com.mongodb.session.ClientSession session,
				MongoCollection<?> collection) {
			return createProxyInstance(session, collection, MongoCollection.class);
		}

		private <T> T createProxyInstance(com.mongodb.session.ClientSession session, T target, Class<T> targetType) {

			ProxyFactory factory = new ProxyFactory();
			factory.setTarget(target);
			factory.setInterfaces(targetType);
			factory.setOpaque(true);

			factory.addAdvice(new SessionAwareMethodInterceptor<>(session, target, ClientSession.class, MongoDatabase.class,
					this::proxyDatabase, MongoCollection.class, this::proxyCollection));

			return targetType.cast(factory.getProxy(target.getClass().getClassLoader()));
		}

	}

}
