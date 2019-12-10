/*
 * Copyright 2018-2019 the original author or authors.
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

import lombok.Value;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.SessionAwareMethodInterceptor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.ClientSessionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Common base class for usage with both {@link com.mongodb.client.MongoClients} defining common properties such as
 * database name and exception translator.
 * <p/>
 * Not intended to be used directly.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <C> Client type.
 * @since 2.1
 * @see SimpleMongoClientDbFactory
 */
public abstract class MongoDbFactorySupport<C> implements MongoDbFactory {

	private final C mongoClient;
	private final String databaseName;
	private final boolean mongoInstanceCreated;
	private final PersistenceExceptionTranslator exceptionTranslator;

	private @Nullable WriteConcern writeConcern;

	/**
	 * Create a new {@link MongoDbFactorySupport} object given {@code mongoClient}, {@code databaseName},
	 * {@code mongoInstanceCreated} and {@link PersistenceExceptionTranslator}.
	 * 
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 * @param mongoInstanceCreated {@literal true} if the client instance was created by a subclass of
	 *          {@link MongoDbFactorySupport} to close the client on {@link #destroy()}.
	 * @param exceptionTranslator must not be {@literal null}.
	 */
	protected MongoDbFactorySupport(C mongoClient, String databaseName, boolean mongoInstanceCreated,
			PersistenceExceptionTranslator exceptionTranslator) {

		Assert.notNull(mongoClient, "MongoClient must not be null!");
		Assert.hasText(databaseName, "Database name must not be empty!");
		Assert.isTrue(databaseName.matches("[^/\\\\.$\"\\s]+"),
				"Database name must not contain slashes, dots, spaces, quotes, or dollar signs!");

		this.mongoClient = mongoClient;
		this.databaseName = databaseName;
		this.mongoInstanceCreated = mongoInstanceCreated;
		this.exceptionTranslator = exceptionTranslator;
	}

	/**
	 * Configures the {@link WriteConcern} to be used on the {@link MongoDatabase} instance being created.
	 *
	 * @param writeConcern the writeConcern to set
	 */
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getMongoDatabase()
	 */
	public MongoDatabase getMongoDatabase() throws DataAccessException {
		return getMongoDatabase(getDefaultDatabaseName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getMongoDatabase(java.lang.String)
	 */
	@Override
	public MongoDatabase getMongoDatabase(String dbName) throws DataAccessException {

		Assert.hasText(dbName, "Database name must not be empty!");

		MongoDatabase db = doGetMongoDatabase(dbName);

		if (writeConcern == null) {
			return db;
		}

		return db.withWriteConcern(writeConcern);
	}

	/**
	 * Get the actual {@link MongoDatabase} from the client.
	 * 
	 * @param dbName must not be {@literal null} or empty.
	 * @return
	 */
	protected abstract MongoDatabase doGetMongoDatabase(String dbName);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() throws Exception {
		if (mongoInstanceCreated) {
			closeClient();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getExceptionTranslator()
	 */
	public PersistenceExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#withSession(com.mongodb.session.Session)
	 */
	public MongoDbFactory withSession(ClientSession session) {
		return new MongoDbFactorySupport.ClientSessionBoundMongoDbFactory(session, this);
	}

	/**
	 * Close the client instance.
	 */
	protected abstract void closeClient();

	/**
	 * @return the Mongo client object.
	 */
	protected C getMongoClient() {
		return mongoClient;
	}

	/**
	 * @return the database name.
	 */
	protected String getDefaultDatabaseName() {
		return databaseName;
	}

	/**
	 * {@link ClientSession} bound {@link MongoDbFactory} decorating the database with a
	 * {@link SessionAwareMethodInterceptor}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	@Value
	static class ClientSessionBoundMongoDbFactory implements MongoDbFactory {

		ClientSession session;
		MongoDbFactory delegate;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.MongoDbFactory#getMongoDatabase()
		 */
		@Override
		public MongoDatabase getMongoDatabase() throws DataAccessException {
			return proxyMongoDatabase(delegate.getMongoDatabase());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.MongoDbFactory#getMongoDatabase(java.lang.String)
		 */
		@Override
		public MongoDatabase getMongoDatabase(String dbName) throws DataAccessException {
			return proxyMongoDatabase(delegate.getMongoDatabase(dbName));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.MongoDbFactory#getExceptionTranslator()
		 */
		@Override
		public PersistenceExceptionTranslator getExceptionTranslator() {
			return delegate.getExceptionTranslator();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.MongoDbFactory#getSession(com.mongodb.ClientSessionOptions)
		 */
		@Override
		public ClientSession getSession(ClientSessionOptions options) {
			return delegate.getSession(options);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.MongoDbFactory#withSession(com.mongodb.session.ClientSession)
		 */
		@Override
		public MongoDbFactory withSession(ClientSession session) {
			return delegate.withSession(session);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.MongoDbFactory#isTransactionActive()
		 */
		@Override
		public boolean isTransactionActive() {
			return session != null && session.hasActiveTransaction();
		}

		private MongoDatabase proxyMongoDatabase(MongoDatabase database) {
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

			return targetType.cast(factory.getProxy());
		}
	}

}
