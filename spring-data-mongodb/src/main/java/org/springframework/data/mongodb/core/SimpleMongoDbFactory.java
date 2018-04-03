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
package org.springframework.data.mongodb.core;

import lombok.Value;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.SessionAwareMethodInterceptor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.ClientSessionOptions;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Factory to create {@link MongoDatabase} instances from a {@link MongoClient} instance.
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author George Moraitis
 * @author Mark Paluch
 */
public class SimpleMongoDbFactory implements DisposableBean, MongoDbFactory {

	private final MongoClient mongoClient;
	private final String databaseName;
	private final boolean mongoInstanceCreated;
	private final PersistenceExceptionTranslator exceptionTranslator;

	private @Nullable WriteConcern writeConcern;

	/**
	 * Creates a new {@link SimpleMongoDbFactory} instance from the given {@link MongoClientURI}.
	 *
	 * @param uri must not be {@literal null}.
	 * @since 1.7
	 */
	public SimpleMongoDbFactory(MongoClientURI uri) {
		this(new MongoClient(uri), uri.getDatabase(), true);
	}

	/**
	 * Creates a new {@link SimpleMongoDbFactory} instance from the given {@link MongoClient}.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null}.
	 * @since 1.7
	 */
	public SimpleMongoDbFactory(MongoClient mongoClient, String databaseName) {
		this(mongoClient, databaseName, false);
	}

	/**
	 * @param mongoClient
	 * @param databaseName
	 * @param mongoInstanceCreated
	 * @since 1.7
	 */
	private SimpleMongoDbFactory(MongoClient mongoClient, String databaseName, boolean mongoInstanceCreated) {

		Assert.notNull(mongoClient, "MongoClient must not be null!");
		Assert.hasText(databaseName, "Database name must not be empty!");
		Assert.isTrue(databaseName.matches("[^/\\\\.$\"\\s]+"),
				"Database name must not contain slashes, dots, spaces, quotes, or dollar signs!");

		this.mongoClient = mongoClient;
		this.databaseName = databaseName;
		this.mongoInstanceCreated = mongoInstanceCreated;
		this.exceptionTranslator = new MongoExceptionTranslator();
	}

	/**
	 * Configures the {@link WriteConcern} to be used on the {@link DB} instance being created.
	 *
	 * @param writeConcern the writeConcern to set
	 */
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getDb()
	 */
	public MongoDatabase getDb() throws DataAccessException {
		return getDb(databaseName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getDb(java.lang.String)
	 */
	public MongoDatabase getDb(String dbName) throws DataAccessException {

		Assert.hasText(dbName, "Database name must not be empty.");

		MongoDatabase db = mongoClient.getDatabase(dbName);

		if (writeConcern == null) {
			return db;
		}

		return db.withWriteConcern(writeConcern);
	}

	/**
	 * Clean up the Mongo instance if it was created by the factory itself.
	 *
	 * @see DisposableBean#destroy()
	 */
	public void destroy() throws Exception {
		if (mongoInstanceCreated) {
			mongoClient.close();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getExceptionTranslator()
	 */
	@Override
	public PersistenceExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	@SuppressWarnings("deprecation")
	@Override
	public DB getLegacyDb() {
		return mongoClient.getDB(databaseName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#getSession(com.mongodb.ClientSessionOptions)
	 */
	@Override
	public ClientSession getSession(ClientSessionOptions options) {
		return mongoClient.startSession(options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.MongoDbFactory#withSession(com.mongodb.session.ClientSession)
	 */
	@Override
	public MongoDbFactory withSession(ClientSession session) {
		return new ClientSessionBoundMongoDbFactory(session, this);
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
		 * @see org.springframework.data.mongodb.MongoDbFactory#getDb()
		 */
		@Override
		public MongoDatabase getDb() throws DataAccessException {
			return proxyMongoDatabase(delegate.getDb());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.MongoDbFactory#getDb(java.lang.String)
		 */
		@Override
		public MongoDatabase getDb(String dbName) throws DataAccessException {
			return proxyMongoDatabase(delegate.getDb(dbName));
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
		 * @see org.springframework.data.mongodb.MongoDbFactory#getLegacyDb()
		 */
		@Override
		public DB getLegacyDb() {
			return delegate.getLegacyDb();
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

		private MongoDatabase proxyMongoDatabase(MongoDatabase database) {
			return createProxyInstance(session, database, MongoDatabase.class);
		}

		private MongoDatabase proxyDatabase(com.mongodb.session.ClientSession session, MongoDatabase database) {
			return createProxyInstance(session, database, MongoDatabase.class);
		}

		private MongoCollection proxyCollection(com.mongodb.session.ClientSession session, MongoCollection collection) {
			return createProxyInstance(session, collection, MongoCollection.class);
		}

		private <T> T createProxyInstance(com.mongodb.session.ClientSession session, T target, Class<T> targetType) {

			ProxyFactory factory = new ProxyFactory();
			factory.setTarget(target);
			factory.setInterfaces(targetType);
			factory.setOpaque(true);

			factory.addAdvice(new SessionAwareMethodInterceptor<>(session, target, ClientSession.class, MongoDatabase.class, this::proxyDatabase,
					MongoCollection.class, this::proxyCollection));

			return targetType.cast(factory.getProxy());
		}
	}
}
