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
package org.springframework.data.mongodb;

import org.springframework.lang.Nullable;
import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;

/**
 * Helper class for managing a {@link MongoDatabase} instances via {@link MongoDbFactory}. Used for obtaining
 * {@link ClientSession session bound} resources, such as {@link MongoDatabase} and
 * {@link com.mongodb.client.MongoCollection} suitable for transactional usage.
 * <p />
 * <strong>Note:</strong> Intended for internal usage only.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @currentRead Shadow's Edge - Brent Weeks
 * @since 2.1
 */
public class MongoDatabaseUtils {

	/**
	 * Obtain the default {@link MongoDatabase database} form the given {@link MongoDbFactory factory} using
	 * {@link SessionSynchronization#ON_ACTUAL_TRANSACTION native session synchronization}.
	 * <p />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the current
	 * {@link Thread} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 *
	 * @param factory the {@link MongoDbFactory} to get the {@link MongoDatabase} from.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static MongoDatabase getDatabase(MongoDbFactory factory) {
		return doGetMongoDatabase(null, factory, SessionSynchronization.ON_ACTUAL_TRANSACTION);
	}

	/**
	 * Obtain the default {@link MongoDatabase database} form the given {@link MongoDbFactory factory}.
	 * <p />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the current
	 * {@link Thread} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 *
	 * @param factory the {@link MongoDbFactory} to get the {@link MongoDatabase} from.
	 * @param sessionSynchronization the synchronization to use. Must not be {@literal null}.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static MongoDatabase getDatabase(MongoDbFactory factory, SessionSynchronization sessionSynchronization) {
		return doGetMongoDatabase(null, factory, sessionSynchronization);
	}

	/**
	 * Obtain the {@link MongoDatabase database} with given name form the given {@link MongoDbFactory factory} using
	 * {@link SessionSynchronization#ON_ACTUAL_TRANSACTION native session synchronization}.
	 * <p />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the current
	 * {@link Thread} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 *
	 * @param dbName the name of the {@link MongoDatabase} to get.
	 * @param factory the {@link MongoDbFactory} to get the {@link MongoDatabase} from.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static MongoDatabase getDatabase(String dbName, MongoDbFactory factory) {
		return doGetMongoDatabase(dbName, factory, SessionSynchronization.ON_ACTUAL_TRANSACTION);
	}

	/**
	 * Obtain the {@link MongoDatabase database} with given name form the given {@link MongoDbFactory factory}.
	 * <p />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the current
	 * {@link Thread} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 * 
	 * @param dbName the name of the {@link MongoDatabase} to get.
	 * @param factory the {@link MongoDbFactory} to get the {@link MongoDatabase} from.
	 * @param sessionSynchronization the synchronization to use. Must not be {@literal null}.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static MongoDatabase getDatabase(String dbName, MongoDbFactory factory,
			SessionSynchronization sessionSynchronization) {
		return doGetMongoDatabase(dbName, factory, sessionSynchronization);
	}

	private static MongoDatabase doGetMongoDatabase(@Nullable String dbName, MongoDbFactory factory,
			SessionSynchronization sessionSynchronization) {

		Assert.notNull(factory, "Factory must not be null!");

		if (!TransactionSynchronizationManager.isSynchronizationActive()) {
			return StringUtils.hasText(dbName) ? factory.getMongoDatabase(dbName) : factory.getMongoDatabase();
		}

		ClientSession session = doGetSession(factory, sessionSynchronization);

		if (session == null) {
			return StringUtils.hasText(dbName) ? factory.getMongoDatabase(dbName) : factory.getMongoDatabase();
		}

		MongoDbFactory factoryToUse = factory.withSession(session);
		return StringUtils.hasText(dbName) ? factoryToUse.getMongoDatabase(dbName) : factoryToUse.getMongoDatabase();
	}

	/**
	 * Check if the {@link MongoDbFactory} is actually bound to a {@link ClientSession} that has an active transaction, or
	 * if a {@link TransactionSynchronization} has been registered for the {@link MongoDbFactory resource} and if the
	 * associated {@link ClientSession} has an {@link ClientSession#hasActiveTransaction() active transaction}.
	 *
	 * @param dbFactory the resource to check transactions for. Must not be {@literal null}.
	 * @return {@literal true} if the factory has an ongoing transaction.
	 * @since 2.1.3
	 */
	public static boolean isTransactionActive(MongoDbFactory dbFactory) {

		if (dbFactory.isTransactionActive()) {
			return true;
		}

		MongoResourceHolder resourceHolder = (MongoResourceHolder) TransactionSynchronizationManager.getResource(dbFactory);
		return resourceHolder != null && resourceHolder.hasActiveTransaction();
	}

	@Nullable
	private static ClientSession doGetSession(MongoDbFactory dbFactory, SessionSynchronization sessionSynchronization) {

		MongoResourceHolder resourceHolder = (MongoResourceHolder) TransactionSynchronizationManager.getResource(dbFactory);

		// check for native MongoDB transaction
		if (resourceHolder != null && (resourceHolder.hasSession() || resourceHolder.isSynchronizedWithTransaction())) {

			if (!resourceHolder.hasSession()) {
				resourceHolder.setSession(createClientSession(dbFactory));
			}

			return resourceHolder.getSession();
		}

		if (SessionSynchronization.ON_ACTUAL_TRANSACTION.equals(sessionSynchronization)) {
			return null;
		}

		// init a non native MongoDB transaction by registering a MongoSessionSynchronization

		resourceHolder = new MongoResourceHolder(createClientSession(dbFactory), dbFactory);
		resourceHolder.getRequiredSession().startTransaction();

		TransactionSynchronizationManager
				.registerSynchronization(new MongoSessionSynchronization(resourceHolder, dbFactory));
		resourceHolder.setSynchronizedWithTransaction(true);
		TransactionSynchronizationManager.bindResource(dbFactory, resourceHolder);

		return resourceHolder.getSession();
	}

	private static ClientSession createClientSession(MongoDbFactory dbFactory) {
		return dbFactory.getSession(ClientSessionOptions.builder().causallyConsistent(true).build());
	}

	/**
	 * MongoDB specific {@link ResourceHolderSynchronization} for resource cleanup at the end of a transaction when
	 * participating in a non-native MongoDB transaction, such as a Jta or JDBC transaction.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	private static class MongoSessionSynchronization extends ResourceHolderSynchronization<MongoResourceHolder, Object> {

		private final MongoResourceHolder resourceHolder;

		MongoSessionSynchronization(MongoResourceHolder resourceHolder, MongoDbFactory dbFactory) {

			super(resourceHolder, dbFactory);
			this.resourceHolder = resourceHolder;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.support.ResourceHolderSynchronization#shouldReleaseBeforeCompletion()
		 */
		@Override
		protected boolean shouldReleaseBeforeCompletion() {
			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.support.ResourceHolderSynchronization#processResourceAfterCommit(java.lang.Object)
		 */
		@Override
		protected void processResourceAfterCommit(MongoResourceHolder resourceHolder) {

			if (resourceHolder.hasActiveTransaction()) {
				resourceHolder.getRequiredSession().commitTransaction();
			}
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.support.ResourceHolderSynchronization#afterCompletion(int)
		 */
		@Override
		public void afterCompletion(int status) {

			if (status == TransactionSynchronization.STATUS_ROLLED_BACK && this.resourceHolder.hasActiveTransaction()) {
				resourceHolder.getRequiredSession().abortTransaction();
			}

			super.afterCompletion(status);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.support.ResourceHolderSynchronization#releaseResource(java.lang.Object, java.lang.Object)
		 */
		@Override
		protected void releaseResource(MongoResourceHolder resourceHolder, Object resourceKey) {

			if (resourceHolder.hasActiveSession()) {
				resourceHolder.getRequiredSession().close();
			}
		}
	}
}
