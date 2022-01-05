/*
 * Copyright 2019-2021 the original author or authors.
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
import reactor.util.context.Context;

import org.springframework.lang.Nullable;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.reactive.ReactiveResourceSynchronization;
import org.springframework.transaction.reactive.TransactionSynchronization;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Helper class for managing reactive {@link MongoDatabase} instances via {@link ReactiveMongoDatabaseFactory}. Used for
 * obtaining {@link ClientSession session bound} resources, such as {@link MongoDatabase} and {@link MongoCollection}
 * suitable for transactional usage.
 * <br />
 * <strong>Note:</strong> Intended for internal usage only.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Mathieu Ouellet
 * @since 2.2
 */
public class ReactiveMongoDatabaseUtils {

	/**
	 * Check if the {@link ReactiveMongoDatabaseFactory} is actually bound to a
	 * {@link com.mongodb.reactivestreams.client.ClientSession} that has an active transaction, or if a
	 * {@link org.springframework.transaction.reactive.TransactionSynchronization} has been registered for the
	 * {@link ReactiveMongoDatabaseFactory resource} and if the associated
	 * {@link com.mongodb.reactivestreams.client.ClientSession} has an
	 * {@link com.mongodb.reactivestreams.client.ClientSession#hasActiveTransaction() active transaction}.
	 *
	 * @param databaseFactory the resource to check transactions for. Must not be {@literal null}.
	 * @return a {@link Mono} emitting {@literal true} if the factory has an ongoing transaction.
	 */
	public static Mono<Boolean> isTransactionActive(ReactiveMongoDatabaseFactory databaseFactory) {

		if (databaseFactory.isTransactionActive()) {
			return Mono.just(true);
		}

		return TransactionSynchronizationManager.forCurrentTransaction() //
				.map(it -> {

					ReactiveMongoResourceHolder holder = (ReactiveMongoResourceHolder) it.getResource(databaseFactory);
					return holder != null && holder.hasActiveTransaction();
				}) //
				.onErrorResume(NoTransactionException.class, e -> Mono.just(false));
	}

	/**
	 * Obtain the default {@link MongoDatabase database} form the given {@link ReactiveMongoDatabaseFactory factory} using
	 * {@link SessionSynchronization#ON_ACTUAL_TRANSACTION native session synchronization}.
	 * <br />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the subscriber
	 * {@link Context} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 *
	 * @param factory the {@link ReactiveMongoDatabaseFactory} to get the {@link MongoDatabase} from.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static Mono<MongoDatabase> getDatabase(ReactiveMongoDatabaseFactory factory) {
		return doGetMongoDatabase(null, factory, SessionSynchronization.ON_ACTUAL_TRANSACTION);
	}

	/**
	 * Obtain the default {@link MongoDatabase database} form the given {@link ReactiveMongoDatabaseFactory factory}.
	 * <br />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the subscriber
	 * {@link Context} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 *
	 * @param factory the {@link ReactiveMongoDatabaseFactory} to get the {@link MongoDatabase} from.
	 * @param sessionSynchronization the synchronization to use. Must not be {@literal null}.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static Mono<MongoDatabase> getDatabase(ReactiveMongoDatabaseFactory factory,
			SessionSynchronization sessionSynchronization) {
		return doGetMongoDatabase(null, factory, sessionSynchronization);
	}

	/**
	 * Obtain the {@link MongoDatabase database} with given name form the given {@link ReactiveMongoDatabaseFactory
	 * factory} using {@link SessionSynchronization#ON_ACTUAL_TRANSACTION native session synchronization}.
	 * <br />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the subscriber
	 * {@link Context} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 *
	 * @param dbName the name of the {@link MongoDatabase} to get.
	 * @param factory the {@link ReactiveMongoDatabaseFactory} to get the {@link MongoDatabase} from.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static Mono<MongoDatabase> getDatabase(String dbName, ReactiveMongoDatabaseFactory factory) {
		return doGetMongoDatabase(dbName, factory, SessionSynchronization.ON_ACTUAL_TRANSACTION);
	}

	/**
	 * Obtain the {@link MongoDatabase database} with given name form the given {@link ReactiveMongoDatabaseFactory
	 * factory}.
	 * <br />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the subscriber
	 * {@link Context} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 *
	 * @param dbName the name of the {@link MongoDatabase} to get.
	 * @param factory the {@link ReactiveMongoDatabaseFactory} to get the {@link MongoDatabase} from.
	 * @param sessionSynchronization the synchronization to use. Must not be {@literal null}.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static Mono<MongoDatabase> getDatabase(String dbName, ReactiveMongoDatabaseFactory factory,
			SessionSynchronization sessionSynchronization) {
		return doGetMongoDatabase(dbName, factory, sessionSynchronization);
	}

	private static Mono<MongoDatabase> doGetMongoDatabase(@Nullable String dbName, ReactiveMongoDatabaseFactory factory,
			SessionSynchronization sessionSynchronization) {

		Assert.notNull(factory, "DatabaseFactory must not be null!");

		if (sessionSynchronization == SessionSynchronization.NEVER) {
			return getMongoDatabaseOrDefault(dbName, factory);
		}

		return TransactionSynchronizationManager.forCurrentTransaction()
				.filter(TransactionSynchronizationManager::isSynchronizationActive) //
				.flatMap(synchronizationManager -> {

					return doGetSession(synchronizationManager, factory, sessionSynchronization) //
							.flatMap(it -> getMongoDatabaseOrDefault(dbName, factory.withSession(it)));
				}) //
				.onErrorResume(NoTransactionException.class, e -> getMongoDatabaseOrDefault(dbName, factory))
				.switchIfEmpty(getMongoDatabaseOrDefault(dbName, factory));
	}

	private static Mono<MongoDatabase> getMongoDatabaseOrDefault(@Nullable String dbName,
			ReactiveMongoDatabaseFactory factory) {
		return StringUtils.hasText(dbName) ? factory.getMongoDatabase(dbName) : factory.getMongoDatabase();
	}

	private static Mono<ClientSession> doGetSession(TransactionSynchronizationManager synchronizationManager,
			ReactiveMongoDatabaseFactory dbFactory, SessionSynchronization sessionSynchronization) {

		final ReactiveMongoResourceHolder registeredHolder = (ReactiveMongoResourceHolder) synchronizationManager
				.getResource(dbFactory);

		// check for native MongoDB transaction
		if (registeredHolder != null
				&& (registeredHolder.hasSession() || registeredHolder.isSynchronizedWithTransaction())) {

			return registeredHolder.hasSession() ? Mono.just(registeredHolder.getSession())
					: createClientSession(dbFactory).map(registeredHolder::setSessionIfAbsent);
		}

		if (SessionSynchronization.ON_ACTUAL_TRANSACTION.equals(sessionSynchronization)) {
			return Mono.empty();
		}

		// init a non native MongoDB transaction by registering a MongoSessionSynchronization
		return createClientSession(dbFactory).map(session -> {

			ReactiveMongoResourceHolder newHolder = new ReactiveMongoResourceHolder(session, dbFactory);
			newHolder.getRequiredSession().startTransaction();

			synchronizationManager
					.registerSynchronization(new MongoSessionSynchronization(synchronizationManager, newHolder, dbFactory));
			newHolder.setSynchronizedWithTransaction(true);
			synchronizationManager.bindResource(dbFactory, newHolder);

			return newHolder.getSession();
		});
	}

	private static Mono<ClientSession> createClientSession(ReactiveMongoDatabaseFactory dbFactory) {
		return dbFactory.getSession(ClientSessionOptions.builder().causallyConsistent(true).build());
	}

	/**
	 * MongoDB specific {@link ResourceHolderSynchronization} for resource cleanup at the end of a transaction when
	 * participating in a non-native MongoDB transaction, such as a R2CBC transaction.
	 *
	 * @author Mark Paluch
	 * @since 2.2
	 */
	private static class MongoSessionSynchronization
			extends ReactiveResourceSynchronization<ReactiveMongoResourceHolder, Object> {

		private final ReactiveMongoResourceHolder resourceHolder;

		MongoSessionSynchronization(TransactionSynchronizationManager synchronizationManager,
				ReactiveMongoResourceHolder resourceHolder, ReactiveMongoDatabaseFactory dbFactory) {

			super(resourceHolder, dbFactory, synchronizationManager);
			this.resourceHolder = resourceHolder;
		}

		@Override
		protected boolean shouldReleaseBeforeCompletion() {
			return false;
		}

		@Override
		protected Mono<Void> processResourceAfterCommit(ReactiveMongoResourceHolder resourceHolder) {

			if (isTransactionActive(resourceHolder)) {
				return Mono.from(resourceHolder.getRequiredSession().commitTransaction());
			}

			return Mono.empty();
		}

		@Override
		public Mono<Void> afterCompletion(int status) {

			return Mono.defer(() -> {

				if (status == TransactionSynchronization.STATUS_ROLLED_BACK && isTransactionActive(this.resourceHolder)) {

					return Mono.from(resourceHolder.getRequiredSession().abortTransaction()) //
							.then(super.afterCompletion(status));
				}

				return super.afterCompletion(status);
			});
		}

		@Override
		protected Mono<Void> releaseResource(ReactiveMongoResourceHolder resourceHolder, Object resourceKey) {

			return Mono.fromRunnable(() -> {
				if (resourceHolder.hasActiveSession()) {
					resourceHolder.getRequiredSession().close();
				}
			});
		}

		private boolean isTransactionActive(ReactiveMongoResourceHolder resourceHolder) {

			if (!resourceHolder.hasSession()) {
				return false;
			}

			return resourceHolder.getRequiredSession().hasActiveTransaction();
		}
	}
}
