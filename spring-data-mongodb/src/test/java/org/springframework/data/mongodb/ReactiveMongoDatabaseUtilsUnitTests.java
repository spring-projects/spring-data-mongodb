/*
 * Copyright 2019-2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.session.ServerSession;

/**
 * Unit tests for {@link ReactiveMongoDatabaseUtils}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveMongoDatabaseUtilsUnitTests {

	@Mock ClientSession session;
	@Mock ServerSession serverSession;
	@Mock ReactiveMongoDatabaseFactory databaseFactory;
	@Mock MongoDatabase db;

	@Before
	public void setUp() {

		when(databaseFactory.getSession(any())).thenReturn(Mono.just(session));
		when(databaseFactory.getMongoDatabase()).thenReturn(db);

		when(session.getServerSession()).thenReturn(serverSession);
		when(session.hasActiveTransaction()).thenReturn(true);
	}

	@Test // DATAMONGO-2265
	public void isTransactionActiveShouldDetectTxViaFactory() {

		when(databaseFactory.isTransactionActive()).thenReturn(true);

		ReactiveMongoDatabaseUtils.isTransactionActive(databaseFactory) //
				.as(StepVerifier::create) //
				.expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void isTransactionActiveShouldReturnFalseIfNoTxActive() {

		when(databaseFactory.isTransactionActive()).thenReturn(false);

		ReactiveMongoDatabaseUtils.isTransactionActive(databaseFactory) //
				.as(StepVerifier::create) //
				.expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void isTransactionActiveShouldLookupTxForActiveTransactionSynchronizationViaTxManager() {

		when(databaseFactory.isTransactionActive()).thenReturn(false);
		when(session.commitTransaction()).thenReturn(Mono.empty());

		ReactiveMongoTransactionManager txManager = new ReactiveMongoTransactionManager(databaseFactory);
		TransactionalOperator operator = TransactionalOperator.create(txManager, new DefaultTransactionDefinition());

		operator.execute(tx -> {

			return ReactiveMongoDatabaseUtils.isTransactionActive(databaseFactory);
		}).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void shouldNotStartSessionWhenNoTransactionOngoing() {

		ReactiveMongoDatabaseUtils.getDatabase(databaseFactory, SessionSynchronization.ON_ACTUAL_TRANSACTION) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		verify(databaseFactory, never()).getSession(any());
		verify(databaseFactory, never()).withSession(any(ClientSession.class));
	}

	@Test // DATAMONGO-2265
	public void shouldParticipateInOngoingMongoTransactionWhenSessionSychronizationIsNative() {

		ReactiveMongoTransactionManager txManager = new ReactiveMongoTransactionManager(databaseFactory);
		when(session.abortTransaction()).thenReturn(Mono.empty());

		TransactionalOperator operator = TransactionalOperator.create(txManager, new DefaultTransactionDefinition());

		operator.execute(tx -> {

			return TransactionSynchronizationManager.forCurrentTransaction().doOnNext(synchronizationManager -> {

				assertThat(synchronizationManager.isSynchronizationActive()).isTrue();
				assertThat(tx.isNewTransaction()).isTrue();

				assertThat(synchronizationManager.hasResource(databaseFactory)).isTrue();

			}).then(Mono.fromRunnable(tx::setRollbackOnly));
		}).as(StepVerifier::create).verifyComplete();

		verify(session).startTransaction();
		verify(session).abortTransaction();
		verify(session).close();
	}
}
