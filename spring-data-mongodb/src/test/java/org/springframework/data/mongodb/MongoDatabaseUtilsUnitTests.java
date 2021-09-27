/*
 * Copyright 2018-2021 the original author or authors.
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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import jakarta.transaction.Status;
import jakarta.transaction.UserTransaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.session.ServerSession;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class MongoDatabaseUtilsUnitTests {

	@Mock ClientSession session;
	@Mock ServerSession serverSession;
	@Mock MongoDatabaseFactory dbFactory;
	@Mock MongoDatabase db;

	@Mock UserTransaction userTransaction;

	@AfterEach
	void verifyTransactionSynchronizationManagerState() {

		assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty()).isTrue();
		assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isFalse();
		assertThat(TransactionSynchronizationManager.getCurrentTransactionName()).isNull();
		assertThat(TransactionSynchronizationManager.isCurrentTransactionReadOnly()).isFalse();
		assertThat(TransactionSynchronizationManager.getCurrentTransactionIsolationLevel()).isNull();
		assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
	}

	@Test // DATAMONGO-2130
	void isTransactionActiveShouldDetectTxViaFactory() {

		when(dbFactory.isTransactionActive()).thenReturn(true);

		assertThat(MongoDatabaseUtils.isTransactionActive(dbFactory)).isTrue();
	}

	@Test // DATAMONGO-2130
	void isTransactionActiveShouldReturnFalseIfNoTxActive() {

		when(dbFactory.isTransactionActive()).thenReturn(false);

		assertThat(MongoDatabaseUtils.isTransactionActive(dbFactory)).isFalse();
	}

	@Test // DATAMONGO-2130
	void isTransactionActiveShouldLookupTxForActiveTransactionSynchronizationViaTxManager() {

		when(dbFactory.getSession(any())).thenReturn(session);
		when(session.getServerSession()).thenReturn(serverSession);
		when(session.hasActiveTransaction()).thenReturn(true);
		when(serverSession.isClosed()).thenReturn(false);

		when(dbFactory.isTransactionActive()).thenReturn(false);

		MongoTransactionManager txManager = new MongoTransactionManager(dbFactory);
		TransactionTemplate txTemplate = new TransactionTemplate(txManager);

		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
				assertThat(MongoDatabaseUtils.isTransactionActive(dbFactory)).isTrue();
			}
		});
	}

	@Test // DATAMONGO-1920
	void shouldNotStartSessionWhenNoTransactionOngoing() {

		MongoDatabaseUtils.getDatabase(dbFactory, SessionSynchronization.ON_ACTUAL_TRANSACTION);

		verify(dbFactory, never()).getSession(any());
		verify(dbFactory, never()).withSession(any(ClientSession.class));
	}

	@Test // GH-3760
	void shouldJustReturnDatabaseIfSessionSynchronizationDisabled() throws Exception {

		when(dbFactory.getMongoDatabase()).thenReturn(db);

		JtaTransactionManager txManager = new JtaTransactionManager(userTransaction);
		TransactionTemplate txTemplate = new TransactionTemplate(txManager);

		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {

				MongoDatabaseUtils.getDatabase(dbFactory, SessionSynchronization.NEVER);

				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isFalse();
			}
		});

		verify(userTransaction).getStatus();
		verifyNoMoreInteractions(userTransaction);
		verifyNoInteractions(session);
	}

	@Test // DATAMONGO-1920
	void shouldParticipateInOngoingJtaTransactionWithCommitWhenSessionSychronizationIsAny() throws Exception {

		when(dbFactory.getSession(any())).thenReturn(session);
		when(dbFactory.withSession(session)).thenReturn(dbFactory);
		when(dbFactory.getMongoDatabase()).thenReturn(db);
		when(session.getServerSession()).thenReturn(serverSession);
		when(session.hasActiveTransaction()).thenReturn(true);
		when(serverSession.isClosed()).thenReturn(false);

		when(userTransaction.getStatus()).thenReturn(Status.STATUS_NO_TRANSACTION, Status.STATUS_ACTIVE,
				Status.STATUS_ACTIVE);

		JtaTransactionManager txManager = new JtaTransactionManager(userTransaction);
		TransactionTemplate txTemplate = new TransactionTemplate(txManager);

		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {

				assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isTrue();
				assertThat(transactionStatus.isNewTransaction()).isTrue();
				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isFalse();

				MongoDatabaseUtils.getDatabase(dbFactory, SessionSynchronization.ALWAYS);

				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isTrue();
			}
		});

		verify(userTransaction).begin();

		verify(session).startTransaction();
		verify(session).commitTransaction();
		verify(session).close();
	}

	@Test // DATAMONGO-1920
	void shouldParticipateInOngoingJtaTransactionWithRollbackWhenSessionSychronizationIsAny() throws Exception {

		when(dbFactory.getSession(any())).thenReturn(session);
		when(dbFactory.withSession(session)).thenReturn(dbFactory);
		when(dbFactory.getMongoDatabase()).thenReturn(db);
		when(session.getServerSession()).thenReturn(serverSession);
		when(session.hasActiveTransaction()).thenReturn(true);
		when(serverSession.isClosed()).thenReturn(false);

		when(userTransaction.getStatus()).thenReturn(Status.STATUS_NO_TRANSACTION, Status.STATUS_ACTIVE,
				Status.STATUS_ACTIVE);

		JtaTransactionManager txManager = new JtaTransactionManager(userTransaction);
		TransactionTemplate txTemplate = new TransactionTemplate(txManager);

		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {

				assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isTrue();
				assertThat(transactionStatus.isNewTransaction()).isTrue();
				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isFalse();

				MongoDatabaseUtils.getDatabase(dbFactory, SessionSynchronization.ALWAYS);

				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isTrue();

				transactionStatus.setRollbackOnly();
			}
		});

		verify(userTransaction).rollback();

		verify(session).startTransaction();
		verify(session).abortTransaction();
		verify(session).close();
	}

	@Test // DATAMONGO-1920
	void shouldNotParticipateInOngoingJtaTransactionWithRollbackWhenSessionSychronizationIsNative() throws Exception {

		when(userTransaction.getStatus()).thenReturn(Status.STATUS_NO_TRANSACTION, Status.STATUS_ACTIVE,
				Status.STATUS_ACTIVE);

		JtaTransactionManager txManager = new JtaTransactionManager(userTransaction);
		TransactionTemplate txTemplate = new TransactionTemplate(txManager);

		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {

				assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isTrue();
				assertThat(transactionStatus.isNewTransaction()).isTrue();
				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isFalse();

				MongoDatabaseUtils.getDatabase(dbFactory, SessionSynchronization.ON_ACTUAL_TRANSACTION);

				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isFalse();

				transactionStatus.setRollbackOnly();
			}
		});

		verify(userTransaction).rollback();

		verify(session, never()).startTransaction();
		verify(session, never()).abortTransaction();
		verify(session, never()).close();
	}

	@Test // DATAMONGO-1920
	void shouldParticipateInOngoingMongoTransactionWhenSessionSychronizationIsNative() {

		when(dbFactory.getSession(any())).thenReturn(session);
		when(dbFactory.withSession(session)).thenReturn(dbFactory);
		when(dbFactory.getMongoDatabase()).thenReturn(db);
		when(session.getServerSession()).thenReturn(serverSession);
		when(serverSession.isClosed()).thenReturn(false);

		MongoTransactionManager txManager = new MongoTransactionManager(dbFactory);
		TransactionTemplate txTemplate = new TransactionTemplate(txManager);

		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {

				assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isTrue();
				assertThat(transactionStatus.isNewTransaction()).isTrue();
				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isTrue();

				MongoDatabaseUtils.getDatabase(dbFactory, SessionSynchronization.ON_ACTUAL_TRANSACTION);

				transactionStatus.setRollbackOnly();
			}
		});

		verify(session).startTransaction();
		verify(session).abortTransaction();
		verify(session).close();
	}

	@Test // DATAMONGO-1920
	void shouldParticipateInOngoingMongoTransactionWhenSessionSynchronizationIsAny() {

		when(dbFactory.getSession(any())).thenReturn(session);
		when(dbFactory.withSession(session)).thenReturn(dbFactory);
		when(dbFactory.getMongoDatabase()).thenReturn(db);
		when(session.getServerSession()).thenReturn(serverSession);
		when(serverSession.isClosed()).thenReturn(false);

		MongoTransactionManager txManager = new MongoTransactionManager(dbFactory);
		TransactionTemplate txTemplate = new TransactionTemplate(txManager);

		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {

				assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isTrue();
				assertThat(transactionStatus.isNewTransaction()).isTrue();
				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isTrue();

				MongoDatabaseUtils.getDatabase(dbFactory, SessionSynchronization.ALWAYS);

				transactionStatus.setRollbackOnly();
			}
		});

		verify(session).startTransaction();
		verify(session).abortTransaction();
		verify(session).close();
	}
}
