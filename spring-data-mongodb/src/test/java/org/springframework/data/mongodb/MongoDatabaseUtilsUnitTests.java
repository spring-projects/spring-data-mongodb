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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import javax.transaction.Status;
import javax.transaction.UserTransaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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
@RunWith(MockitoJUnitRunner.class)
public class MongoDatabaseUtilsUnitTests {

	@Mock ClientSession session;
	@Mock ServerSession serverSession;
	@Mock MongoDbFactory dbFactory;
	@Mock MongoDatabase db;

	@Mock UserTransaction userTransaction;

	@Before
	public void setUp() {

		when(dbFactory.getSession(any())).thenReturn(session);

		when(dbFactory.withSession(session)).thenReturn(dbFactory);

		when(dbFactory.getMongoDatabase()).thenReturn(db);

		when(session.getServerSession()).thenReturn(serverSession);
		when(session.hasActiveTransaction()).thenReturn(true);

		when(serverSession.isClosed()).thenReturn(false);
	}

	@After
	public void verifyTransactionSynchronizationManagerState() {

		assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty()).isTrue();
		assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isFalse();
		assertThat(TransactionSynchronizationManager.getCurrentTransactionName()).isNull();
		assertThat(TransactionSynchronizationManager.isCurrentTransactionReadOnly()).isFalse();
		assertThat(TransactionSynchronizationManager.getCurrentTransactionIsolationLevel()).isNull();
		assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
	}

	@Test // DATAMONGO-2130
	public void isTransactionActiveShouldDetectTxViaFactory() {

		when(dbFactory.isTransactionActive()).thenReturn(true);

		assertThat(MongoDatabaseUtils.isTransactionActive(dbFactory)).isTrue();
	}

	@Test // DATAMONGO-2130
	public void isTransactionActiveShouldReturnFalseIfNoTxActive() {

		when(dbFactory.isTransactionActive()).thenReturn(false);

		assertThat(MongoDatabaseUtils.isTransactionActive(dbFactory)).isFalse();
	}

	@Test // DATAMONGO-2130
	public void isTransactionActiveShouldLookupTxForActiveTransactionSynchronizationViaTxManager() {

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
	public void shouldNotStartSessionWhenNoTransactionOngoing() {

		MongoDatabaseUtils.getDatabase(dbFactory, SessionSynchronization.ON_ACTUAL_TRANSACTION);

		verify(dbFactory, never()).getSession(any());
		verify(dbFactory, never()).withSession(any(ClientSession.class));
	}

	@Test // DATAMONGO-1920
	public void shouldParticipateInOngoingJtaTransactionWithCommitWhenSessionSychronizationIsAny() throws Exception {

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
	public void shouldParticipateInOngoingJtaTransactionWithRollbackWhenSessionSychronizationIsAny() throws Exception {

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
	public void shouldNotParticipateInOngoingJtaTransactionWithRollbackWhenSessionSychronizationIsNative()
			throws Exception {

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
	public void shouldParticipateInOngoingMongoTransactionWhenSessionSychronizationIsNative() {

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
	public void shouldParticipateInOngoingMongoTransactionWhenSessionSychronizationIsAny() {

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
