/*
 * Copyright 2018-2023 the original author or authors.
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
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.session.ServerSession;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MongoTransactionManagerUnitTests {

	@Mock ClientSession session;
	@Mock ClientSession session2;
	@Mock ServerSession serverSession;
	@Mock MongoDatabaseFactory dbFactory;
	@Mock MongoDatabaseFactory dbFactory2;
	@Mock MongoDatabase db;
	@Mock MongoDatabase db2;

	@BeforeEach
	void setUp() {

		when(dbFactory.getSession(any())).thenReturn(session, session2);
		when(dbFactory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());
		when(dbFactory2.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());
		when(dbFactory.withSession(session)).thenReturn(dbFactory);
		when(dbFactory.getMongoDatabase()).thenReturn(db);
		when(session.getServerSession()).thenReturn(serverSession);
	}

	@AfterEach
	void verifyTransactionSynchronizationManager() {

		assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty()).isTrue();
		assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isFalse();
	}

	@Test // DATAMONGO-1920
	void triggerCommitCorrectly() {

		MongoTransactionManager txManager = new MongoTransactionManager(dbFactory);
		TransactionStatus txStatus = txManager.getTransaction(new DefaultTransactionDefinition());

		MongoTemplate template = new MongoTemplate(dbFactory);

		template.execute(db -> {
			db.drop();
			return null;
		});

		verify(dbFactory).withSession(eq(session));

		txManager.commit(txStatus);

		verify(session).startTransaction();
		verify(session).commitTransaction();
		verify(session).close();
	}

	@Test // DATAMONGO-1920
	void participateInOnGoingTransactionWithCommit() {

		MongoTransactionManager txManager = new MongoTransactionManager(dbFactory);
		TransactionStatus txStatus = txManager.getTransaction(new DefaultTransactionDefinition());

		MongoTemplate template = new MongoTemplate(dbFactory);

		template.execute(db -> {
			db.drop();
			return null;
		});

		TransactionTemplate txTemplate = new TransactionTemplate(txManager);
		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {

				template.execute(db -> {
					db.drop();
					return null;
				});
			}
		});

		verify(dbFactory, times(2)).withSession(eq(session));

		txManager.commit(txStatus);

		verify(session).startTransaction();
		verify(session).commitTransaction();
		verify(session).close();
	}

	@Test // DATAMONGO-1920
	void participateInOnGoingTransactionWithRollbackOnly() {

		MongoTransactionManager txManager = new MongoTransactionManager(dbFactory);
		TransactionStatus txStatus = txManager.getTransaction(new DefaultTransactionDefinition());

		MongoTemplate template = new MongoTemplate(dbFactory);

		template.execute(db -> {
			db.drop();
			return null;
		});

		TransactionTemplate txTemplate = new TransactionTemplate(txManager);
		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {

				template.execute(db -> {
					db.drop();
					return null;
				});

				status.setRollbackOnly();
			}
		});

		verify(dbFactory, times(2)).withSession(eq(session));

		assertThatExceptionOfType(UnexpectedRollbackException.class).isThrownBy(() -> txManager.commit(txStatus));

		verify(session).startTransaction();
		verify(session).abortTransaction();
		verify(session).close();
	}

	@Test // DATAMONGO-1920
	void triggerRollbackCorrectly() {

		MongoTransactionManager txManager = new MongoTransactionManager(dbFactory);
		TransactionStatus txStatus = txManager.getTransaction(new DefaultTransactionDefinition());

		MongoTemplate template = new MongoTemplate(dbFactory);

		template.execute(db -> {
			db.drop();
			return null;
		});

		verify(dbFactory).withSession(eq(session));

		txManager.rollback(txStatus);

		verify(session).startTransaction();
		verify(session).abortTransaction();
		verify(session).close();
	}

	@Test // DATAMONGO-1920
	void suspendTransactionWhilePropagationNotSupported() {

		MongoTransactionManager txManager = new MongoTransactionManager(dbFactory);
		TransactionStatus txStatus = txManager.getTransaction(new DefaultTransactionDefinition());

		MongoTemplate template = new MongoTemplate(dbFactory);

		template.execute(db -> {
			db.drop();
			return null;
		});

		TransactionTemplate txTemplate = new TransactionTemplate(txManager);
		txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NOT_SUPPORTED);
		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {

				template.execute(db -> {
					db.drop();
					return null;
				});
			}
		});

		template.execute(MongoDatabase::listCollections);
		txManager.commit(txStatus);

		verify(session).startTransaction();
		verify(session2, never()).startTransaction();

		verify(dbFactory, times(2)).withSession(eq(session));
		verify(dbFactory, never()).withSession(eq(session2));

		verify(db, times(2)).drop();
		verify(db).listCollections();

		verify(session).close();
		verify(session2, never()).close();
	}

	@Test // DATAMONGO-1920
	void suspendTransactionWhilePropagationRequiresNew() {

		when(dbFactory.withSession(session2)).thenReturn(dbFactory2);
		when(dbFactory2.getMongoDatabase()).thenReturn(db2);
		when(session2.getServerSession()).thenReturn(serverSession);
		when(serverSession.isClosed()).thenReturn(false);

		MongoTransactionManager txManager = new MongoTransactionManager(dbFactory);
		TransactionStatus txStatus = txManager.getTransaction(new DefaultTransactionDefinition());

		MongoTemplate template = new MongoTemplate(dbFactory);

		template.execute(db -> {
			db.drop();
			return null;
		});

		TransactionTemplate txTemplate = new TransactionTemplate(txManager);
		txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {

				template.execute(db -> {
					db.drop();
					return null;
				});
			}
		});

		template.execute(MongoDatabase::listCollections);
		txManager.commit(txStatus);

		verify(session).startTransaction();
		verify(session2).startTransaction();

		verify(dbFactory, times(2)).withSession(eq(session));
		verify(dbFactory).withSession(eq(session2));

		verify(db).drop();
		verify(db2).drop();
		verify(db).listCollections();

		verify(session).close();
		verify(session2).close();
	}

	@Test // DATAMONGO-1920
	void readonlyShouldInitiateASessionStartAndCommitTransaction() {

		MongoTransactionManager txManager = new MongoTransactionManager(dbFactory);

		DefaultTransactionDefinition readonlyTxDefinition = new DefaultTransactionDefinition();
		readonlyTxDefinition.setReadOnly(true);

		TransactionStatus txStatus = txManager.getTransaction(readonlyTxDefinition);

		MongoTemplate template = new MongoTemplate(dbFactory);

		template.execute(db -> {
			db.drop();
			return null;
		});

		verify(dbFactory).withSession(eq(session));

		txManager.commit(txStatus);

		verify(session).startTransaction();
		verify(session).commitTransaction();
		verify(session).close();
	}

	@Test // DATAMONGO-1920
	void readonlyShouldInitiateASessionStartAndRollbackTransaction() {

		MongoTransactionManager txManager = new MongoTransactionManager(dbFactory);

		DefaultTransactionDefinition readonlyTxDefinition = new DefaultTransactionDefinition();
		readonlyTxDefinition.setReadOnly(true);

		TransactionStatus txStatus = txManager.getTransaction(readonlyTxDefinition);

		MongoTemplate template = new MongoTemplate(dbFactory);

		template.execute(db -> {
			db.drop();
			return null;
		});

		verify(dbFactory).withSession(eq(session));

		txManager.rollback(txStatus);

		verify(session).startTransaction();
		verify(session).abortTransaction();
		verify(session).close();
	}
}
