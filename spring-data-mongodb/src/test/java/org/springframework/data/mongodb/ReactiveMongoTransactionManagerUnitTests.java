/*
 * Copyright 2019-2023 the original author or authors.
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

import static org.mockito.Mockito.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.session.ServerSession;

/**
 * Unit tests for {@link ReactiveMongoTransactionManager}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Mathieu Ouellet
 */
@ExtendWith(MockitoExtension.class)
class ReactiveMongoTransactionManagerUnitTests {

	@Mock ClientSession session;
	@Mock ClientSession session2;
	@Mock ServerSession serverSession;
	@Mock ReactiveMongoDatabaseFactory databaseFactory;
	@Mock ReactiveMongoDatabaseFactory databaseFactory2;
	@Mock MongoDatabase db;
	@Mock MongoDatabase db2;

	@BeforeEach
	void setUp() {
		when(databaseFactory.getSession(any())).thenReturn(Mono.just(session), Mono.just(session2));
		when(databaseFactory.withSession(session)).thenReturn(databaseFactory);
		when(databaseFactory.getMongoDatabase()).thenReturn(Mono.just(db));
		when(session.getServerSession()).thenReturn(serverSession);
	}

	@Test // DATAMONGO-2265
	void triggerCommitCorrectly() {

		ReactiveMongoTransactionManager txManager = new ReactiveMongoTransactionManager(databaseFactory);
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(databaseFactory);
		when(session.commitTransaction()).thenReturn(Mono.empty());

		TransactionalOperator operator = TransactionalOperator.create(txManager, new DefaultTransactionDefinition());

		template.execute(db -> {
			db.drop();
			return Mono.empty();

		}).as(operator::transactional) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(databaseFactory).withSession(eq(session));

		verify(session).startTransaction();
		verify(session).commitTransaction();

		verify(session).close();
	}

	@Test // DATAMONGO-2265
	void participateInOnGoingTransactionWithCommit() {

		ReactiveMongoTransactionManager txManager = new ReactiveMongoTransactionManager(databaseFactory);
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(databaseFactory);
		when(session.commitTransaction()).thenReturn(Mono.empty());

		TransactionalOperator operator = TransactionalOperator.create(txManager, new DefaultTransactionDefinition());

		template.execute(db -> {
			db.drop();
			return Mono.empty();
		}).as(StepVerifier::create).verifyComplete();

		template.execute(db -> {
			db.drop();
			return Mono.empty();
		}).as(operator::transactional) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(databaseFactory, times(1)).withSession(eq(session));

		verify(session).startTransaction();
		verify(session).commitTransaction();
		verify(session).close();
	}

	@Test // DATAMONGO-2265
	void participateInOnGoingTransactionWithRollbackOnly() {

		ReactiveMongoTransactionManager txManager = new ReactiveMongoTransactionManager(databaseFactory);
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(databaseFactory);
		when(session.abortTransaction()).thenReturn(Mono.empty());

		TransactionalOperator operator = TransactionalOperator.create(txManager, new DefaultTransactionDefinition());

		operator.execute(tx -> {

			return template.execute(db -> {
				db.drop();
				tx.setRollbackOnly();
				return Mono.empty();
			});
		}).as(StepVerifier::create).verifyComplete();

		verify(databaseFactory, times(1)).withSession(eq(session));

		verify(session).startTransaction();
		verify(session).abortTransaction();
		verify(session).close();
	}

	@Test // DATAMONGO-2265
	void suspendTransactionWhilePropagationNotSupported() {

		ReactiveMongoTransactionManager txManager = new ReactiveMongoTransactionManager(databaseFactory);
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(databaseFactory);
		when(session.commitTransaction()).thenReturn(Mono.empty());

		TransactionalOperator outer = TransactionalOperator.create(txManager, new DefaultTransactionDefinition());

		DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
		definition.setPropagationBehavior(TransactionDefinition.PROPAGATION_NOT_SUPPORTED);
		TransactionalOperator inner = TransactionalOperator.create(txManager, definition);

		outer.execute(tx1 -> {

			return template.execute(db -> {

				db.drop();

				return inner.execute(tx2 -> {
					return template.execute(db2 -> {
						db2.drop();
						return Mono.empty();
					});
				});
			});
		}).as(StepVerifier::create).verifyComplete();

		verify(session).startTransaction();
		verify(session2, never()).startTransaction();

		verify(databaseFactory, times(1)).withSession(eq(session));
		verify(databaseFactory, never()).withSession(eq(session2));

		verify(db, times(2)).drop();

		verify(session2, never()).close();
	}

	@Test // DATAMONGO-2265
	void suspendTransactionWhilePropagationRequiresNew() {

		when(databaseFactory.withSession(session2)).thenReturn(databaseFactory2);
		when(databaseFactory2.getMongoDatabase()).thenReturn(Mono.just(db2));
		when(session2.getServerSession()).thenReturn(serverSession);

		ReactiveMongoTransactionManager txManager = new ReactiveMongoTransactionManager(databaseFactory);
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(databaseFactory);
		when(session.commitTransaction()).thenReturn(Mono.empty());
		when(session2.commitTransaction()).thenReturn(Mono.empty());

		TransactionalOperator outer = TransactionalOperator.create(txManager, new DefaultTransactionDefinition());

		DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
		definition.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
		TransactionalOperator inner = TransactionalOperator.create(txManager, definition);

		outer.execute(tx1 -> {

			return template.execute(db -> {

				db.drop();

				return inner.execute(tx2 -> {
					return template.execute(db2 -> {
						db2.drop();
						return Mono.empty();
					});
				});
			});
		}).as(StepVerifier::create).verifyComplete();

		verify(session).startTransaction();
		verify(session2).startTransaction();

		verify(databaseFactory, times(1)).withSession(eq(session));
		verify(databaseFactory).withSession(eq(session2));

		verify(db).drop();
		verify(db2).drop();

		verify(session).close();
		verify(session2).close();
	}

	@Test // DATAMONGO-2265
	void readonlyShouldInitiateASessionStartAndCommitTransaction() {

		ReactiveMongoTransactionManager txManager = new ReactiveMongoTransactionManager(databaseFactory);
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(databaseFactory);
		when(session.commitTransaction()).thenReturn(Mono.empty());

		DefaultTransactionDefinition readonlyTxDefinition = new DefaultTransactionDefinition();
		readonlyTxDefinition.setReadOnly(true);
		TransactionalOperator operator = TransactionalOperator.create(txManager, readonlyTxDefinition);

		template.execute(db -> {
			db.drop();
			return Mono.empty();

		}).as(operator::transactional) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(databaseFactory).withSession(eq(session));

		verify(session).startTransaction();
		verify(session).commitTransaction();
		verify(session).close();
	}
}
