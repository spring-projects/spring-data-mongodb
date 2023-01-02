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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.EnableIfReplicaSetAvailable;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.reactive.TransactionCallback;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.mongodb.ClientSessionOptions;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Integration tests for Mongo Transactions using {@link ReactiveMongoTemplate}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Mathieu Ouellet
 * @currentRead The Core - Peter V. Brett
 */
@ExtendWith(MongoClientExtension.class)
@EnableIfReplicaSetAvailable
@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
public class ReactiveMongoTemplateTransactionTests {

	static final String DATABASE_NAME = "reactive-template-tx-tests";
	static final String COLLECTION_NAME = "test";
	static final Document DOCUMENT = new Document("_id", "id-1").append("value", "spring");
	static final Query ID_QUERY = query(where("_id").is("id-1"));

	static final Person AHMANN = new Person("ahmann", 32);
	static final Person ARLEN = new Person("arlen", 24);
	static final Person LEESHA = new Person("leesha", 22);
	static final Person RENNA = new Person("renna", 22);

	static @Client MongoClient client;
	ReactiveMongoTemplate template;

	@BeforeEach
	public void setUp() {

		template = new ReactiveMongoTemplate(client, DATABASE_NAME);

		MongoTestUtils.createOrReplaceCollection(DATABASE_NAME, COLLECTION_NAME, client).as(StepVerifier::create) //
				.verifyComplete();

		MongoTestUtils.createOrReplaceCollection(DATABASE_NAME, "person", client).as(StepVerifier::create).verifyComplete();

		MongoTestUtils.createOrReplaceCollection(DATABASE_NAME, "personWithVersionPropertyOfTypeInteger", client)
				.as(StepVerifier::create) //
				.verifyComplete();

		template.insert(DOCUMENT, COLLECTION_NAME).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.insertAll(Arrays.asList(AHMANN, ARLEN, LEESHA, RENNA)) //
				.as(StepVerifier::create) //
				.expectNextCount(4) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1970
	public void reactiveTransactionWithExplicitTransactionStart() {

		Publisher<ClientSession> sessionPublisher = client
				.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

		ClientSession clientSession = Mono.from(sessionPublisher).block();

		template.withSession(Mono.just(clientSession))
				.execute(action -> ReactiveMongoContext.getSession().flatMap(session -> {

					session.startTransaction();
					return action.remove(ID_QUERY, Document.class, COLLECTION_NAME);

				})).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.exists(ID_QUERY, COLLECTION_NAME) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		assertThat(clientSession.hasActiveTransaction()).isTrue();
		StepVerifier.create(clientSession.commitTransaction()).verifyComplete();

		template.exists(ID_QUERY, COLLECTION_NAME) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1970
	public void reactiveTransactionsCommitOnComplete() {

		initTx().transactional(template.remove(ID_QUERY, Document.class, COLLECTION_NAME)).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.exists(ID_QUERY, COLLECTION_NAME) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1970
	public void reactiveTransactionsAbortOnError() {

		initTx().transactional(
				template.remove(ID_QUERY, Document.class, COLLECTION_NAME).flatMap(result -> Mono.fromSupplier(() -> {
					throw new RuntimeException("¯\\_(ツ)_/¯");
				}))).as(StepVerifier::create) //
				.expectError() //
				.verify();

		template.exists(ID_QUERY, COLLECTION_NAME) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1970
	public void withSessionDoesNotManageTransactions() {

		Mono.from(client.startSession()).flatMap(session -> {

			session.startTransaction();
			return template.withSession(session).remove(ID_QUERY, Document.class, COLLECTION_NAME);
		}).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.exists(ID_QUERY, COLLECTION_NAME) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1970
	public void changesNotVisibleOutsideTransaction() {

		initTx().execute(new TransactionCallback<>() {
			@Override
			public Publisher<Object> doInTransaction(ReactiveTransaction status) {
				return template.remove(ID_QUERY, Document.class, COLLECTION_NAME).flatMapMany(val -> {

					// once we use the collection directly we're no longer participating in the tx
					return client.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME).find(ID_QUERY.getQueryObject())
							.first();
				});
			}
		}).as(StepVerifier::create).expectNext(DOCUMENT).verifyComplete();

		template.exists(ID_QUERY, COLLECTION_NAME) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1970
	public void executeCreatesNewTransaction() {

		ReactiveSessionScoped sessionScoped = template.withSession(client.startSession());

		sessionScoped.execute(action -> {
			return action.remove(ID_QUERY, Document.class, COLLECTION_NAME);
		}) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.exists(ID_QUERY, COLLECTION_NAME) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();

		sessionScoped.execute(action -> {
			return action.insert(DOCUMENT, COLLECTION_NAME);
		}) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.exists(ID_QUERY, COLLECTION_NAME) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1970
	public void takeDoesNotAbortTransaction() {

		initTx()
				.transactional(template.find(query(where("age").exists(true)).with(Sort.by("age")), Person.class).take(3)
						.flatMap(template::remove)) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		template.count(query(where("age").exists(true)), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1970
	public void errorInFlowOutsideTransactionDoesNotAbortIt() {

		initTx().execute(new TransactionCallback<>() {
			@Override
			public Publisher<Object> doInTransaction(ReactiveTransaction status) {
				return template.find(query(where("age").is(22)).with(Sort.by("age")), Person.class).buffer(2)
						.flatMap(values -> {

							return template
									.remove(query(where("id").in(values.stream().map(Person::getId).collect(Collectors.toList()))),
											Person.class)
									.then(Mono.just(values));
						});
			}
		}).collectList() // completes the above computation
				.flatMap(deleted -> {
					throw new RuntimeException("error outside the transaction does not influence it.");
				}).as(StepVerifier::create) //
				.verifyError();

		template.count(query(where("age").exists(true)), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2195
	public void deleteWithMatchingVersion() {

		PersonWithVersionPropertyOfTypeInteger rojer = new PersonWithVersionPropertyOfTypeInteger();
		rojer.firstName = "rojer";

		PersonWithVersionPropertyOfTypeInteger saved = template.insert(rojer).block();

		initTx().transactional(template.remove(saved)) //
				.as(StepVerifier::create) //
				.consumeNextWith(result -> assertThat(result.getDeletedCount()).isOne()) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2195
	public void deleteWithVersionMismatch() {

		PersonWithVersionPropertyOfTypeInteger rojer = new PersonWithVersionPropertyOfTypeInteger();
		rojer.firstName = "rojer";

		PersonWithVersionPropertyOfTypeInteger saved = template.insert(rojer).block();
		saved.version = 5;

		initTx().transactional(template.remove(saved)) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.wasAcknowledged()).isTrue();
					assertThat(actual.getDeletedCount()).isZero();
				}).verifyComplete();
	}

	@Test // DATAMONGO-2195
	public void deleteNonExistingWithVersion() {

		PersonWithVersionPropertyOfTypeInteger rojer = new PersonWithVersionPropertyOfTypeInteger();
		rojer.id = "deceased";
		rojer.firstName = "rojer";
		rojer.version = 5;

		initTx().transactional(template.remove(rojer)) //
				.as(StepVerifier::create) //
				.consumeNextWith(result -> assertThat(result.getDeletedCount()).isZero()) //
				.verifyComplete();
	}

	TransactionalOperator initTx() {

		ReactiveMongoTransactionManager txmgr = new ReactiveMongoTransactionManager(template.getMongoDatabaseFactory());
		return TransactionalOperator.create(txmgr, new DefaultTransactionDefinition());
	}
}
