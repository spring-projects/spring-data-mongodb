/*
 * Copyright 2018 the original author or authors.
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

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.bson.Document;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.reactivestreams.Publisher;
import org.springframework.data.mongodb.core.ReactiveSessionCallback.ReactiveSessionContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.data.util.Version;

import com.mongodb.ClientSessionOptions;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.Success;

/**
 * @author Christoph Strobl
 * @currentRead Beyond the Shadows - Brent Weeks
 */
public class ReactiveMongoTemplateTransactionTests {

	public static @ClassRule MongoVersionRule REQUIRES_AT_LEAST_3_7_5 = MongoVersionRule.atLeast(Version.parse("3.7.5"));
	public static @ClassRule TestRule replSet = ReplicaSet.required();

	static final String DATABASE_NAME = "reflective-client-session-tests";
	static final String COLLECTION_NAME = "test";

	MongoClient client;
	ReactiveMongoTemplate template;

	@Before
	public void setUp() {

		client = MongoTestUtils.reactiveReplSetClient();

		template = new ReactiveMongoTemplate(client, DATABASE_NAME);

		StepVerifier.create(MongoTestUtils.createOrReplaceCollection(DATABASE_NAME, COLLECTION_NAME, client))
				.expectNext(Success.SUCCESS).verifyComplete();

		StepVerifier.create(template.insert(new Document("_id", "id-1").append("value", "spring"), COLLECTION_NAME))
				.expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1920
	public void reactiveTransactionWithExplicitTransactionStart() throws InterruptedException {

		Query docWithId = query(where("_id").is("id-1"));
		template.exists(docWithId, COLLECTION_NAME) //
				.as(StepVerifier::create).expectNext(true).verifyComplete();

		Publisher<ClientSession> sessionPublisher = client
				.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

		template.withSession(sessionPublisher).execute(action -> ReactiveSessionContext.getSession().flatMap(session -> {

			session.startTransaction();
			return action.remove(docWithId, Document.class, COLLECTION_NAME);

		})).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.exists(docWithId, COLLECTION_NAME) //
				.as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1920
	public void reactiveTransactionsCommitOnComplete() throws InterruptedException {

		Query docWithId = query(where("_id").is("id-1"));

		template.exists(docWithId, COLLECTION_NAME) //
				.as(StepVerifier::create).expectNext(true).verifyComplete();

		template.inTransaction().execute(action -> action.remove(docWithId, Document.class, COLLECTION_NAME)) //
				.as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.exists(docWithId, COLLECTION_NAME) //
				.as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1920
	public void reactiveTransactionsAbortOnError() throws InterruptedException {

		Query docWithId = query(where("_id").is("id-1"));

		template.exists(docWithId, COLLECTION_NAME) //
				.as(StepVerifier::create).expectNext(true).verifyComplete();

		template.inTransaction().execute(action -> {
			return action.remove(docWithId, Document.class, COLLECTION_NAME).flatMap(result -> Mono.fromSupplier(() -> {
				throw new RuntimeException("¯\\_(ツ)_/¯");
			}));
		}).as(StepVerifier::create).expectError().verify();

		template.exists(docWithId, COLLECTION_NAME) //
				.as(StepVerifier::create).expectNext(true).verifyComplete();
	}
}
