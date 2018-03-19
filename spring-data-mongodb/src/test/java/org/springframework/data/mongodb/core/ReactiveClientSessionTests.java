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

import static org.assertj.core.api.Assertions.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.bson.Document;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.data.util.Version;

import com.mongodb.ClientSessionOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.session.ClientSession;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ReactiveClientSessionTests {

	public static @ClassRule MongoVersionRule REQUIRES_AT_LEAST_3_6_0 = MongoVersionRule.atLeast(Version.parse("3.6.0"));
	public static @ClassRule TestRule replSet = ReplicaSet.required();

	MongoClient client;
	ReactiveMongoTemplate template;

	@Before
	public void setUp() {

		client = MongoClients.create();

		template = new ReactiveMongoTemplate(client, "reflective-client-session-tests");

		StepVerifier.create(template.dropCollection("test")).verifyComplete();

		StepVerifier.create(template.insert(new Document("_id", "id-1").append("value", "spring"), "test"))
				.expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1880
	public void shouldApplyClientSession() {

		ClientSession session = Mono
				.from(client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build())).block();

		assertThat(session.getOperationTime()).isNull();

		StepVerifier.create(template.withSession(() -> session).execute(action -> action.findAll(Document.class, "test")))
				.expectNextCount(1).verifyComplete();

		assertThat(session.getOperationTime()).isNotNull();
		assertThat(session.getServerSession().isClosed()).isFalse();

		session.close();
	}

	@Test // DATAMONGO-1880
	public void useMonoInCallback() {

		ClientSession session = Mono
				.from(client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build())).block();

		assertThat(session.getOperationTime()).isNull();

		StepVerifier
				.create(
						template.withSession(() -> session).execute(action -> action.findOne(new Query(), Document.class, "test")))
				.expectNextCount(1).verifyComplete();

		assertThat(session.getOperationTime()).isNotNull();
		assertThat(session.getServerSession().isClosed()).isFalse();

		session.close();
	}

	@Test // DATAMONGO-1880
	public void reusesClientSessionInSessionScopedCallback() {

		ClientSession session = Mono
				.from(client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build())).block();
		CountingSessionSupplier sessionSupplier = new CountingSessionSupplier(session);

		ReactiveSessionScoped sessionScoped = template.withSession(sessionSupplier);

		sessionScoped.execute(action -> action.findOne(new Query(), Document.class, "test")).blockFirst();
		assertThat(sessionSupplier.getInvocationCount()).isEqualTo(1);

		sessionScoped.execute(action -> action.findOne(new Query(), Document.class, "test")).blockFirst();
		assertThat(sessionSupplier.getInvocationCount()).isEqualTo(1);
	}

	static class CountingSessionSupplier implements Supplier<ClientSession> {

		AtomicInteger invocationCount = new AtomicInteger(0);
		final ClientSession session;

		public CountingSessionSupplier(ClientSession session) {
			this.session = session;
		}

		@Override
		public ClientSession get() {

			invocationCount.incrementAndGet();
			return session;
		}

		int getInvocationCount() {
			return invocationCount.get();
		}
	}
}
