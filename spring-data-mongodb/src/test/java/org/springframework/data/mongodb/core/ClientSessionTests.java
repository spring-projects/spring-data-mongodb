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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import com.mongodb.MongoClient;
import lombok.AllArgsConstructor;
import lombok.Data;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.mongodb.test.util.MongoVersion;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.data.util.Version;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ClientSession;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ClientSessionTests {

	public static @ClassRule TestRule replSet = ReplicaSet.required();
	public @Rule MongoVersionRule REQUIRES_AT_LEAST_3_6_0 = MongoVersionRule.atLeast(Version.parse("3.6.0"));

	private static final String DB_NAME = "client-session-tests";
	private static final String COLLECTION_NAME = "test";

	MongoTemplate template;
	MongoClient client;

	@Before
	public void setUp() {

		client = MongoTestUtils.replSetClient();

		MongoTestUtils.createOrReplaceCollection(DB_NAME, COLLECTION_NAME, client);

		template = new MongoTemplate(client, DB_NAME);
		template.getDb().getCollection(COLLECTION_NAME).insertOne(new Document("_id", "id-1").append("value", "spring"));
	}

	@After
	public void tearDown() {
		client.close();
	}

	@Test // DATAMONGO-1880
	public void shouldApplyClientSession() {

		ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

		assertThat(session.getOperationTime()).isNull();

		Document doc = template.withSession(() -> session)
				.execute(action -> action.findOne(new Query(), Document.class, "test"));

		assertThat(doc).isNotNull();
		assertThat(session.getOperationTime()).isNotNull();
		assertThat(session.getServerSession().isClosed()).isFalse();

		session.close();
	}

	@Test // DATAMONGO-1920
	@MongoVersion(asOf = "3.7.3")
	public void withCommittedTransaction() {

		ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

		assertThat(session.getOperationTime()).isNull();

		session.startTransaction();

		SomeDoc saved = template.withSession(() -> session).execute(action -> {

			SomeDoc doc = new SomeDoc("id-2", "value2");
			action.insert(doc);
			return doc;
		});

		session.commitTransaction();
		session.close();

		assertThat(saved).isNotNull();
		assertThat(session.getOperationTime()).isNotNull();

		assertThat(template.exists(query(where("id").is(saved.getId())), SomeDoc.class)).isTrue();
	}

	@Test // DATAMONGO-1920
	@MongoVersion(asOf = "3.7.3")
	public void withAbortedTransaction() {

		ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

		assertThat(session.getOperationTime()).isNull();

		session.startTransaction();

		SomeDoc saved = template.withSession(() -> session).execute(action -> {

			SomeDoc doc = new SomeDoc("id-2", "value2");
			action.insert(doc);
			return doc;
		});

		session.abortTransaction();
		session.close();

		assertThat(saved).isNotNull();
		assertThat(session.getOperationTime()).isNotNull();

		assertThat(template.exists(query(where("id").is(saved.getId())), SomeDoc.class)).isFalse();
	}

	@Data
	@AllArgsConstructor
	@org.springframework.data.mongodb.core.mapping.Document(COLLECTION_NAME)
	static class SomeDoc {

		@Id String id;
		String value;
	}

}
