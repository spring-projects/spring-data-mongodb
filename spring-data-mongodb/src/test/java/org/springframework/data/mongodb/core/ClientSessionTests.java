/*
 * Copyright 2018-2020 the original author or authors.
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
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.mongodb.test.util.MongoVersion;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.data.util.Version;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.ClientSession;

/**
 * Integration tests for {@link ClientSession} through {@link MongoTemplate#withSession(ClientSession)}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ClientSessionTests {

	public static @ClassRule TestRule replSet = ReplicaSet.required();
	public @Rule MongoVersionRule REQUIRES_AT_LEAST_3_6_0 = MongoVersionRule.atLeast(Version.parse("3.6.0"));

	private static final String DB_NAME = "client-session-tests";
	private static final String COLLECTION_NAME = "test";
	private static final String REF_COLLECTION_NAME = "test-with-ref";

	MongoTemplate template;
	MongoClient client;

	@Before
	public void setUp() {

		client = MongoTestUtils.replSetClient();

		MongoTestUtils.createOrReplaceCollection(DB_NAME, COLLECTION_NAME, client);

		template = new MongoTemplate(client, DB_NAME);
		template.getDb().getCollection(COLLECTION_NAME).insertOne(new Document("_id", "id-1").append("value", "spring"));
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

	@Test // DATAMONGO-2241
	public void shouldReuseConfiguredInfrastructure() {

		ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

		MappingMongoConverter source = MappingMongoConverter.class.cast(template.getConverter());
		MappingMongoConverter sessionTemplateConverter = MappingMongoConverter.class
				.cast(template.withSession(() -> session).execute(MongoOperations::getConverter));

		assertThat(sessionTemplateConverter.getMappingContext()).isSameAs(source.getMappingContext());
		assertThat(ReflectionTestUtils.getField(sessionTemplateConverter, "conversions"))
				.isSameAs(ReflectionTestUtils.getField(source, "conversions"));
		assertThat(ReflectionTestUtils.getField(sessionTemplateConverter, "instantiators"))
				.isSameAs(ReflectionTestUtils.getField(source, "instantiators"));
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

	@Test // DATAMONGO-2490
	public void shouldBeAbleToReadDbRefDuringTransaction() {

		SomeDoc ref = new SomeDoc("ref-1", "da value");
		WithDbRef source = new WithDbRef("source-1", "da source", ref);

		ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

		assertThat(session.getOperationTime()).isNull();

		session.startTransaction();

		WithDbRef saved = template.withSession(() -> session).execute(action -> {

			template.save(ref);
			template.save(source);

			return template.findOne(query(where("id").is(source.id)), WithDbRef.class);
		});

		assertThat(saved.getSomeDocRef()).isEqualTo(ref);

		session.abortTransaction();
	}

	@Data
	@AllArgsConstructor
	@org.springframework.data.mongodb.core.mapping.Document(COLLECTION_NAME)
	static class SomeDoc {

		@Id String id;
		String value;
	}

	@Data
	@AllArgsConstructor
	@org.springframework.data.mongodb.core.mapping.Document(REF_COLLECTION_NAME)
	static class WithDbRef {

		@Id String id;
		String value;
		@DBRef SomeDoc someDocRef;
	}

}
