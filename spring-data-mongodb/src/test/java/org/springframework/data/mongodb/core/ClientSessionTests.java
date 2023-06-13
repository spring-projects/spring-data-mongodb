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

import java.util.Objects;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.EnableIfReplicaSetAvailable;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.mongodb.test.util.ReplSetClient;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;

/**
 * Integration tests for {@link ClientSession} through {@link MongoTemplate#withSession(ClientSession)}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith({ MongoClientExtension.class })
@EnableIfReplicaSetAvailable
@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
class ClientSessionTests {

	private static final String DB_NAME = "client-session-tests";
	private static final String COLLECTION_NAME = "test";
	private static final String REF_COLLECTION_NAME = "test-with-ref";

	private static @ReplSetClient MongoClient mongoClient;

	private MongoTemplate template;

	@BeforeEach
	void setUp() {

		MongoTestUtils.createOrReplaceCollection(DB_NAME, COLLECTION_NAME, mongoClient);

		template = new MongoTemplate(mongoClient, DB_NAME);
		template.getDb().getCollection(COLLECTION_NAME).insertOne(new Document("_id", "id-1").append("value", "spring"));
	}

	@Test // DATAMONGO-1880
	void shouldApplyClientSession() {

		ClientSession session = mongoClient.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

		assertThat(session.getOperationTime()).isNull();

		Document doc = template.withSession(() -> session)
				.execute(action -> action.findOne(new Query(), Document.class, "test"));

		assertThat(doc).isNotNull();
		assertThat(session.getOperationTime()).isNotNull();
		assertThat(session.getServerSession().isClosed()).isFalse();

		session.close();
	}

	@Test // DATAMONGO-2241
	void shouldReuseConfiguredInfrastructure() {

		ClientSession session = mongoClient.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

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
	void withCommittedTransaction() {

		ClientSession session = mongoClient.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

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
	void withAbortedTransaction() {

		ClientSession session = mongoClient.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

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
	void shouldBeAbleToReadDbRefDuringTransaction() {

		SomeDoc ref = new SomeDoc("ref-1", "da value");
		WithDbRef source = new WithDbRef("source-1", "da source", ref);

		ClientSession session = mongoClient.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

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

	@org.springframework.data.mongodb.core.mapping.Document(COLLECTION_NAME)
	static class SomeDoc {

		@Id String id;
		String value;

		SomeDoc(String id, String value) {

			this.id = id;
			this.value = value;
		}

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SomeDoc someDoc = (SomeDoc) o;
			return Objects.equals(id, someDoc.id) && Objects.equals(value, someDoc.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, value);
		}

		public String toString() {
			return "ClientSessionTests.SomeDoc(id=" + this.getId() + ", value=" + this.getValue() + ")";
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document(REF_COLLECTION_NAME)
	static class WithDbRef {

		@Id String id;
		String value;
		@DBRef SomeDoc someDocRef;

		WithDbRef(String id, String value, SomeDoc someDocRef) {
			this.id = id;
			this.value = value;
			this.someDocRef = someDocRef;
		}

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public SomeDoc getSomeDocRef() {
			return this.someDocRef;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public void setSomeDocRef(SomeDoc someDocRef) {
			this.someDocRef = someDocRef;
		}

		public String toString() {
			return "ClientSessionTests.WithDbRef(id=" + this.getId() + ", value=" + this.getValue() + ", someDocRef="
					+ this.getSomeDocRef() + ")";
		}
	}

}
