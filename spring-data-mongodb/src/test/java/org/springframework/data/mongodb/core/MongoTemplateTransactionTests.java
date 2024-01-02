/*
 * Copyright 2018-2024 the original author or authors.
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
import static org.springframework.data.mongodb.test.util.MongoTestUtils.*;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.test.util.AfterTransactionAssertion;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.EnableIfReplicaSetAvailable;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.ReplSetClient;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.transaction.AfterTransaction;
import org.springframework.test.context.transaction.BeforeTransaction;
import org.springframework.transaction.annotation.Transactional;

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

/**
 * @author Christoph Strobl
 * @currentRead Shadow's Edge - Brent Weeks
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
@EnableIfReplicaSetAvailable
@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
@ContextConfiguration
@Transactional(transactionManager = "txManager")
public class MongoTemplateTransactionTests {

	static final String DB_NAME = "template-tx-tests";
	static final String COLLECTION_NAME = "assassins";

	static @ReplSetClient MongoClient mongoClient;

	@Configuration
	static class Config extends AbstractMongoClientConfiguration {

		@Bean
		public MongoClient mongoClient() {
			return mongoClient;
		}

		@Override
		protected String getDatabaseName() {
			return DB_NAME;
		}

		@Override
		protected boolean autoIndexCreation() {
			return false;
		}

		@Bean
		MongoTransactionManager txManager(MongoDatabaseFactory dbFactory) {
			return new MongoTransactionManager(dbFactory);
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.emptySet();
		}
	}

	@Autowired MongoTemplate template;
	@Autowired MongoClient client;

	List<AfterTransactionAssertion<? extends Persistable<?>>> assertionList;

	@BeforeEach
	public void setUp() {

		template.setReadPreference(ReadPreference.primary());
		assertionList = new CopyOnWriteArrayList<>();
	}

	@BeforeTransaction
	public void beforeTransaction() {
		createOrReplaceCollection(DB_NAME, COLLECTION_NAME, client);
	}

	@AfterTransaction
	public void verifyDbState() {

		MongoCollection<Document> collection = client.getDatabase(DB_NAME).withReadPreference(ReadPreference.primary())
				.getCollection(COLLECTION_NAME);

		assertionList.forEach(it -> {

			boolean isPresent = collection.countDocuments(Filters.eq("_id", it.getId())) != 0;

			assertThat(isPresent).isEqualTo(it.shouldBePresent())
					.withFailMessage(String.format("After transaction entity %s should %s.", it.getPersistable(),
							it.shouldBePresent() ? "be present" : "NOT be present"));
		});
	}

	@Rollback(false)
	@Test // DATAMONGO-1920
	public void shouldOperateCommitCorrectly() {

		Assassin hu = new Assassin("hu", "Hu Gibbet");
		template.save(hu);

		assertAfterTransaction(hu).isPresent();
	}

	@Test // DATAMONGO-1920
	public void shouldOperateRollbackCorrectly() {

		Assassin vi = new Assassin("vi", "Viridiana Sovari");
		template.save(vi);

		assertAfterTransaction(vi).isNotPresent();
	}

	@Test // DATAMONGO-1920
	public void shouldBeAbleToViewChangesDuringTransaction() throws InterruptedException {

		Assassin durzo = new Assassin("durzo", "Durzo Blint");
		template.save(durzo);

		Thread.sleep(100);
		Assassin retrieved = template.findOne(query(where("id").is(durzo.getId())), Assassin.class);

		assertThat(retrieved).isEqualTo(durzo);

		assertAfterTransaction(durzo).isNotPresent();
	}

	// --- Just some helpers and tests entities

	private AfterTransactionAssertion assertAfterTransaction(Assassin assassin) {

		AfterTransactionAssertion<Assassin> assertion = new AfterTransactionAssertion<>(assassin);
		assertionList.add(assertion);
		return assertion;
	}

	@Data
	@AllArgsConstructor
	@org.springframework.data.mongodb.core.mapping.Document(COLLECTION_NAME)
	static class Assassin implements Persistable<String> {

		@Id String id;
		String name;

		@Override
		public boolean isNew() {
			return id == null;
		}
	}
}
