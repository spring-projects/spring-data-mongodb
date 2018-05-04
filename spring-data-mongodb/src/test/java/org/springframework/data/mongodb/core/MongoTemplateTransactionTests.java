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
import static org.springframework.data.mongodb.test.util.MongoTestUtils.*;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.bson.Document;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.test.util.AfterTransactionAssertion;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.data.util.Version;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.AfterTransaction;
import org.springframework.test.context.transaction.BeforeTransaction;
import org.springframework.transaction.annotation.Transactional;

import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

/**
 * @author Christoph Strobl
 * @currentRead Shadow's Edge - Brent Weeks
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@Transactional(transactionManager = "txManager")
public class MongoTemplateTransactionTests {

	public static @ClassRule RuleChain TEST_RULES = RuleChain.outerRule(MongoVersionRule.atLeast(Version.parse("3.7.3")))
			.around(ReplicaSet.required());

	static final String DB_NAME = "template-tx-tests";
	static final String COLLECTION_NAME = "assassins";

	@Configuration
	static class Config extends AbstractMongoConfiguration {

		@Bean
		public MongoClient mongoClient() {
			return MongoTestUtils.replSetClient();
		}

		@Override
		protected String getDatabaseName() {
			return DB_NAME;
		}

		@Bean
		MongoTransactionManager txManager(MongoDbFactory dbFactory) {
			return new MongoTransactionManager(dbFactory);
		}
	}

	@Autowired MongoTemplate template;
	@Autowired MongoClient client;

	List<AfterTransactionAssertion<? extends Persistable<?>>> assertionList;

	@Before
	public void setUp() {

		template.setReadPreference(ReadPreference.primary());
		assertionList = new CopyOnWriteArrayList<>();
	}

	@BeforeTransaction
	public void beforeTransaction() {
		createOrReplaceCollection(DB_NAME, COLLECTION_NAME, client);
	}

	@AfterTransaction
	public void verifyDbState()  {

		MongoCollection<Document> collection = client.getDatabase(DB_NAME).withReadPreference(ReadPreference.primary())
				.getCollection(COLLECTION_NAME);

		assertionList.forEach(it -> {

			boolean isPresent = collection.count(Filters.eq("_id", it.getId())) != 0;

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
