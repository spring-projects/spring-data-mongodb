/*
 * Copyright 2017 the original author or authors.
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

import java.util.List;
import java.util.Locale;

import org.bson.Document;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.Collation.Alternate;
import org.springframework.data.mongodb.core.Collation.ComparisonLevel;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.util.Version;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.MongoClient;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class MongoTemplateCollationTests {

	public static @ClassRule MongoVersionRule REQUIRES_AT_LEAST_3_4_0 = MongoVersionRule.atLeast(Version.parse("3.4.0"));
	public static final String COLLECTION_NAME = "collation-1";

	@Configuration
	static class Config extends AbstractMongoConfiguration {

		@Override
		public MongoClient mongoClient() {
			return new MongoClient();
		}

		@Override
		protected String getDatabaseName() {
			return "collation-tests";
		}
	}

	@Autowired MongoTemplate template;

	@Before
	public void setUp() {
		template.dropCollection(COLLECTION_NAME);
	}

	@Test // DATAMONGO-1518
	public void createCollectionWithCollation() {

		template.createCollection(COLLECTION_NAME, CollectionOptions.just(Collation.of("en_US")));

		Document collation = getCollationInfo(COLLECTION_NAME);
		assertThat(collation.get("locale")).isEqualTo("en_US");
	}

	@Test // DATAMONGO-1518
	public void createCollectionWithCollationHavingLocaleVariant() {

		template.createCollection(COLLECTION_NAME,
				CollectionOptions.just(Collation.of(new Locale("de", "AT", "phonebook"))));

		Document collation = getCollationInfo(COLLECTION_NAME);
		assertThat(collation.get("locale")).isEqualTo("de_AT@collation=phonebook");
	}

	@Test // DATAMONGO-1518
	public void createCollectionWithCollationHavingStrength() {

		template.createCollection(COLLECTION_NAME,
				CollectionOptions.just(Collation.of("en_US").strength(ComparisonLevel.primary().includeCase())));

		Document collation = getCollationInfo(COLLECTION_NAME);
		assertThat(collation.get("strength")).isEqualTo(1);
		assertThat(collation.get("caseLevel")).isEqualTo(true);
	}

	@Test // DATAMONGO-1518
	public void createCollectionWithCollationHavingBackwardsAndNumericOrdering() {

		template.createCollection(COLLECTION_NAME,
				CollectionOptions.just(Collation.of("en_US").backwardDiacriticSort().numericOrderingEnabled()));

		Document collation = getCollationInfo(COLLECTION_NAME);
		assertThat(collation.get("backwards")).isEqualTo(true);
		assertThat(collation.get("numericOrdering")).isEqualTo(true);
	}

	@Test // DATAMONGO-1518
	public void createCollationWithCollationHavingAlternate() {

		template.createCollection(COLLECTION_NAME,
				CollectionOptions.just(Collation.of("en_US").alternate(Alternate.shifted().punct())));

		Document collation = getCollationInfo(COLLECTION_NAME);
		assertThat(collation.get("alternate")).isEqualTo("shifted");
		assertThat(collation.get("maxVariable")).isEqualTo("punct");
	}

	private Document getCollationInfo(String collectionName) {
		return getCollectionInfo(collectionName).get("options", Document.class).get("collation", Document.class);
	}

	private Document getCollectionInfo(String collectionName) {

		return template.execute(db -> {

			Document result = db.runCommand(
					new Document().append("listCollections", 1).append("filter", new Document("name", collectionName)));
			return (Document) result.get("cursor", Document.class).get("firstBatch", List.class).get(0);
		});
	}

}
