/*
 * Copyright 2017-2023 the original author or authors.
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

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Collation.Alternate;
import org.springframework.data.mongodb.core.query.Collation.ComparisonLevel;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.client.MongoClient;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
public class MongoTemplateCollationTests {

	public static final String COLLECTION_NAME = "collation-1";
	static @Client MongoClient mongoClient;

	@Configuration
	static class Config extends AbstractMongoClientConfiguration {

		@Override
		public MongoClient mongoClient() {
			return mongoClient;
		}

		@Override
		protected String getDatabaseName() {
			return "collation-tests";
		}

		@Override
		protected boolean autoIndexCreation() {
			return false;
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.emptySet();
		}
	}

	@Autowired MongoTemplate template;

	@BeforeEach
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
