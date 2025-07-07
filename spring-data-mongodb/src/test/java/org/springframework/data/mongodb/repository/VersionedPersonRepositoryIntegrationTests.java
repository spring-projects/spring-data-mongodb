/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.assertj.core.api.Assertions.*;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.repository.CrudRepository;
import org.springframework.lang.Nullable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for Repositories using optimistic locking.
 *
 * @author Christoph Strobl
 */
@ExtendWith({ SpringExtension.class })
@ContextConfiguration
class VersionedPersonRepositoryIntegrationTests {

	static @Client MongoClient mongoClient;

	@Autowired VersionedPersonRepository versionedPersonRepository;
	@Autowired MongoTemplate template;

	@Configuration
	@EnableMongoRepositories(considerNestedRepositories = true,
			includeFilters = @Filter(type = FilterType.ASSIGNABLE_TYPE, classes = VersionedPersonRepository.class))
	static class Config extends AbstractMongoClientConfiguration {

		@Override
		protected String getDatabaseName() {
			return "versioned-person-tests";
		}

		@Override
		public MongoClient mongoClient() {
			return mongoClient;
		}
	}

	@BeforeEach
	void beforeEach() {
		MongoTestUtils.flushCollection("versioned-person-tests",
				template.getCollectionName(VersionedPersonWithCounter.class), mongoClient);
	}

	@Test // GH-4918
	void updatesVersionedTypeCorrectly() {

		VersionedPerson person = template.insert(VersionedPersonWithCounter.class)
				.one(new VersionedPersonWithCounter("Donald", "Duckling"));

		int updateCount = versionedPersonRepository.findAndSetFirstnameToLastnameByLastname(person.getLastname());

		assertThat(updateCount).isOne();

		Document document = template.execute(VersionedPersonWithCounter.class, collection -> {
			return collection.find(new Document("_id", new ObjectId(person.getId()))).first();
		});

		assertThat(document).containsEntry("firstname", "Duckling").containsEntry("version", 1L);
	}

	@Test // GH-4918
	void updatesVersionedTypeCorrectlyWhenUpdateIsUsingInc() {

		VersionedPerson person = template.insert(VersionedPersonWithCounter.class)
				.one(new VersionedPersonWithCounter("Donald", "Duckling"));

		int updateCount = versionedPersonRepository.findAndIncCounterByLastname(person.getLastname());

		assertThat(updateCount).isOne();

		Document document = template.execute(VersionedPersonWithCounter.class, collection -> {
			return collection.find(new Document("_id", new ObjectId(person.getId()))).first();
		});

		assertThat(document).containsEntry("lastname", "Duckling").containsEntry("version", 1L).containsEntry("counter",
				42);
	}

	@Test // GH-4918
	void updatesVersionedTypeCorrectlyWhenUpdateCoversVersionBump() {

		VersionedPerson person = template.insert(VersionedPersonWithCounter.class)
				.one(new VersionedPersonWithCounter("Donald", "Duckling"));

		int updateCount = versionedPersonRepository.findAndSetFirstnameToLastnameIncVersionByLastname(person.getLastname(),
				10);

		assertThat(updateCount).isOne();

		Document document = template.execute(VersionedPersonWithCounter.class, collection -> {
			return collection.find(new Document("_id", new ObjectId(person.getId()))).first();
		});

		assertThat(document).containsEntry("firstname", "Duckling").containsEntry("version", 10L);
	}

	interface VersionedPersonRepository extends CrudRepository<VersionedPersonWithCounter, String> {

		@Update("{ '$set': { 'firstname' : ?0 } }")
		int findAndSetFirstnameToLastnameByLastname(String lastname);

		@Update("{ '$inc': { 'counter' : 42 } }")
		int findAndIncCounterByLastname(String lastname);

		@Update("""
				{
					'$set': { 'firstname' : ?0 },
					'$inc': { 'version' : ?1 }
				}""")
		int findAndSetFirstnameToLastnameIncVersionByLastname(String lastname, int incVersion);

	}

	@org.springframework.data.mongodb.core.mapping.Document("versioned-person")
	static class VersionedPersonWithCounter extends VersionedPerson {

		int counter;

		public VersionedPersonWithCounter(String firstname, @Nullable String lastname) {
			super(firstname, lastname);
		}

		public int getCounter() {
			return counter;
		}

		public void setCounter(int counter) {
			this.counter = counter;
		}

	}

}
