/*
 * Copyright 2015-2020 the original author or authors.
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

import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for DATAMONGO-1289.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class NoExplicitIdTests {

	@Configuration
	@EnableMongoRepositories(considerNestedRepositories = true)
	static class Config extends AbstractMongoClientConfiguration {

		@Override
		protected String getDatabaseName() {
			return "test";
		}

		@Override
		public MongoClient mongoClient() {
			return MongoTestUtils.client();
		}
	}

	@Autowired MongoOperations mongoOps;
	@Autowired TypeWithoutExplicitIdPropertyRepository repo;

	@Before
	public void setUp() {
		mongoOps.dropCollection(TypeWithoutIdProperty.class);
	}

	@Test // DATAMONGO-1289
	public void saveAndRetrieveTypeWithoutIdPropertyViaTemplate() {

		TypeWithoutIdProperty noid = new TypeWithoutIdProperty();
		noid.someString = "o.O";

		mongoOps.save(noid);

		TypeWithoutIdProperty retrieved = mongoOps.findOne(query(where("someString").is(noid.someString)),
				TypeWithoutIdProperty.class);

		assertThat(retrieved.someString).isEqualTo(noid.someString);
	}

	@Test // DATAMONGO-1289
	public void saveAndRetrieveTypeWithoutIdPropertyViaRepository() {

		TypeWithoutIdProperty noid = new TypeWithoutIdProperty();
		noid.someString = "o.O";

		repo.save(noid);

		TypeWithoutIdProperty retrieved = repo.findBySomeString(noid.someString);
		assertThat(retrieved.someString).isEqualTo(noid.someString);
	}

	@Test // DATAMONGO-1289
	@SuppressWarnings("unchecked")
	public void saveAndRetrieveTypeWithoutIdPropertyViaRepositoryFindOne() {

		TypeWithoutIdProperty noid = new TypeWithoutIdProperty();
		noid.someString = "o.O";

		repo.save(noid);

		Map<String, Object> map = mongoOps.findOne(query(where("someString").is(noid.someString)), Map.class,
				"typeWithoutIdProperty");

		Optional<TypeWithoutIdProperty> retrieved = repo.findById(map.get("_id").toString());
		assertThat(retrieved.get().someString).isEqualTo(noid.someString);
	}

	static class TypeWithoutIdProperty {

		String someString;
	}

	static interface TypeWithoutExplicitIdPropertyRepository extends MongoRepository<TypeWithoutIdProperty, String> {

		TypeWithoutIdProperty findBySomeString(String someString);
	}
}
