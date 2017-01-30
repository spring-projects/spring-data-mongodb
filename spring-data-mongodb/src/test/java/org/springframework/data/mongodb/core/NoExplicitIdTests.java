/*
 * Copyright 2015-2017 the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;

/**
 * Integration tests for DATAMONGO-1289.
 *
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class NoExplicitIdTests {

	@Configuration
	@EnableMongoRepositories(considerNestedRepositories = true)
	static class Config extends AbstractMongoConfiguration {

		@Override
		protected String getDatabaseName() {
			return "test";
		}

		@Override
		public Mongo mongo() throws Exception {
			return new MongoClient();
		}
	}

	@Autowired MongoOperations mongoOps;
	@Autowired TypeWithoutExplicitIdPropertyRepository repo;

	@Before
	public void setUp() {
		mongoOps.dropCollection(TypeWithoutIdProperty.class);
	}

	@Test // DATAMONGO-1289
	public void saveAndRetrieveTypeWithoutIdPorpertyViaTemplate() {

		TypeWithoutIdProperty noid = new TypeWithoutIdProperty();
		noid.someString = "o.O";

		mongoOps.save(noid);

		TypeWithoutIdProperty retrieved = mongoOps.findOne(query(where("someString").is(noid.someString)),
				TypeWithoutIdProperty.class);

		assertThat(retrieved.someString, is(noid.someString));
	}

	@Test // DATAMONGO-1289
	public void saveAndRetrieveTypeWithoutIdPorpertyViaRepository() {

		TypeWithoutIdProperty noid = new TypeWithoutIdProperty();
		noid.someString = "o.O";

		repo.save(noid);

		TypeWithoutIdProperty retrieved = repo.findBySomeString(noid.someString);
		assertThat(retrieved.someString, is(noid.someString));
	}

	@Test // DATAMONGO-1289
	@SuppressWarnings("unchecked")
	public void saveAndRetrieveTypeWithoutIdPorpertyViaRepositoryFindOne() {

		TypeWithoutIdProperty noid = new TypeWithoutIdProperty();
		noid.someString = "o.O";

		repo.save(noid);

		Map<String, Object> map = mongoOps.findOne(query(where("someString").is(noid.someString)), Map.class,
				"typeWithoutIdProperty");

		Optional<TypeWithoutIdProperty> retrieved = repo.findOne(map.get("_id").toString());
		assertThat(retrieved.get().someString, is(noid.someString));
	}

	static class TypeWithoutIdProperty {

		String someString;
	}

	static interface TypeWithoutExplicitIdPropertyRepository extends MongoRepository<TypeWithoutIdProperty, String> {

		TypeWithoutIdProperty findBySomeString(String someString);
	}
}
