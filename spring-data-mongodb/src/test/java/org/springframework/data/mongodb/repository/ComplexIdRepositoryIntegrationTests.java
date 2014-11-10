/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;

/**
 * @author Christoph Strobl
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ComplexIdRepositoryIntegrationTests {

	@Configuration
	@EnableMongoRepositories
	static class Config extends AbstractMongoConfiguration {

		@Override
		protected String getDatabaseName() {
			return "complexIdTest";
		}

		@Override
		public Mongo mongo() throws Exception {
			return new MongoClient();
		}

	}

	@Autowired UserWithComplexIdRepository repo;
	@Autowired MongoTemplate template;

	MyId id;
	UserWithComplexId userWithId;

	@Before
	public void setUp() {

		repo.deleteAll();

		id = new MyId();
		id.val1 = "v1";
		id.val2 = "v2";

		userWithId = new UserWithComplexId();
		userWithId.firstname = "foo";
		userWithId.id = id;
	}

	/**
	 * @see DATAMONGO-1078
	 */
	@Test
	public void annotatedFindQueryShouldWorkWhenUsingComplexId() {

		repo.save(userWithId);

		assertThat(repo.getUserByComplexId(id), is(userWithId));
	}

	/**
	 * @see DATAMONGO-1078
	 */
	@Test
	public void annotatedFindQueryShouldWorkWhenUsingComplexIdWithinCollection() {

		repo.save(userWithId);

		List<UserWithComplexId> loaded = repo.findByUserIds(Collections.singleton(id));

		assertThat(loaded, hasSize(1));
		assertThat(loaded, contains(userWithId));
	}

	/**
	 * @see DATAMONGO-1078
	 */
	@Test
	public void findOneShouldWorkWhenUsingComplexId() {

		repo.save(userWithId);

		assertThat(repo.findOne(id), is(userWithId));
	}

	/**
	 * @see DATAMONGO-1078
	 */
	@Test
	public void findAllShouldWorkWhenUsingComplexId() {

		repo.save(userWithId);

		Iterable<UserWithComplexId> loaded = repo.findAll(Collections.singleton(id));

		assertThat(loaded, is(Matchers.<UserWithComplexId> iterableWithSize(1)));
		assertThat(loaded, contains(userWithId));
	}
}
