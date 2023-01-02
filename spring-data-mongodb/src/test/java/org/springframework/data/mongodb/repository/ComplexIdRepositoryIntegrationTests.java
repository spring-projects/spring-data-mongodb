/*
 * Copyright 2014-2023 the original author or authors.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.client.MongoClient;

/**
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
public class ComplexIdRepositoryIntegrationTests {

	static @Client MongoClient mongoClient;

	@Configuration
	@EnableMongoRepositories(includeFilters=@Filter(type = FilterType.ASSIGNABLE_TYPE, classes = UserWithComplexIdRepository.class))
	static class Config extends AbstractMongoClientConfiguration {

		@Override
		protected String getDatabaseName() {
			return "complexIdTest";
		}

		@Override
		public MongoClient mongoClient() {
			return mongoClient;
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.emptySet();
		}
	}

	@Autowired UserWithComplexIdRepository repo;
	@Autowired MongoTemplate template;

	MyId id;
	UserWithComplexId userWithId;

	@BeforeEach
	public void setUp() {

		repo.deleteAll();

		id = new MyId();
		id.val1 = "v1";
		id.val2 = "v2";

		userWithId = new UserWithComplexId();
		userWithId.firstname = "foo";
		userWithId.id = id;
	}

	@Test // DATAMONGO-1078
	public void annotatedFindQueryShouldWorkWhenUsingComplexId() {

		repo.save(userWithId);

		assertThat(repo.getUserByComplexId(id)).isEqualTo(userWithId);
	}

	@Test // DATAMONGO-1078
	public void annotatedFindQueryShouldWorkWhenUsingComplexIdWithinCollection() {

		repo.save(userWithId);

		List<UserWithComplexId> loaded = repo.findByUserIds(Collections.singleton(id));

		assertThat(loaded).hasSize(1);
		assertThat(loaded).containsExactly(userWithId);
	}

	@Test // DATAMONGO-1078
	public void findOneShouldWorkWhenUsingComplexId() {

		repo.save(userWithId);

		assertThat(repo.findById(id)).isEqualTo(Optional.of(userWithId));
	}

	@Test // DATAMONGO-1078
	public void findAllShouldWorkWhenUsingComplexId() {

		repo.save(userWithId);

		Iterable<UserWithComplexId> loaded = repo.findAllById(Collections.singleton(id));

		assertThat(loaded).hasSize(1);
		assertThat(loaded).containsExactly(userWithId);
	}

	@Test // DATAMONGO-1373
	public void composedAnnotationFindQueryShouldWorkWhenUsingComplexId() {

		repo.save(userWithId);

		assertThat(repo.getUserUsingComposedAnnotationByComplexId(id)).isEqualTo(userWithId);
	}

	@Test // DATAMONGO-1373
	public void composedAnnotationFindMetaShouldWorkWhenUsingComplexId() {

		repo.save(userWithId);

		assertThat(repo.findUsersUsingComposedMetaAnnotationByUserIds(Arrays.asList(id))).hasSize(1);
	}

}
