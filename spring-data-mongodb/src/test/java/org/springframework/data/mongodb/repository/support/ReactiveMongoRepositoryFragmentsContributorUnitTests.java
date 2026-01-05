/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.User;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.data.repository.core.support.RepositoryFragment;

/**
 * Unit tests for {@link ReactiveMongoRepositoryFragmentsContributor}.
 *
 * @author Mark Paluch
 */
class ReactiveMongoRepositoryFragmentsContributorUnitTests {

	@Test // GH-4964
	void composedContributorShouldCreateFragments() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		ReactiveMongoOperations operations = mock(ReactiveMongoOperations.class);
		when(operations.getConverter()).thenReturn(converter);

		ReactiveMongoRepositoryFragmentsContributor contributor = ReactiveMongoRepositoryFragmentsContributor.DEFAULT
				.andThen(MyMongoRepositoryFragmentsContributor.INSTANCE);

		RepositoryComposition.RepositoryFragments fragments = contributor.contribute(
				AbstractRepositoryMetadata.getMetadata(QuerydslUserRepository.class),
				new MappingMongoEntityInformation<>(mappingContext.getPersistentEntity(User.class)), operations);

		assertThat(fragments).hasSize(2);

		Iterator<RepositoryFragment<?>> iterator = fragments.iterator();

		RepositoryFragment<?> querydsl = iterator.next();
		assertThat(querydsl.getImplementationClass()).contains(ReactiveQuerydslMongoPredicateExecutor.class);

		RepositoryFragment<?> additional = iterator.next();
		assertThat(additional.getImplementationClass()).contains(MyFragment.class);
	}

	enum MyMongoRepositoryFragmentsContributor implements ReactiveMongoRepositoryFragmentsContributor {

		INSTANCE;

		@Override
		public RepositoryComposition.RepositoryFragments contribute(RepositoryMetadata metadata,
				MongoEntityInformation<?, ?> entityInformation, ReactiveMongoOperations operations) {
			return RepositoryComposition.RepositoryFragments.just(new MyFragment());
		}

		@Override
		public RepositoryComposition.RepositoryFragments describe(RepositoryMetadata metadata) {
			return RepositoryComposition.RepositoryFragments.just(new MyFragment());
		}
	}

	static class MyFragment {

	}

	interface QuerydslUserRepository extends Repository<User, Long>, ReactiveQuerydslPredicateExecutor<User> {}

}
