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
package org.springframework.data.mongodb.repository.support;

import static org.springframework.data.querydsl.QuerydslUtils.*;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.data.repository.core.support.RepositoryFragment;
import org.springframework.data.repository.core.support.RepositoryFragmentsContributor;

/**
 * MongoDB-specific {@link RepositoryFragmentsContributor} contributing Querydsl fragments if a repository implements
 * {@link QuerydslPredicateExecutor}.
 *
 * @author Mark Paluch
 * @since 5.0
 * @see QuerydslMongoPredicateExecutor
 */
enum QuerydslContributor implements MongoRepositoryFragmentsContributor {

	INSTANCE;

	@Override
	public RepositoryComposition.RepositoryFragments contribute(RepositoryMetadata metadata,
			MongoEntityInformation<?, ?> entityInformation, MongoOperations operations) {

		if (isQuerydslRepository(metadata)) {

			QuerydslMongoPredicateExecutor<?> executor = new QuerydslMongoPredicateExecutor<>(entityInformation, operations);

			return RepositoryComposition.RepositoryFragments
					.of(RepositoryFragment.implemented(QuerydslPredicateExecutor.class, executor));
		}

		return RepositoryComposition.RepositoryFragments.empty();
	}

	@Override
	public RepositoryComposition.RepositoryFragments describe(RepositoryMetadata metadata) {

		if (isQuerydslRepository(metadata)) {
			return RepositoryComposition.RepositoryFragments
					.of(RepositoryFragment.structural(QuerydslPredicateExecutor.class, QuerydslMongoPredicateExecutor.class));
		}

		return RepositoryComposition.RepositoryFragments.empty();
	}

	private static boolean isQuerydslRepository(RepositoryMetadata metadata) {
		return QUERY_DSL_PRESENT && QuerydslPredicateExecutor.class.isAssignableFrom(metadata.getRepositoryInterface());
	}

}
