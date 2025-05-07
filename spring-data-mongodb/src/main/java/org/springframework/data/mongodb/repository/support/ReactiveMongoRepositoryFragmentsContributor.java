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

import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.data.repository.core.support.RepositoryFragmentsContributor;
import org.springframework.util.Assert;

/**
 * Reactive MongoDB-specific {@link RepositoryFragmentsContributor} contributing fragments based on the repository.
 * <p>
 * Implementations must define a no-args constructor.
 *
 * @author Mark Paluch
 * @since 5.0
 * @see ReactiveQuerydslMongoPredicateExecutor
 */
public interface ReactiveMongoRepositoryFragmentsContributor extends RepositoryFragmentsContributor {

	ReactiveMongoRepositoryFragmentsContributor DEFAULT = ReactiveQuerydslContributor.INSTANCE;

	/**
	 * Returns a composed {@code ReactiveMongoRepositoryFragmentsContributor} that first applies this contributor to its
	 * inputs, and then applies the {@code after} contributor concatenating effectively both results. If evaluation of
	 * either contributors throws an exception, it is relayed to the caller of the composed contributor.
	 *
	 * @param after the contributor to apply after this contributor is applied.
	 * @return a composed contributor that first applies this contributor and then applies the {@code after} contributor.
	 */
	default ReactiveMongoRepositoryFragmentsContributor andThen(ReactiveMongoRepositoryFragmentsContributor after) {

		Assert.notNull(after, "ReactiveMongoRepositoryFragmentsContributor must not be null");

		return new ReactiveMongoRepositoryFragmentsContributor() {

			@Override
			public RepositoryComposition.RepositoryFragments contribute(RepositoryMetadata metadata,
					MongoEntityInformation<?, ?> entityInformation, ReactiveMongoOperations operations) {
				return ReactiveMongoRepositoryFragmentsContributor.this.contribute(metadata, entityInformation, operations)
						.append(after.contribute(metadata, entityInformation, operations));
			}

			@Override
			public RepositoryComposition.RepositoryFragments describe(RepositoryMetadata metadata) {
				return ReactiveMongoRepositoryFragmentsContributor.this.describe(metadata).append(after.describe(metadata));
			}
		};
	}

	/**
	 * Creates {@link RepositoryComposition.RepositoryFragments} based on {@link RepositoryMetadata} to add
	 * MongoDB-specific extensions.
	 *
	 * @param metadata repository metadata.
	 * @param entityInformation must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @return {@link RepositoryComposition.RepositoryFragments} to be added to the repository.
	 */
	RepositoryComposition.RepositoryFragments contribute(RepositoryMetadata metadata,
			MongoEntityInformation<?, ?> entityInformation, ReactiveMongoOperations operations);

}
