/*
 * Copyright 2016-2025 the original author or authors.
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

import java.io.Serializable;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.index.IndexOperationsAdapter;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.beans.factory.FactoryBean} to create
 * {@link org.springframework.data.mongodb.repository.ReactiveMongoRepository} instances.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 * @see org.springframework.data.repository.reactive.ReactiveSortingRepository
 * @see org.springframework.data.repository.reactive.RxJava3SortingRepository
 */
public class ReactiveMongoRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
		extends RepositoryFactoryBeanSupport<T, S, ID> {

	private @Nullable ReactiveMongoOperations operations;
	private ReactiveMongoRepositoryFragmentsContributor repositoryFragmentsContributor = ReactiveMongoRepositoryFragmentsContributor.DEFAULT;
	private boolean createIndexesForQueryMethods = false;
	private boolean mappingContextConfigured = false;

	/**
	 * Creates a new {@link ReactiveMongoRepositoryFactoryBean} for the given repository interface.
	 *
	 * @param repositoryInterface must not be {@literal null}.
	 */
	public ReactiveMongoRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
		super(repositoryInterface);
	}

	/**
	 * Configures the {@link ReactiveMongoOperations} to be used.
	 *
	 * @param operations the operations to set
	 */
	public void setReactiveMongoOperations(@Nullable ReactiveMongoOperations operations) {
		this.operations = operations;
	}

	@Override
	public ReactiveMongoRepositoryFragmentsContributor getRepositoryFragmentsContributor() {
		return repositoryFragmentsContributor;
	}

	/**
	 * Configures the {@link MongoRepositoryFragmentsContributor} to contribute built-in fragment functionality to the
	 * repository.
	 *
	 * @param repositoryFragmentsContributor must not be {@literal null}.
	 * @since 5.0
	 */
	public void setRepositoryFragmentsContributor(
			ReactiveMongoRepositoryFragmentsContributor repositoryFragmentsContributor) {
		this.repositoryFragmentsContributor = repositoryFragmentsContributor;
	}

	/**
	 * Configures whether to automatically create indexes for the properties referenced in a query method.
	 *
	 * @param createIndexesForQueryMethods the createIndexesForQueryMethods to set
	 */
	public void setCreateIndexesForQueryMethods(boolean createIndexesForQueryMethods) {
		this.createIndexesForQueryMethods = createIndexesForQueryMethods;
	}

	@Override
	public void setMappingContext(MappingContext<?, ?> mappingContext) {

		super.setMappingContext(mappingContext);
		this.mappingContextConfigured = true;
	}

	@Override
	@SuppressWarnings("NullAway")
	protected RepositoryFactorySupport createRepositoryFactory() {

		ReactiveMongoRepositoryFactory factory = getFactoryInstance(operations);
		factory.setFragmentsContributor(repositoryFragmentsContributor);

		if (createIndexesForQueryMethods) {
			factory.addQueryCreationListener(new IndexEnsuringQueryCreationListener(
					(collectionName, javaType) -> IndexOperationsAdapter.blocking(operations.indexOps(javaType))));
		}

		return factory;
	}

	/**
	 * Creates and initializes a {@link RepositoryFactorySupport} instance.
	 *
	 * @param operations
	 * @return
	 */
	protected ReactiveMongoRepositoryFactory getFactoryInstance(ReactiveMongoOperations operations) {
		return new ReactiveMongoRepositoryFactory(operations);
	}

	@Override
	public void afterPropertiesSet() {

		super.afterPropertiesSet();
		Assert.state(operations != null, "ReactiveMongoOperations must not be null");

		if (!mappingContextConfigured) {
			setMappingContext(operations.getConverter().getMappingContext());
		}
	}
}
