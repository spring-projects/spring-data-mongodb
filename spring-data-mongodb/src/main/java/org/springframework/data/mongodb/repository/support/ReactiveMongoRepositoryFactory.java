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
import java.lang.reflect.Method;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.query.PartTreeMongoQuery;
import org.springframework.data.mongodb.repository.query.ReactiveMongoQueryMethod;
import org.springframework.data.mongodb.repository.query.ReactivePartTreeMongoQuery;
import org.springframework.data.mongodb.repository.query.ReactiveStringBasedAggregation;
import org.springframework.data.mongodb.repository.query.ReactiveStringBasedMongoQuery;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport;
import org.springframework.data.repository.core.support.RepositoryComposition.RepositoryFragments;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.util.Assert;

/**
 * Factory to create {@link org.springframework.data.mongodb.repository.ReactiveMongoRepository} instances.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 2.0
 */
public class ReactiveMongoRepositoryFactory extends ReactiveRepositoryFactorySupport {

	private final CrudMethodMetadataPostProcessor crudMethodMetadataPostProcessor = new CrudMethodMetadataPostProcessor();
	private final ReactiveMongoOperations operations;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private ReactiveMongoRepositoryFragmentsContributor fragmentsContributor = ReactiveMongoRepositoryFragmentsContributor.DEFAULT;
	@Nullable private QueryMethodValueEvaluationContextAccessor accessor;

	/**
	 * Creates a new {@link ReactiveMongoRepositoryFactory} with the given {@link ReactiveMongoOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 */
	public ReactiveMongoRepositoryFactory(ReactiveMongoOperations mongoOperations) {

		Assert.notNull(mongoOperations, "ReactiveMongoOperations must not be null");

		this.operations = mongoOperations;
		this.mappingContext = mongoOperations.getConverter().getMappingContext();

		addRepositoryProxyPostProcessor(crudMethodMetadataPostProcessor);
	}

	/**
	 * Configures the {@link ReactiveMongoRepositoryFragmentsContributor} to be used. Defaults to
	 * {@link ReactiveMongoRepositoryFragmentsContributor#DEFAULT}.
	 *
	 * @param fragmentsContributor
	 * @since 5.0
	 */
	public void setFragmentsContributor(ReactiveMongoRepositoryFragmentsContributor fragmentsContributor) {
		this.fragmentsContributor = fragmentsContributor;
	}

	@Override
	public void setBeanClassLoader(@Nullable ClassLoader classLoader) {

		super.setBeanClassLoader(classLoader);
		crudMethodMetadataPostProcessor.setBeanClassLoader(classLoader);
	}

	@Override
	@SuppressWarnings("NullAway")
	protected ProjectionFactory getProjectionFactory(@Nullable ClassLoader classLoader, @Nullable BeanFactory beanFactory) {
		return this.operations.getConverter().getProjectionFactory();
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleReactiveMongoRepository.class;
	}

	/**
	 * Creates {@link RepositoryFragments} based on {@link RepositoryMetadata} to add Mongo-specific extensions.
	 * Typically, adds a {@link ReactiveQuerydslContributor} if the repository interface uses Querydsl.
	 * <p>
	 * Built-in fragment contribution can be customized by configuring
	 * {@link ReactiveMongoRepositoryFragmentsContributor}.
	 *
	 * @param metadata repository metadata.
	 * @return {@link RepositoryFragments} to be added to the repository.
	 */
	@Override
	protected RepositoryFragments getRepositoryFragments(RepositoryMetadata metadata) {
		return fragmentsContributor.contribute(metadata, getEntityInformation(metadata.getDomainType(), metadata),
				operations);
	}

	@Override
	protected Object getTargetRepository(RepositoryInformation information) {

		MongoEntityInformation<?, Serializable> entityInformation = getEntityInformation(information.getDomainType(),
				information);
		Object targetRepository = getTargetRepositoryViaReflection(information, entityInformation, operations);

		if (targetRepository instanceof SimpleReactiveMongoRepository<?, ?> repository) {
			repository.setRepositoryMethodMetadata(crudMethodMetadataPostProcessor.getCrudMethodMetadata());
		}

		return targetRepository;
	}

	@Override
	@SuppressWarnings("NullAway")
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(Key key,
			ValueExpressionDelegate valueExpressionDelegate) {
		return Optional.of(new MongoQueryLookupStrategy(operations, mappingContext, valueExpressionDelegate));
	}

	@Override
	public <T, ID> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
		return getEntityInformation(domainClass, null);
	}

	@SuppressWarnings("unchecked")
	private <T, ID> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass,
			@Nullable RepositoryMetadata metadata) {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(domainClass);

		return new MappingMongoEntityInformation<>((MongoPersistentEntity<T>) entity,
				metadata != null ? (Class<ID>) metadata.getIdType() : null);
	}

	/**
	 * {@link QueryLookupStrategy} to create {@link PartTreeMongoQuery} instances.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	private record MongoQueryLookupStrategy(ReactiveMongoOperations operations,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
			ValueExpressionDelegate delegate) implements QueryLookupStrategy {

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			ReactiveMongoQueryMethod queryMethod = new ReactiveMongoQueryMethod(method, metadata, factory, mappingContext);
			queryMethod.verify();

			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName)) {
				String namedQuery = namedQueries.getQuery(namedQueryName);
				return new ReactiveStringBasedMongoQuery(namedQuery, queryMethod, operations, delegate);
			} else if (queryMethod.hasAnnotatedAggregation()) {
				return new ReactiveStringBasedAggregation(queryMethod, operations, delegate);
			} else if (queryMethod.hasAnnotatedQuery()) {
				return new ReactiveStringBasedMongoQuery(queryMethod, operations, delegate);
			} else {
				return new ReactivePartTreeMongoQuery(queryMethod, operations, delegate);
			}
		}
	}
}
