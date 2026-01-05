/*
 * Copyright 2010-present the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.mongodb.repository.query.PartTreeMongoQuery;
import org.springframework.data.mongodb.repository.query.StringBasedAggregation;
import org.springframework.data.mongodb.repository.query.StringBasedMongoQuery;
import org.springframework.data.mongodb.repository.query.VectorSearchAggregation;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition.RepositoryFragments;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.util.Assert;

/**
 * Factory to create {@link MongoRepository} instances.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MongoRepositoryFactory extends RepositoryFactorySupport {

	private final CrudMethodMetadataPostProcessor crudMethodMetadataPostProcessor = new CrudMethodMetadataPostProcessor();
	private final MongoOperations operations;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private MongoRepositoryFragmentsContributor fragmentsContributor = MongoRepositoryFragmentsContributor.DEFAULT;

	/**
	 * Creates a new {@link MongoRepositoryFactory} with the given {@link MongoOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 */
	public MongoRepositoryFactory(MongoOperations mongoOperations) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null");

		this.operations = mongoOperations;
		this.mappingContext = mongoOperations.getConverter().getMappingContext();

		addRepositoryProxyPostProcessor(crudMethodMetadataPostProcessor);
	}

	/**
	 * Configures the {@link MongoRepositoryFragmentsContributor} to be used. Defaults to
	 * {@link MongoRepositoryFragmentsContributor#DEFAULT}.
	 *
	 * @param fragmentsContributor
	 * @since 5.0
	 */
	public void setFragmentsContributor(MongoRepositoryFragmentsContributor fragmentsContributor) {
		this.fragmentsContributor = fragmentsContributor;
	}

	@Override
	public void setBeanClassLoader(@Nullable ClassLoader classLoader) {

		super.setBeanClassLoader(classLoader);
		crudMethodMetadataPostProcessor.setBeanClassLoader(classLoader);
	}

	@Override
	protected ProjectionFactory getProjectionFactory(@Nullable ClassLoader classLoader, @Nullable BeanFactory beanFactory) {
		return this.operations.getConverter().getProjectionFactory();
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleMongoRepository.class;
	}

	@Override
	protected RepositoryFragments getRepositoryFragments(RepositoryMetadata metadata) {
		return getRepositoryFragments(metadata, operations);
	}

	/**
	 * Creates {@link RepositoryFragments} based on {@link RepositoryMetadata} to add Mongo-specific extensions.
	 * Typically, adds a {@link QuerydslMongoPredicateExecutor} if the repository interface uses Querydsl.
	 * <p>
	 * Built-in fragment contribution can be customized by configuring {@link MongoRepositoryFragmentsContributor}.
	 *
	 * @param metadata repository metadata.
	 * @param operations the MongoDB operations manager.
	 * @return {@link RepositoryFragments} to be added to the repository.
	 * @since 3.2.1
	 */
	protected RepositoryFragments getRepositoryFragments(RepositoryMetadata metadata, MongoOperations operations) {
		return fragmentsContributor.contribute(metadata, getEntityInformation(metadata), operations);
	}

	@Override
	protected Object getTargetRepository(RepositoryInformation information) {

		MongoEntityInformation<?, ?> entityInformation = getEntityInformation(information);
		Object targetRepository = getTargetRepositoryViaReflection(information, entityInformation, operations);

		if (targetRepository instanceof SimpleMongoRepository<?, ?> repository) {
			repository.setRepositoryMethodMetadata(crudMethodMetadataPostProcessor.getCrudMethodMetadata());
		}

		return targetRepository;
	}

	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable Key key,
			ValueExpressionDelegate valueExpressionDelegate) {
		return Optional.of(new MongoQueryLookupStrategy(operations, mappingContext, valueExpressionDelegate));
	}

	@Deprecated
	@Override
	public <T, ID> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(domainClass);
		return MongoEntityInformationSupport.entityInformationFor(entity, null);
	}

	@Override
	public MongoEntityInformation<?, ?> getEntityInformation(RepositoryMetadata metadata) {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(metadata.getDomainType());
		return MongoEntityInformationSupport.entityInformationFor(entity, metadata.getIdType());
	}

	/**
	 * {@link QueryLookupStrategy} to create {@link PartTreeMongoQuery} instances.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	private record MongoQueryLookupStrategy(MongoOperations operations,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
			ValueExpressionDelegate expressionSupport) implements QueryLookupStrategy {

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, mappingContext);
			queryMethod.verify();

			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName)) {
				String namedQuery = namedQueries.getQuery(namedQueryName);
				return new StringBasedMongoQuery(namedQuery, queryMethod, operations, expressionSupport);
			} else if (queryMethod.hasAnnotatedVectorSearch()) {
				return new VectorSearchAggregation(queryMethod, operations, expressionSupport);
			} else if (queryMethod.hasAnnotatedAggregation()) {
				return new StringBasedAggregation(queryMethod, operations, expressionSupport);
			} else if (queryMethod.hasAnnotatedQuery()) {
				return new StringBasedMongoQuery(queryMethod, operations, expressionSupport);
			} else {
				return new PartTreeMongoQuery(queryMethod, operations, expressionSupport);
			}
		}
	}
}
