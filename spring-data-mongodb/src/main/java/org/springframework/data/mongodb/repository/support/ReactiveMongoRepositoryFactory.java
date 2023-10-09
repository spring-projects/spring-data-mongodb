/*
 * Copyright 2016-2023 the original author or authors.
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

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Optional;

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
import org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport;
import org.springframework.data.repository.core.support.RepositoryComposition.RepositoryFragments;
import org.springframework.data.repository.core.support.RepositoryFragment;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
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

	private static final SpelExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();

	private final CrudMethodMetadataPostProcessor crudMethodMetadataPostProcessor = new CrudMethodMetadataPostProcessor();
	private final ReactiveMongoOperations operations;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	/**
	 * Creates a new {@link ReactiveMongoRepositoryFactory} with the given {@link ReactiveMongoOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 */
	public ReactiveMongoRepositoryFactory(ReactiveMongoOperations mongoOperations) {

		Assert.notNull(mongoOperations, "ReactiveMongoOperations must not be null");

		this.operations = mongoOperations;
		this.mappingContext = mongoOperations.getConverter().getMappingContext();

		setEvaluationContextProvider(ReactiveQueryMethodEvaluationContextProvider.DEFAULT);
		addRepositoryProxyPostProcessor(crudMethodMetadataPostProcessor);
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {

		super.setBeanClassLoader(classLoader);
		crudMethodMetadataPostProcessor.setBeanClassLoader(classLoader);
	}

	@Override
	protected ProjectionFactory getProjectionFactory(ClassLoader classLoader, BeanFactory beanFactory) {
		return this.operations.getConverter().getProjectionFactory();
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleReactiveMongoRepository.class;
	}

	@Override
	protected RepositoryFragments getRepositoryFragments(RepositoryMetadata metadata) {

		RepositoryFragments fragments = RepositoryFragments.empty();

		boolean isQueryDslRepository = QUERY_DSL_PRESENT
				&& ReactiveQuerydslPredicateExecutor.class.isAssignableFrom(metadata.getRepositoryInterface());

		if (isQueryDslRepository) {

			MongoEntityInformation<?, Serializable> entityInformation = getEntityInformation(metadata.getDomainType(),
					metadata);

			fragments = fragments.append(RepositoryFragment
					.implemented(instantiateClass(ReactiveQuerydslMongoPredicateExecutor.class, entityInformation, operations)));
		}

		return fragments;
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
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable Key key,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {
		return Optional.of(new MongoQueryLookupStrategy(operations,
				(ReactiveQueryMethodEvaluationContextProvider) evaluationContextProvider, mappingContext));
	}

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
	private static class MongoQueryLookupStrategy implements QueryLookupStrategy {

		private final ReactiveMongoOperations operations;
		private final ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider;
		private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
		private final ExpressionParser expressionParser = new CachingExpressionParser(EXPRESSION_PARSER);

		MongoQueryLookupStrategy(ReactiveMongoOperations operations,
				ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

			this.operations = operations;
			this.evaluationContextProvider = evaluationContextProvider;
			this.mappingContext = mappingContext;
		}

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			ReactiveMongoQueryMethod queryMethod = new ReactiveMongoQueryMethod(method, metadata, factory, mappingContext);
			queryMethod.verify();

			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName)) {
				String namedQuery = namedQueries.getQuery(namedQueryName);
				return new ReactiveStringBasedMongoQuery(namedQuery, queryMethod, operations, expressionParser,
						evaluationContextProvider);
			} else if (queryMethod.hasAnnotatedAggregation()) {
				return new ReactiveStringBasedAggregation(queryMethod, operations, expressionParser, evaluationContextProvider);
			} else if (queryMethod.hasAnnotatedQuery()) {
				return new ReactiveStringBasedMongoQuery(queryMethod, operations, expressionParser, evaluationContextProvider);
			} else {
				return new ReactivePartTreeMongoQuery(queryMethod, operations, expressionParser, evaluationContextProvider);
			}
		}
	}
}
