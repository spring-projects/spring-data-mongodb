/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.reactivestreams.Publisher;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.mongodb.repository.query.PartTreeMongoQuery;
import org.springframework.data.mongodb.repository.query.ReactiveMongoQueryMethod;
import org.springframework.data.mongodb.repository.query.ReactivePartTreeMongoQuery;
import org.springframework.data.mongodb.repository.query.ReactiveStringBasedMongoQuery;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.util.QueryExecutionConverters;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.data.repository.util.ReactiveWrappers;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Factory to create {@link org.springframework.data.mongodb.repository.ReactiveMongoRepository} instances.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public class ReactiveMongoRepositoryFactory extends RepositoryFactorySupport {

	private static final SpelExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();

	private final ReactiveMongoOperations operations;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final ConversionService conversionService;

	/**
	 * Creates a new {@link ReactiveMongoRepositoryFactory} with the given {@link ReactiveMongoOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 */
	public ReactiveMongoRepositoryFactory(ReactiveMongoOperations mongoOperations) {

		Assert.notNull(mongoOperations);

		this.operations = mongoOperations;
		this.mappingContext = mongoOperations.getConverter().getMappingContext();

		DefaultConversionService conversionService = new DefaultConversionService();
		QueryExecutionConverters.registerConvertersIn(conversionService);
		this.conversionService = conversionService;
		setConversionService(conversionService);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getRepositoryBaseClass(org.springframework.data.repository.core.RepositoryMetadata)
	 */
	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleReactiveMongoRepository.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getTargetRepository(org.springframework.data.repository.core.RepositoryInformation)
	 */
	@Override
	protected Object getTargetRepository(RepositoryInformation information) {

		MongoEntityInformation<?, Serializable> entityInformation = getEntityInformation(information.getDomainType(),
				information);
		return getTargetRepositoryViaReflection(information, entityInformation, operations);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getQueryLookupStrategy(org.springframework.data.repository.query.QueryLookupStrategy.Key, org.springframework.data.repository.query.EvaluationContextProvider)
	 */
	@Override
	protected QueryLookupStrategy getQueryLookupStrategy(Key key, EvaluationContextProvider evaluationContextProvider) {
		return new MongoQueryLookupStrategy(operations, evaluationContextProvider, mappingContext, conversionService);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getEntityInformation(java.lang.Class)
	 */
	public <T, ID extends Serializable> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
		return getEntityInformation(domainClass, null);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#validate(org.springframework.data.repository.core.RepositoryMetadata)
	 */
	@Override
	protected void validate(RepositoryMetadata repositoryMetadata) {

		if (!ReactiveWrappers.isAvailable()) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Cannot implement Repository %s without reactive library support.",
							repositoryMetadata.getRepositoryInterface().getName()));
		}

		Arrays.stream(repositoryMetadata.getRepositoryInterface().getMethods())
				.forEach(ReactiveMongoRepositoryFactory::validate);
	}

	/**
	 * Reactive MongoDB support requires reactive wrapper support. If return type/parameters are reactive wrapper types,
	 * then it's required to be able to convert these into Publisher.
	 *
	 * @param method the method to validate.
	 */
	private static void validate(Method method) {

		if (ReactiveWrappers.supports(method.getReturnType())
				&& !ClassUtils.isAssignable(Publisher.class, method.getReturnType())) {

			if (!ReactiveWrapperConverters.supports(method.getReturnType())) {

				throw new InvalidDataAccessApiUsageException(
						String.format("No reactive type converter found for type %s used in %s, method %s.",
								method.getReturnType().getName(), method.getDeclaringClass().getName(), method));
			}
		}

		Arrays.stream(method.getParameterTypes()) //
				.filter(ReactiveWrappers::supports) //
				.filter(parameterType -> !ClassUtils.isAssignable(Publisher.class, parameterType)) //
				.filter(parameterType -> !ReactiveWrapperConverters.supports(parameterType)) //
				.forEach(parameterType -> {
					throw new InvalidDataAccessApiUsageException(
							String.format("No reactive type converter found for type %s used in %s, method %s.",
									parameterType.getName(), method.getDeclaringClass().getName(), method));
				});
	}

	@SuppressWarnings("unchecked")
	private <T, ID extends Serializable> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass,
			RepositoryInformation information) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(domainClass);

		if (entity == null) {
			throw new MappingException(
					String.format("Could not lookup mapping metadata for domain class %s!", domainClass.getName()));
		}

		return new MappingMongoEntityInformation<T, ID>((MongoPersistentEntity<T>) entity,
				information != null ? (Class<ID>) information.getIdType() : null);
	}

	/**
	 * {@link QueryLookupStrategy} to create {@link PartTreeMongoQuery} instances.
	 *
	 * @author Mark Paluch
	 */
	private static class MongoQueryLookupStrategy implements QueryLookupStrategy {

		private final ReactiveMongoOperations operations;
		private final EvaluationContextProvider evaluationContextProvider;
		MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
		final ConversionService conversionService;

		MongoQueryLookupStrategy(ReactiveMongoOperations operations, EvaluationContextProvider evaluationContextProvider,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
				ConversionService conversionService) {

			this.operations = operations;
			this.evaluationContextProvider = evaluationContextProvider;
			this.mappingContext = mappingContext;
			this.conversionService = conversionService;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.QueryLookupStrategy#resolveQuery(java.lang.reflect.Method, org.springframework.data.repository.core.RepositoryMetadata, org.springframework.data.projection.ProjectionFactory, org.springframework.data.repository.core.NamedQueries)
		 */
		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			ReactiveMongoQueryMethod queryMethod = new ReactiveMongoQueryMethod(method, metadata, factory, mappingContext);
			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName)) {
				String namedQuery = namedQueries.getQuery(namedQueryName);
				return new ReactiveStringBasedMongoQuery(namedQuery, queryMethod, operations, EXPRESSION_PARSER,
						evaluationContextProvider, conversionService);
			} else if (queryMethod.hasAnnotatedQuery()) {
				return new ReactiveStringBasedMongoQuery(queryMethod, operations, EXPRESSION_PARSER, evaluationContextProvider,
						conversionService);
			} else {
				return new ReactivePartTreeMongoQuery(queryMethod, operations, conversionService);
			}
		}
	}
}
