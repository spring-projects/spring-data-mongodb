/*
 * Copyright 2010-2017 the original author or authors.
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

import static org.springframework.data.querydsl.QuerydslUtils.*;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Optional;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.mongodb.repository.query.PartTreeMongoQuery;
import org.springframework.data.mongodb.repository.query.StringBasedMongoQuery;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.reactive.RxJava1CrudRepository;
import org.springframework.data.repository.util.QueryExecutionConverters;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Factory to create {@link MongoRepository} instances.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MongoRepositoryFactory extends RepositoryFactorySupport {

	// TODO: to we need this here and in ReactiveWrappers?
	private static final boolean PROJECT_REACTOR_PRESENT = ClassUtils.isPresent("reactor.core.publisher.Flux",
			QueryExecutionConverters.class.getClassLoader());
	private static final boolean RXJAVA_OBSERVABLE_PRESENT = ClassUtils.isPresent("rx.Observable",
			QueryExecutionConverters.class.getClassLoader());

	private static final SpelExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();

	private final MongoOperations operations;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	/**
	 * Creates a new {@link MongoRepositoryFactory} with the given {@link MongoOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 */
	public MongoRepositoryFactory(MongoOperations mongoOperations) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null!");

		this.operations = mongoOperations;
		this.mappingContext = mongoOperations.getConverter().getMappingContext();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getRepositoryBaseClass(org.springframework.data.repository.core.RepositoryMetadata)
	 */
	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {

		boolean isReactiveRepository = (PROJECT_REACTOR_PRESENT
				&& ReactiveCrudRepository.class.isAssignableFrom(metadata.getRepositoryInterface()))
				|| (RXJAVA_OBSERVABLE_PRESENT
						&& RxJava1CrudRepository.class.isAssignableFrom(metadata.getRepositoryInterface()));

		boolean isQueryDslRepository = QUERY_DSL_PRESENT
				&& QuerydslPredicateExecutor.class.isAssignableFrom(metadata.getRepositoryInterface());

		if (isReactiveRepository) {

			if (isQueryDslRepository) {
				throw new InvalidDataAccessApiUsageException(
						"Cannot combine Querydsl and reactive repository in one interface");
			}
			return SimpleReactiveMongoRepository.class;
		}

		return isQueryDslRepository ? QueryDslMongoRepository.class : SimpleMongoRepository.class;
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
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(Key key,
			EvaluationContextProvider evaluationContextProvider) {
		return Optional.of(new MongoQueryLookupStrategy(operations, evaluationContextProvider, mappingContext));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getEntityInformation(java.lang.Class)
	 */
	public <T, ID> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
		return getEntityInformation(domainClass, null);
	}

	private <T, ID> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass,
			RepositoryInformation information) {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(domainClass);
		return MongoEntityInformationSupport.<T, ID> entityInformationFor(entity,
				information != null ? information.getIdType() : null);
	}

	/**
	 * {@link QueryLookupStrategy} to create {@link PartTreeMongoQuery} instances.
	 * 
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	private static class MongoQueryLookupStrategy implements QueryLookupStrategy {

		private final MongoOperations operations;
		private final EvaluationContextProvider evaluationContextProvider;
		MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

		public MongoQueryLookupStrategy(MongoOperations operations, EvaluationContextProvider evaluationContextProvider,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

			this.operations = operations;
			this.evaluationContextProvider = evaluationContextProvider;
			this.mappingContext = mappingContext;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.QueryLookupStrategy#resolveQuery(java.lang.reflect.Method, org.springframework.data.repository.core.RepositoryMetadata, org.springframework.data.projection.ProjectionFactory, org.springframework.data.repository.core.NamedQueries)
		 */
		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, mappingContext);
			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName)) {
				String namedQuery = namedQueries.getQuery(namedQueryName);
				return new StringBasedMongoQuery(namedQuery, queryMethod, operations, EXPRESSION_PARSER,
						evaluationContextProvider);
			} else if (queryMethod.hasAnnotatedQuery()) {
				return new StringBasedMongoQuery(queryMethod, operations, EXPRESSION_PARSER, evaluationContextProvider);
			} else {
				return new PartTreeMongoQuery(queryMethod, operations);
			}
		}
	}
}
