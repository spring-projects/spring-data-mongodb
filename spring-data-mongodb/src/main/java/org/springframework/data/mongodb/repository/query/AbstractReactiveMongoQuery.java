/*
 * Copyright 2016-2019 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.bson.Document;
import org.reactivestreams.Publisher;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.ReactiveFindOperation.FindWithProjection;
import org.springframework.data.mongodb.core.ReactiveFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.ReactiveFindOperation.TerminatingFind;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.ReactiveMongoQueryExecution.DeleteExecution;
import org.springframework.data.mongodb.repository.query.ReactiveMongoQueryExecution.GeoNearExecution;
import org.springframework.data.mongodb.repository.query.ReactiveMongoQueryExecution.ResultProcessingConverter;
import org.springframework.data.mongodb.repository.query.ReactiveMongoQueryExecution.ResultProcessingExecution;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

/**
 * Base class for reactive {@link RepositoryQuery} implementations for MongoDB.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public abstract class AbstractReactiveMongoQuery implements RepositoryQuery {

	private final ReactiveMongoQueryMethod method;
	private final ReactiveMongoOperations operations;
	private final EntityInstantiators instantiators;
	private final FindWithProjection<?> findOperationWithProjection;
	private final SpelExpressionParser expressionParser;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;

	/**
	 * Creates a new {@link AbstractReactiveMongoQuery} from the given {@link MongoQueryMethod} and
	 * {@link MongoOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public AbstractReactiveMongoQuery(ReactiveMongoQueryMethod method, ReactiveMongoOperations operations,
			SpelExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {

		Assert.notNull(method, "MongoQueryMethod must not be null!");
		Assert.notNull(operations, "ReactiveMongoOperations must not be null!");
		Assert.notNull(expressionParser, "SpelExpressionParser must not be null!");
		Assert.notNull(evaluationContextProvider, "QueryMethodEvaluationContextProvider must not be null!");

		this.method = method;
		this.operations = operations;
		this.instantiators = new EntityInstantiators();
		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;

		MongoEntityMetadata<?> metadata = method.getEntityInformation();
		Class<?> type = metadata.getCollectionEntity().getType();

		this.findOperationWithProjection = operations.query(type);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	public MongoQueryMethod getQueryMethod() {
		return method;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	public Object execute(Object[] parameters) {

		return method.hasReactiveWrapperParameter() ? executeDeferred(parameters)
				: execute(new MongoParametersParameterAccessor(method, parameters));
	}

	@SuppressWarnings("unchecked")
	private Object executeDeferred(Object[] parameters) {

		ReactiveMongoParameterAccessor parameterAccessor = new ReactiveMongoParameterAccessor(method, parameters);

		if (getQueryMethod().isCollectionQuery()) {
			return Flux.defer(() -> (Publisher<Object>) execute(parameterAccessor));
		}

		return Mono.defer(() -> (Mono<Object>) execute(parameterAccessor));
	}

	private Object execute(MongoParameterAccessor parameterAccessor) {

		ConvertingParameterAccessor convertingParamterAccessor = new ConvertingParameterAccessor(operations.getConverter(),
				parameterAccessor);
		Query query = createQuery(convertingParamterAccessor);

		applyQueryMetaAttributesWhenPresent(query);
		query = applyAnnotatedDefaultSortIfPresent(query);
		query = applyAnnotatedCollationIfPresent(query, convertingParamterAccessor);

		ResultProcessor processor = method.getResultProcessor().withDynamicProjection(convertingParamterAccessor);
		Class<?> typeToRead = processor.getReturnedType().getTypeToRead();

		FindWithQuery<?> find = typeToRead == null //
				? findOperationWithProjection //
				: findOperationWithProjection.as(typeToRead);

		String collection = method.getEntityInformation().getCollectionName();

		ReactiveMongoQueryExecution execution = getExecution(convertingParamterAccessor,
				new ResultProcessingConverter(processor, operations, instantiators), find);

		return execution.execute(query, processor.getReturnedType().getDomainType(), collection);
	}

	/**
	 * Returns the execution instance to use.
	 *
	 * @param accessor must not be {@literal null}.
	 * @param resultProcessing must not be {@literal null}.
	 * @return
	 */
	private ReactiveMongoQueryExecution getExecution(MongoParameterAccessor accessor,
			Converter<Object, Object> resultProcessing, FindWithQuery<?> operation) {
		return new ResultProcessingExecution(getExecutionToWrap(accessor, operation), resultProcessing);
	}

	private ReactiveMongoQueryExecution getExecutionToWrap(MongoParameterAccessor accessor, FindWithQuery<?> operation) {

		if (isDeleteQuery()) {
			return new DeleteExecution(operations, method);
		} else if (method.isGeoNearQuery()) {
			return new GeoNearExecution(operations, accessor, method.getReturnType());
		} else if (isTailable(method)) {
			return (q, t, c) -> operation.matching(q.with(accessor.getPageable())).tail();
		} else if (method.isCollectionQuery()) {
			return (q, t, c) -> operation.matching(q.with(accessor.getPageable())).all();
		} else if (isCountQuery()) {
			return (q, t, c) -> operation.matching(q).count();
		} else if (isExistsQuery()) {
			return (q, t, c) -> operation.matching(q).exists();
		} else {
			return (q, t, c) -> {

				TerminatingFind<?> find = operation.matching(q);

				if (isCountQuery()) {
					return find.count();
				}

				return isLimiting() ? find.first() : find.one();
			};
		}
	}

	private boolean isTailable(MongoQueryMethod method) {
		return method.getTailableAnnotation() != null;
	}

	Query applyQueryMetaAttributesWhenPresent(Query query) {

		if (method.hasQueryMetaAttributes()) {
			query.setMeta(method.getQueryMetaAttributes());
		}

		return query;
	}

	/**
	 * Add a default sort derived from {@link org.springframework.data.mongodb.repository.Query#sort()} to the given
	 * {@link Query} if present.
	 *
	 * @param query the {@link Query} to potentially apply the sort to.
	 * @return the query with potential default sort applied.
	 * @since 2.1
	 */
	Query applyAnnotatedDefaultSortIfPresent(Query query) {

		if (!method.hasAnnotatedSort()) {
			return query;
		}

		return QueryUtils.decorateSort(query, Document.parse(method.getAnnotatedSort()));
	}

	/**
	 * If present apply a {@link org.springframework.data.mongodb.core.query.Collation} derived from the
	 * {@link org.springframework.data.repository.query.QueryMethod} the given {@link Query}.
	 *
	 * @param query must not be {@literal null}.
	 * @param accessor the {@link ParameterAccessor} used to obtain parameter placeholder replacement values.
	 * @return
	 * @since 2.2
	 */
	Query applyAnnotatedCollationIfPresent(Query query, ConvertingParameterAccessor accessor) {

		return QueryUtils.applyCollation(query, method.hasAnnotatedCollation() ? method.getAnnotatedCollation() : null,
				accessor, getQueryMethod().getParameters(), expressionParser, evaluationContextProvider);
	}

	/**
	 * Creates a {@link Query} instance using the given {@link ConvertingParameterAccessor}. Will delegate to
	 * {@link #createQuery(ConvertingParameterAccessor)} by default but allows customization of the count query to be
	 * triggered.
	 *
	 * @param accessor must not be {@literal null}.
	 * @return
	 */
	protected Query createCountQuery(ConvertingParameterAccessor accessor) {
		return applyQueryMetaAttributesWhenPresent(createQuery(accessor));
	}

	/**
	 * Creates a {@link Query} instance using the given {@link ParameterAccessor}
	 *
	 * @param accessor must not be {@literal null}.
	 * @return
	 */
	protected abstract Query createQuery(ConvertingParameterAccessor accessor);

	/**
	 * Returns whether the query should get a count projection applied.
	 *
	 * @return
	 */
	protected abstract boolean isCountQuery();

	/**
	 * Returns whether the query should get an exists projection applied.
	 *
	 * @return
	 * @since 2.0.9
	 */
	protected abstract boolean isExistsQuery();

	/**
	 * Return weather the query should delete matching documents.
	 *
	 * @return
	 * @since 1.5
	 */
	protected abstract boolean isDeleteQuery();

	/**
	 * Return whether the query has an explicit limit set.
	 *
	 * @return
	 * @since 2.0.4
	 */
	protected abstract boolean isLimiting();
}
