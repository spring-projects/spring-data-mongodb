/*
 * Copyright 2016-2020 the original author or authors.
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

import reactor.core.publisher.Mono;

import java.util.Optional;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.repository.query.ReactiveExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

/**
 * Query to use a plain JSON String to create the {@link Query} to actually execute.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public class ReactiveStringBasedMongoQuery extends AbstractReactiveMongoQuery {

	private static final String COUNT_EXISTS_AND_DELETE = "Manually defined query for %s cannot be a count and exists or delete query at the same time!";
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveStringBasedMongoQuery.class);
	private static final ParameterBindingDocumentCodec CODEC = new ParameterBindingDocumentCodec();

	private final String query;
	private final String fieldSpec;

	private final ExpressionParser expressionParser;
	private final ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider;

	private final boolean isCountQuery;
	private final boolean isExistsQuery;
	private final boolean isDeleteQuery;

	/**
	 * Creates a new {@link ReactiveStringBasedMongoQuery} for the given {@link MongoQueryMethod} and
	 * {@link MongoOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public ReactiveStringBasedMongoQuery(ReactiveMongoQueryMethod method, ReactiveMongoOperations mongoOperations,
			ExpressionParser expressionParser, ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider) {
		this(method.getAnnotatedQuery(), method, mongoOperations, expressionParser, evaluationContextProvider);
	}

	/**
	 * Creates a new {@link ReactiveStringBasedMongoQuery} for the given {@link String}, {@link MongoQueryMethod},
	 * {@link MongoOperations}, {@link SpelExpressionParser} and
	 * {@link ReactiveExtensionAwareQueryMethodEvaluationContextProvider}.
	 *
	 * @param query must not be {@literal null}.
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 */
	public ReactiveStringBasedMongoQuery(String query, ReactiveMongoQueryMethod method,
			ReactiveMongoOperations mongoOperations, ExpressionParser expressionParser,
			ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(method, mongoOperations, expressionParser, evaluationContextProvider);

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(expressionParser, "SpelExpressionParser must not be null!");

		this.query = query;
		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;
		this.fieldSpec = method.getFieldSpecification();

		if (method.hasAnnotatedQuery()) {

			org.springframework.data.mongodb.repository.Query queryAnnotation = method.getQueryAnnotation();

			this.isCountQuery = queryAnnotation.count();
			this.isExistsQuery = queryAnnotation.exists();
			this.isDeleteQuery = queryAnnotation.delete();

			if (hasAmbiguousProjectionFlags(this.isCountQuery, this.isExistsQuery, this.isDeleteQuery)) {
				throw new IllegalArgumentException(String.format(COUNT_EXISTS_AND_DELETE, method));
			}

		} else {

			this.isCountQuery = false;
			this.isExistsQuery = false;
			this.isDeleteQuery = false;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#createQuery(org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Mono<Query> createQuery(ConvertingParameterAccessor accessor) {

		Mono<Document> queryObject = getBindingContext(accessor, expressionParser, this.query)
				.map(it -> CODEC.decode(this.query, it));
		Mono<Document> fieldsObject = getBindingContext(accessor, expressionParser, this.fieldSpec)
				.map(it -> CODEC.decode(this.fieldSpec, it));

		return queryObject.zipWith(fieldsObject).map(tuple -> {

			Query query = new BasicQuery(tuple.getT1(), tuple.getT2()).with(accessor.getSort());

			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Created query %s for %s fields.", query.getQueryObject(), query.getFieldsObject()));
			}

			return query;
		});
	}

	private Mono<ParameterBindingContext> getBindingContext(ConvertingParameterAccessor accessor,
			ExpressionParser expressionParser, String json) {

		Optional<ExpressionDependencies> dependencies = CODEC.getExpressionDependencies(json, accessor::getBindableValue,
				expressionParser);

		Mono<SpELExpressionEvaluator> evaluator = dependencies
				.map(it -> evaluationContextProvider.getEvaluationContextLater(getQueryMethod().getParameters(),
						accessor.getValues(), it))
				.map(evaluationContext -> evaluationContext
						.map(it -> (SpELExpressionEvaluator) new DefaultSpELExpressionEvaluator(expressionParser, it)))
				.orElseGet(() -> Mono.just(DefaultSpELExpressionEvaluator.unsupported()));

		return evaluator.map(it -> new ParameterBindingContext(accessor::getBindableValue, it));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#isCountQuery()
	 */
	@Override
	protected boolean isCountQuery() {
		return isCountQuery;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#isExistsQuery()
	 */
	@Override
	protected boolean isExistsQuery() {
		return isExistsQuery;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#isDeleteQuery()
	 */
	@Override
	protected boolean isDeleteQuery() {
		return this.isDeleteQuery;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#isLimiting()
	 */
	@Override
	protected boolean isLimiting() {
		return false;
	}

	private static boolean hasAmbiguousProjectionFlags(boolean isCountQuery, boolean isExistsQuery,
			boolean isDeleteQuery) {
		return BooleanUtil.countBooleanTrueValues(isCountQuery, isExistsQuery, isDeleteQuery) > 1;
	}

}
