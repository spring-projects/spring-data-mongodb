/*
 * Copyright 2016-2021 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

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
	private static final Log LOG = LogFactory.getLog(ReactiveStringBasedMongoQuery.class);

	private final String query;
	private final String fieldSpec;

	private final ExpressionParser expressionParser;

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

	@Override
	protected Mono<Query> createQuery(ConvertingParameterAccessor accessor) {

		return getCodecRegistry().map(ParameterBindingDocumentCodec::new).flatMap(codec -> {

			Mono<Document> queryObject = getBindingContext(query, accessor, codec)
					.map(context -> codec.decode(query, context));
			Mono<Document> fieldsObject = getBindingContext(fieldSpec, accessor, codec)
					.map(context -> codec.decode(fieldSpec, context));

			return queryObject.zipWith(fieldsObject).map(tuple -> {

				Query query = new BasicQuery(tuple.getT1(), tuple.getT2()).with(accessor.getSort());

				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Created query %s for %s fields.", query.getQueryObject(), query.getFieldsObject()));
				}

				return query;
			});
		});
	}

	private Mono<ParameterBindingContext> getBindingContext(String json, ConvertingParameterAccessor accessor,
			ParameterBindingDocumentCodec codec) {

		ExpressionDependencies dependencies = codec.captureExpressionDependencies(json, accessor::getBindableValue,
				expressionParser);

		return getSpelEvaluatorFor(dependencies, accessor)
				.map(it -> new ParameterBindingContext(accessor::getBindableValue, it));
	}

	@Override
	protected boolean isCountQuery() {
		return isCountQuery;
	}

	@Override
	protected boolean isExistsQuery() {
		return isExistsQuery;
	}

	@Override
	protected boolean isDeleteQuery() {
		return this.isDeleteQuery;
	}

	@Override
	protected boolean isLimiting() {
		return false;
	}

	private static boolean hasAmbiguousProjectionFlags(boolean isCountQuery, boolean isExistsQuery,
			boolean isDeleteQuery) {
		return BooleanUtil.countBooleanTrueValues(isCountQuery, isExistsQuery, isDeleteQuery) > 1;
	}

}
