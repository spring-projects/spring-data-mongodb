/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.ExpressionEvaluatingParameterBinder.BindingContext;
import org.springframework.data.mongodb.repository.query.StringBasedMongoQuery.ParameterBinding;
import org.springframework.data.mongodb.repository.query.StringBasedMongoQuery.ParameterBindingParser;
import org.springframework.data.repository.query.EvaluationContextProvider;
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

	private static final String COUND_AND_DELETE = "Manually defined query for %s cannot be both a count and delete query at the same time!";
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveStringBasedMongoQuery.class);
	private static final ParameterBindingParser BINDING_PARSER = ParameterBindingParser.INSTANCE;

	private final String query;
	private final String fieldSpec;
	private final boolean isCountQuery;
	private final boolean isDeleteQuery;
	private final List<ParameterBinding> queryParameterBindings;
	private final List<ParameterBinding> fieldSpecParameterBindings;
	private final ExpressionEvaluatingParameterBinder parameterBinder;

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
			SpelExpressionParser expressionParser, EvaluationContextProvider evaluationContextProvider) {
		this(method.getAnnotatedQuery(), method, mongoOperations, expressionParser, evaluationContextProvider);
	}

	/**
	 * Creates a new {@link ReactiveStringBasedMongoQuery} for the given {@link String}, {@link MongoQueryMethod},
	 * {@link MongoOperations}, {@link SpelExpressionParser} and {@link EvaluationContextProvider}.
	 *
	 * @param query must not be {@literal null}.
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 */
	public ReactiveStringBasedMongoQuery(String query, ReactiveMongoQueryMethod method,
			ReactiveMongoOperations mongoOperations, SpelExpressionParser expressionParser,
			EvaluationContextProvider evaluationContextProvider) {

		super(method, mongoOperations);

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(expressionParser, "SpelExpressionParser must not be null!");

		this.queryParameterBindings = new ArrayList<ParameterBinding>();
		this.query = BINDING_PARSER.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				this.queryParameterBindings);

		this.fieldSpecParameterBindings = new ArrayList<ParameterBinding>();
		this.fieldSpec = BINDING_PARSER.parseAndCollectParameterBindingsFromQueryIntoBindings(
				method.getFieldSpecification(), this.fieldSpecParameterBindings);

		this.isCountQuery = method.hasAnnotatedQuery() ? method.getQueryAnnotation().count() : false;
		this.isDeleteQuery = method.hasAnnotatedQuery() ? method.getQueryAnnotation().delete() : false;

		if (isCountQuery && isDeleteQuery) {
			throw new IllegalArgumentException(String.format(COUND_AND_DELETE, method));
		}

		this.parameterBinder = new ExpressionEvaluatingParameterBinder(expressionParser, evaluationContextProvider);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#createQuery(org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {

		String queryString = parameterBinder.bind(this.query, accessor,
				new BindingContext(getQueryMethod().getParameters(), queryParameterBindings));
		String fieldsString = parameterBinder.bind(this.fieldSpec, accessor,
				new BindingContext(getQueryMethod().getParameters(), fieldSpecParameterBindings));

		Query query = new BasicQuery(queryString, fieldsString).with(accessor.getSort());

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Created query %s for %s fields.", query.getQueryObject(), query.getFieldsObject()));
		}

		return query;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isCountQuery()
	 */
	@Override
	protected boolean isCountQuery() {
		return isCountQuery;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isDeleteQuery()
	 */
	@Override
	protected boolean isDeleteQuery() {
		return this.isDeleteQuery;
	}

}
