/*
 * Copyright 2011-2021 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;

/**
 * Query to use a plain JSON String to create the {@link Query} to actually execute.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class StringBasedMongoQuery extends AbstractMongoQuery {

	private static final String COUNT_EXISTS_AND_DELETE = "Manually defined query for %s cannot be a count and exists or delete query at the same time!";
	private static final Log LOG = LogFactory.getLog(StringBasedMongoQuery.class);

	private final String query;
	private final String fieldSpec;

	private final boolean isCountQuery;
	private final boolean isExistsQuery;
	private final boolean isDeleteQuery;

	/**
	 * Creates a new {@link StringBasedMongoQuery} for the given {@link MongoQueryMethod}, {@link MongoOperations},
	 * {@link SpelExpressionParser} and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public StringBasedMongoQuery(MongoQueryMethod method, MongoOperations mongoOperations,
			ExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {
		this(method.getAnnotatedQuery(), method, mongoOperations, expressionParser, evaluationContextProvider);
	}

	/**
	 * Creates a new {@link StringBasedMongoQuery} for the given {@link String}, {@link MongoQueryMethod},
	 * {@link MongoOperations}, {@link SpelExpressionParser} and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param query must not be {@literal null}.
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 */
	public StringBasedMongoQuery(String query, MongoQueryMethod method, MongoOperations mongoOperations,
			ExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(method, mongoOperations, expressionParser, evaluationContextProvider);

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(expressionParser, "SpelExpressionParser must not be null!");

		this.query = query;
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
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#createQuery(org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {

		Document queryObject = decode(this.query, prepareBindingContext(this.query, accessor));
		Document fieldsObject = decode(this.fieldSpec, prepareBindingContext(this.fieldSpec, accessor));

		Query query = new BasicQuery(queryObject, fieldsObject).with(accessor.getSort());

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
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isExistsQuery()
	 */
	@Override
	protected boolean isExistsQuery() {
		return isExistsQuery;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isDeleteQuery()
	 */
	@Override
	protected boolean isDeleteQuery() {
		return this.isDeleteQuery;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isLimiting()
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
