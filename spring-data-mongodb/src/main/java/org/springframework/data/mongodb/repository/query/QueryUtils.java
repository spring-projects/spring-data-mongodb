/*
 * Copyright 2018-2020 the original author or authors.
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

import org.aopalliance.intercept.MethodInterceptor;
import org.bson.Document;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;

/**
 * Internal utility class to help avoid duplicate code required in both the reactive and the sync {@link Query} support
 * offered by repositories.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 * @currentRead Assassin's Apprentice - Robin Hobb
 */
class QueryUtils {

	/**
	 * Decorate {@link Query} and add a default sort expression to the given {@link Query}. Attributes of the given
	 * {@code sort} may be overwritten by the sort explicitly defined by the {@link Query} itself.
	 *
	 * @param query the {@link Query} to decorate.
	 * @param defaultSort the default sort expression to apply to the query.
	 * @return the query having the given {@code sort} applied.
	 */
	static Query decorateSort(Query query, Document defaultSort) {

		if (defaultSort.isEmpty()) {
			return query;
		}

		ProxyFactory factory = new ProxyFactory(query);
		factory.addAdvice((MethodInterceptor) invocation -> {

			if (!invocation.getMethod().getName().equals("getSortObject")) {
				return invocation.proceed();
			}

			Document combinedSort = new Document(defaultSort);
			combinedSort.putAll((Document) invocation.proceed());
			return combinedSort;
		});

		return (Query) factory.getProxy(query.getClass().getClassLoader());
	}

	/**
	 * Apply a collation extracted from the given {@literal collationExpression} to the given {@link Query}. Potentially
	 * replace parameter placeholders with values from the {@link ConvertingParameterAccessor accessor}.
	 *
	 * @param query must not be {@literal null}.
	 * @param collationExpression must not be {@literal null}.
	 * @param accessor must not be {@literal null}.
	 * @return the {@link Query} having proper {@link Collation}.
	 * @see Query#collation(Collation)
	 * @since 2.2
	 */
	static Query applyCollation(Query query, @Nullable String collationExpression, ConvertingParameterAccessor accessor,
			MongoParameters parameters, ExpressionParser expressionParser,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {

		Collation collation = CollationUtils.computeCollation(collationExpression, accessor, parameters, expressionParser,
				evaluationContextProvider);
		return collation == null ? query : query.collation(collation);
	}
}
