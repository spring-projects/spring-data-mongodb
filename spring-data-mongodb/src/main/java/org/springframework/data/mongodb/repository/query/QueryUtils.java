/*
 * Copyright 2018-2025 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.util.ClassUtils;

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

	protected static final Log LOGGER = LogFactory.getLog(QueryUtils.class);

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

		BasicQuery defaultSortQuery = query instanceof BasicQuery bq ? bq : new BasicQuery(query);

		Document combinedSort = new Document(defaultSort);
		combinedSort.putAll(defaultSortQuery.getSortObject());
		defaultSortQuery.setSortObject(combinedSort);

		return defaultSortQuery;
	}

	/**
	 * Apply a collation extracted from the given {@literal collationExpression} to the given {@link Query}. Potentially
	 * replace parameter placeholders with values from the {@link ConvertingParameterAccessor accessor}.
	 *
	 * @param query must not be {@literal null}.
	 * @param collationExpression must not be {@literal null}.
	 * @param accessor must not be {@literal null}.
	 * @param expressionEvaluator must not be {@literal null}.
	 * @return the {@link Query} having proper {@link Collation}.
	 * @see Query#collation(Collation)
	 * @since 2.2
	 */
	static Query applyCollation(Query query, @Nullable String collationExpression, ConvertingParameterAccessor accessor,
			ValueExpressionEvaluator expressionEvaluator) {

		Collation collation = CollationUtils.computeCollation(collationExpression, accessor, expressionEvaluator);
		return collation == null ? query : query.collation(collation);
	}

	/**
	 * Get the first index of the parameter that can be assigned to the given type.
	 *
	 * @param type the type to look for.
	 * @param parameters the actual parameters.
	 * @return -1 if not found.
	 * @since 3.4
	 */
	static int indexOfAssignableParameter(Class<?> type, Class<?>[] parameters) {
		return indexOfAssignableParameter(type, Arrays.asList(parameters));
	}

	/**
	 * Get the first index of the parameter that can be assigned to the given type.
	 *
	 * @param type the type to look for.
	 * @param parameters the actual parameters.
	 * @return -1 if not found.
	 * @since 3.4
	 */
	static int indexOfAssignableParameter(Class<?> type, List<Class<?>> parameters) {

		if (parameters.isEmpty()) {
			return -1;
		}

		int i = 0;
		for (Class<?> parameterType : parameters) {
			if (ClassUtils.isAssignable(type, parameterType)) {
				return i;
			}
			i++;
		}
		return -1;
	}

}
