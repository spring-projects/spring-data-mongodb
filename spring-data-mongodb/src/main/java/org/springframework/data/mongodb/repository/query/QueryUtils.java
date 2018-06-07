/*
 * Copyright 2018 the original author or authors.
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

import org.aopalliance.intercept.MethodInterceptor;
import org.bson.Document;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Internal utility class to help avoid duplicate code required in both the reactive and the sync {@link Query} support
 * offered by repositories.
 *
 * @author Christoph Strobl
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

		return (Query) factory.getProxy();
	}
}
