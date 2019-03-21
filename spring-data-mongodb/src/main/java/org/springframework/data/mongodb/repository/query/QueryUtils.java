/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.aopalliance.intercept.MethodInterceptor;
import org.bson.Document;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

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

	private static final ParameterBindingDocumentCodec CODEC = new ParameterBindingDocumentCodec();

	private static final Pattern PARAMETER_BINDING_PATTERN = Pattern.compile("\\?(\\d+)");

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
			MongoParameters parameters, SpelExpressionParser expressionParser,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {

		if (accessor.getCollation() != null) {
			return query.collation(accessor.getCollation());
		}

		if (collationExpression == null) {
			return query;
		}

		if (StringUtils.trimLeadingWhitespace(collationExpression).startsWith("{")) {

			ParameterBindingContext bindingContext = new ParameterBindingContext((accessor::getBindableValue),
					expressionParser, evaluationContextProvider.getEvaluationContext(parameters, accessor.getValues()));

			return query.collation(Collation.from(CODEC.decode(collationExpression, bindingContext)));
		}

		Matcher matcher = PARAMETER_BINDING_PATTERN.matcher(collationExpression);
		if (!matcher.find()) {
			return query.collation(Collation.parse(collationExpression));
		}

		String placeholder = matcher.group();
		Object placeholderValue = accessor.getBindableValue(computeParameterIndex(placeholder));

		if (collationExpression.startsWith("?")) {

			if (placeholderValue instanceof String) {
				return query.collation(Collation.parse(placeholderValue.toString()));
			}
			if (placeholderValue instanceof Locale) {
				return query.collation(Collation.of((Locale) placeholderValue));
			}
			if (placeholderValue instanceof Document) {
				return query.collation(Collation.from((Document) placeholderValue));
			}
			throw new IllegalArgumentException(String.format("Collation must be a String, Locale or Document but was %s",
					ObjectUtils.nullSafeClassName(placeholderValue)));
		}

		return query.collation(Collation.parse(collationExpression.replace(placeholder, placeholderValue.toString())));
	}

	private static int computeParameterIndex(String parameter) {
		return NumberUtils.parseNumber(parameter.replace("?", ""), Integer.class);
	}
}
