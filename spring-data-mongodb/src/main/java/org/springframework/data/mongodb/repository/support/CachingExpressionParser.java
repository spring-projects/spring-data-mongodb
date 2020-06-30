/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParseException;
import org.springframework.expression.ParserContext;

/**
 * Caching variant of {@link ExpressionParser}. This implementation does not support
 * {@link #parseExpression(String, ParserContext) parsing with ParseContext}.
 *
 * @author Mark Paluch
 * @since 3.1
 */
class CachingExpressionParser implements ExpressionParser {

	private final ExpressionParser delegate;
	private final Map<String, Expression> cache = new ConcurrentHashMap<>();

	public CachingExpressionParser(ExpressionParser delegate) {
		this.delegate = delegate;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.expression.ExpressionParser#parseExpression(java.lang.String)
	 */
	@Override
	public Expression parseExpression(String expressionString) throws ParseException {
		return cache.computeIfAbsent(expressionString, delegate::parseExpression);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.expression.ExpressionParser#parseExpression(java.lang.String, org.springframework.expression.ParserContext)
	 */
	@Override
	public Expression parseExpression(String expressionString, ParserContext context) throws ParseException {
		throw new UnsupportedOperationException("Parsing using ParserContext is not supported");
	}
}
