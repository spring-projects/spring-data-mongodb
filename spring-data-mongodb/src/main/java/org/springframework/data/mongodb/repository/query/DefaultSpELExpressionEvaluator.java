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
package org.springframework.data.mongodb.repository.query;

import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;

/**
 * Simple {@link SpELExpressionEvaluator} implementation using {@link ExpressionParser} and {@link EvaluationContext}.
 *
 * @author Mark Paluch
 * @since 3.1
 */
class DefaultSpELExpressionEvaluator implements SpELExpressionEvaluator {

	private final ExpressionParser parser;
	private final EvaluationContext context;

	DefaultSpELExpressionEvaluator(ExpressionParser parser, EvaluationContext context) {
		this.parser = parser;
		this.context = context;
	}

	/**
	 * Return a {@link SpELExpressionEvaluator} that does not support expression evaluation.
	 *
	 * @return a {@link SpELExpressionEvaluator} that does not support expression evaluation.
	 */
	public static SpELExpressionEvaluator unsupported() {
		return NoOpExpressionEvaluator.INSTANCE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.SpELExpressionEvaluator#evaluate(java.lang.String)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> T evaluate(String expression) {
		return (T) parser.parseExpression(expression).getValue(context, Object.class);
	}

	/**
	 * {@link SpELExpressionEvaluator} that does not support SpEL evaluation.
	 *
	 * @author Mark Paluch
	 * @since 3.1
	 */
	enum NoOpExpressionEvaluator implements SpELExpressionEvaluator {

		INSTANCE;

		@Override
		public <T> T evaluate(String expression) {
			throw new UnsupportedOperationException("Expression evaluation not supported");
		}
	}
}
