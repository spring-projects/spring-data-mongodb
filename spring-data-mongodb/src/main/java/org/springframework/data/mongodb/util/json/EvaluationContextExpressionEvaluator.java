/*
 * Copyright 2022-2025 the original author or authors.
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
package org.springframework.data.mongodb.util.json;

import java.util.Collections;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author Christoph Strobl
 * @since 3.3.5
 */
class EvaluationContextExpressionEvaluator implements ValueExpressionEvaluator {

	final ValueProvider valueProvider;
	final ExpressionParser expressionParser;

	EvaluationContextExpressionEvaluator(ValueProvider valueProvider, ExpressionParser expressionParser) {

		this.valueProvider = valueProvider;
		this.expressionParser = expressionParser;
	}

	@Override
	public <T> @Nullable T evaluate(String expression) {
		return evaluateExpression(expression, Collections.emptyMap());
	}

	EvaluationContext getEvaluationContext(String expressionString) {
		return new StandardEvaluationContext();
	}

	Expression getParsedExpression(String expressionString) {
		return expressionParser.parseExpression(expressionString);
	}

	@SuppressWarnings("unchecked")
	<T> @Nullable T evaluateExpression(String expressionString, Map<String, Object> variables) {

		Expression expression = getParsedExpression(expressionString);
		EvaluationContext ctx = getEvaluationContext(expressionString);
		variables.forEach(ctx::setVariable);

		Object result = expression.getValue(ctx, Object.class);
		return (T) result;
	}
}
