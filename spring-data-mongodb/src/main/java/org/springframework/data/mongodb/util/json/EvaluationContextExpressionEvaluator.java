/*
 * Copyright 2022-2023 the original author or authors.
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
import java.util.function.Supplier;

import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @since 3.3.5
 */
class EvaluationContextExpressionEvaluator implements SpELExpressionEvaluator {

	ValueProvider valueProvider;
	ExpressionParser expressionParser;
	Supplier<EvaluationContext> evaluationContext;

	public EvaluationContextExpressionEvaluator(ValueProvider valueProvider, ExpressionParser expressionParser,
			Supplier<EvaluationContext> evaluationContext) {

		this.valueProvider = valueProvider;
		this.expressionParser = expressionParser;
		this.evaluationContext = evaluationContext;
	}

	@Nullable
	@Override
	public <T> T evaluate(String expression) {
		return evaluateExpression(expression, Collections.emptyMap());
	}

	public EvaluationContext getEvaluationContext(String expressionString) {
		return evaluationContext != null ? evaluationContext.get() : new StandardEvaluationContext();
	}

	public SpelExpression getParsedExpression(String expressionString) {
		return (SpelExpression) (expressionParser != null ? expressionParser : new SpelExpressionParser())
				.parseExpression(expressionString);
	}

	public <T> T evaluateExpression(String expressionString, Map<String, Object> variables) {

		SpelExpression expression = getParsedExpression(expressionString);
		EvaluationContext ctx = getEvaluationContext(expressionString);
		variables.forEach((key, value) -> ctx.setVariable(key, value));

		Object result = expression.getValue(ctx, Object.class);
		return (T) result;
	}
}
