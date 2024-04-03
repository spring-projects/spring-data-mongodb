/*
 * Copyright 2024 the original author or authors.
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

import java.util.function.Function;

import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.repository.query.ValueExpressionDelegate;

class ValueExpressionDelegateValueExpressionEvaluator implements ValueExpressionEvaluator {

	private final ValueExpressionDelegate delegate;
	private final Function<ValueExpression, ValueEvaluationContext> expressionToContext;

	ValueExpressionDelegateValueExpressionEvaluator(ValueExpressionDelegate delegate, Function<ValueExpression, ValueEvaluationContext> expressionToContext) {
		this.delegate = delegate;
		this.expressionToContext = expressionToContext;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T evaluate(String expressionString) {
		ValueExpression expression = delegate.parse(expressionString);
		return (T) expression.evaluate(expressionToContext.apply(expression));
	}
}
