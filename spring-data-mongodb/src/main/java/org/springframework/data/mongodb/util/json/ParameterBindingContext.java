/*
 * Copyright 2019-2020 the original author or authors.
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

import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.data.util.Lazy;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;

/**
 * Reusable context for binding parameters to a placeholder or a SpEL expression within a JSON structure. <br />
 * To be used along with {@link ParameterBindingDocumentCodec#decode(String, ParameterBindingContext)}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.2
 */
public class ParameterBindingContext {

	private final ValueProvider valueProvider;
	private final SpELExpressionEvaluator expressionEvaluator;

	/**
	 * @param valueProvider
	 * @param expressionParser
	 * @param evaluationContext
	 */
	public ParameterBindingContext(ValueProvider valueProvider, SpelExpressionParser expressionParser,
			EvaluationContext evaluationContext) {
		this(valueProvider, expressionParser, () -> evaluationContext);
	}

	/**
	 * @param valueProvider
	 * @param expressionParser
	 * @param evaluationContext a {@link Supplier} for {@link Lazy} context retrieval.
	 * @since 2.2.3
	 */
	public ParameterBindingContext(ValueProvider valueProvider, SpelExpressionParser expressionParser,
			Supplier<EvaluationContext> evaluationContext) {

		this(valueProvider, new SpELExpressionEvaluator() {
			@Override
			public <T> T evaluate(String expressionString) {
				return (T) expressionParser.parseExpression(expressionString).getValue(evaluationContext.get(), Object.class);
			}
		});
	}

	/**
	 * @param valueProvider
	 * @param expressionEvaluator
	 * @since 3.1
	 */
	public ParameterBindingContext(ValueProvider valueProvider, SpELExpressionEvaluator expressionEvaluator) {
		this.valueProvider = valueProvider;
		this.expressionEvaluator = expressionEvaluator;
	}

	/**
	 * Create a new {@link ParameterBindingContext} that is capable of expression parsing and can provide a
	 * {@link EvaluationContext} based on {@link ExpressionDependencies}.
	 *
	 * @param valueProvider
	 * @param expressionParser
	 * @param contextFunction
	 * @return
	 * @since 3.1
	 */
	public static ParameterBindingContext forExpressions(ValueProvider valueProvider,
			SpelExpressionParser expressionParser, Function<ExpressionDependencies, EvaluationContext> contextFunction) {

		return new ParameterBindingContext(valueProvider, new SpELExpressionEvaluator() {
			@Override
			public <T> T evaluate(String expressionString) {

				Expression expression = expressionParser.parseExpression(expressionString);
				ExpressionDependencies dependencies = ExpressionDependencies.discover(expression);
				EvaluationContext evaluationContext = contextFunction.apply(dependencies);

				return (T) expression.getValue(evaluationContext, Object.class);
			}
		});
	}

	@Nullable
	public Object bindableValueForIndex(int index) {
		return valueProvider.getBindableValue(index);
	}

	@Nullable
	public Object evaluateExpression(String expressionString) {
		return expressionEvaluator.evaluate(expressionString);
	}

	public ValueProvider getValueProvider() {
		return valueProvider;
	}
}
