/*
 * Copyright 2019-2025 the original author or authors.
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

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.data.util.Lazy;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParseException;
import org.springframework.expression.ParserContext;

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
	private final ValueExpressionEvaluator expressionEvaluator;

	/**
	 * @param valueProvider
	 * @param expressionParser
	 * @param evaluationContext a {@link Supplier} for {@link Lazy} context retrieval.
	 * @since 2.2.3
	 */
	public ParameterBindingContext(ValueProvider valueProvider, ExpressionParser expressionParser,
			Supplier<EvaluationContext> evaluationContext) {
		this(valueProvider, new EvaluationContextExpressionEvaluator(valueProvider, unwrap(expressionParser)) {
			@Override
			public EvaluationContext getEvaluationContext(String expressionString) {
				return evaluationContext.get();
			}
		});
	}

	private static ExpressionParser unwrap(ExpressionParser expressionParser) {
		return new ExpressionParser() {
			@Override
			public Expression parseExpression(String expressionString) throws ParseException {
				return expressionParser.parseExpression(unwrap(expressionString));
			}

			@Override
			public Expression parseExpression(String expressionString, ParserContext context) throws ParseException {
				return expressionParser.parseExpression(unwrap(expressionString), context);
			}
		};
	}

	private static String unwrap(String expressionString) {
		return expressionString.startsWith("#{") && expressionString.endsWith("}")
				? expressionString.substring(2, expressionString.length() - 1).trim()
				: expressionString;
	}

	/**
	 * @param valueProvider
	 * @param expressionEvaluator
	 * @since 4.4.0
	 */
	public ParameterBindingContext(ValueProvider valueProvider, ValueExpressionEvaluator expressionEvaluator) {
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
	public static ParameterBindingContext forExpressions(ValueProvider valueProvider, ExpressionParser expressionParser,
			Function<ExpressionDependencies, EvaluationContext> contextFunction) {

		return new ParameterBindingContext(valueProvider,
				new EvaluationContextExpressionEvaluator(valueProvider, expressionParser) {

					@Override
					public EvaluationContext getEvaluationContext(String expressionString) {

						Expression expression = getParsedExpression(expressionString);
						ExpressionDependencies dependencies = ExpressionDependencies.discover(expression);
						return contextFunction.apply(dependencies);
					}
				});
	}

	/**
	 * Create a new {@link ParameterBindingContext} that is capable of expression parsing.
	 *
	 * @param valueProvider
	 * @param expressionEvaluator
	 * @return
	 * @since 4.4.0
	 */
	public static ParameterBindingContext forExpressions(ValueProvider valueProvider,
			ValueExpressionEvaluator expressionEvaluator) {

		return new ParameterBindingContext(valueProvider, expressionEvaluator);
	}

	public @Nullable Object bindableValueForIndex(int index) {
		return valueProvider.getBindableValue(index);
	}

	public @Nullable Object evaluateExpression(String expressionString) {
		return expressionEvaluator.evaluate(expressionString);
	}

	public @Nullable Object evaluateExpression(String expressionString, Map<String, Object> variables) {

		if (expressionEvaluator instanceof EvaluationContextExpressionEvaluator expressionEvaluator) {
			return expressionEvaluator.evaluateExpression(expressionString, variables);
		}
		return expressionEvaluator.evaluate(expressionString);
	}

	public ValueProvider getValueProvider() {
		return valueProvider;
	}
}
