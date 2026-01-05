/*
 * Copyright 2024-present the original author or authors.
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
package org.springframework.data.mongodb.util;

import java.time.Duration;
import java.util.function.Supplier;

import org.springframework.core.env.Environment;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.format.datetime.standard.DurationFormatterUtils;
import org.springframework.lang.Nullable;

/**
 * Helper to evaluate Duration from expressions.
 *
 * @author Christoph Strobl
 * @since 4.4
 */
public class DurationUtil {

	private static final ValueExpressionParser PARSER = ValueExpressionParser.create(SpelExpressionParser::new);

	/**
	 * Evaluates and potentially parses the given string representation into a {@link Duration} value.
	 *
	 * @param value the {@link String} representation of the duration to evaluate.
	 * @param evaluationContext context supplier for property and expression language evaluation.
	 * @return the evaluated duration.
	 */
	public static Duration evaluate(String value, ValueEvaluationContext evaluationContext) {

		ValueExpression expression = PARSER.parse(value);
		Object evaluatedTimeout = expression.evaluate(evaluationContext);

		if (evaluatedTimeout == null) {
			return Duration.ZERO;
		}

		if (evaluatedTimeout instanceof Duration duration) {
			return duration;
		}

		return parse(evaluatedTimeout.toString());
	}

	/**
	 * Evaluates and potentially parses the given string representation into a {@link Duration} value.
	 *
	 * @param value the {@link String} representation of the duration to evaluate.
	 * @param evaluationContext context supplier for expression language evaluation.
	 * @return the evaluated duration.
	 */
	public static Duration evaluate(String value, Supplier<EvaluationContext> evaluationContext) {

		return evaluate(value, new ValueEvaluationContext() {
			@Nullable
			@Override
			public Environment getEnvironment() {
				return null;
			}

			@Nullable
			@Override
			public EvaluationContext getEvaluationContext() {
				return evaluationContext.get();
			}
		});
	}

	/**
	 *
	 * @param duration duration string to parse.
	 * @return parsed {@link Duration}.
	 * @see DurationFormatterUtils
	 */
	public static Duration parse(String duration) {
		return DurationFormatterUtils.detectAndParse(duration);
	}
}
