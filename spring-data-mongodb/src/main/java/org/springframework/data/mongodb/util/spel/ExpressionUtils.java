/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.util.spel;

import java.util.function.Supplier;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * Internal utility class for dealing with {@link Expression} and potential ones.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
public final class ExpressionUtils {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	/**
	 * Returns a SpEL {@link Expression} if the given {@link String} is actually an expression that does not evaluate to a
	 * {@link LiteralExpression} (indicating that no subsequent evaluation is necessary).
	 *
	 * @param potentialExpression can be {@literal null}
	 * @return can be {@literal null}.
	 */
	@Nullable
	public static Expression detectExpression(@Nullable String potentialExpression) {

		if (!StringUtils.hasText(potentialExpression)) {
			return null;
		}

		Expression expression = PARSER.parseExpression(potentialExpression, ParserContext.TEMPLATE_EXPRESSION);
		return expression instanceof LiteralExpression ? null : expression;
	}

	@Nullable
	public static Object evaluate(String value, Supplier<EvaluationContext> evaluationContext) {

		Expression expression = detectExpression(value);
		if (expression == null) {
			return value;
		}

		return expression.getValue(evaluationContext.get(), Object.class);
	}
}
