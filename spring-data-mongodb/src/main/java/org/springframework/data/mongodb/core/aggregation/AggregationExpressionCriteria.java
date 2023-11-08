/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.EvaluationOperators.Expr;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;

/**
 * A {@link CriteriaDefinition criteria} to use {@code $expr} within a
 * {@link org.springframework.data.mongodb.core.query.Query}.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
public class AggregationExpressionCriteria implements CriteriaDefinition {

	private final AggregationExpression expression;

	AggregationExpressionCriteria(AggregationExpression expression) {
		this.expression = expression;
	}

	/**
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link AggregationExpressionCriteria}.
	 */
	public static AggregationExpressionCriteria whereExpr(AggregationExpression expression) {
		return new AggregationExpressionCriteria(expression);
	}

	@Override
	public Document getCriteriaObject() {

		if (expression instanceof Expr expr) {
			return new Document(getKey(), expr.get(0));
		}
		return new Document(getKey(), expression);
	}

	@Override
	public String getKey() {
		return "$expr";
	}
}
