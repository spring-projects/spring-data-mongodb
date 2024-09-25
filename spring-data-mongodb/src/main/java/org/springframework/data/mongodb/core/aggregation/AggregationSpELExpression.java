/*
 * Copyright 2016-2024 the original author or authors.
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
import org.springframework.util.Assert;

/**
 * An {@link AggregationExpression} that renders a MongoDB Aggregation Framework expression from the AST of a
 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/core.html#expressions">SpEL
 * expression</a>. <br />
 * <br />
 * <strong>Samples:</strong> <br />
 * <pre>
 * <code>
 * // { $and: [ { $gt: [ "$qty", 100 ] }, { $lt: [ "$qty", 250 ] } ] }
 * expressionOf("qty > 100 && qty < 250);
 *
 * // { $cond : { if : { $gte : [ "$a", 42 ]}, then : "answer", else : "no-answer" } }
 * expressionOf("cond(a >= 42, 'answer', 'no-answer')");
 * </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @see SpelExpressionTransformer
 * @since 1.10
 */
public class AggregationSpELExpression implements AggregationExpression {

	private static final SpelExpressionTransformer TRANSFORMER = new SpelExpressionTransformer();
	private final String rawExpression;
	private final Object[] parameters;

	private AggregationSpELExpression(String rawExpression, Object[] parameters) {

		this.rawExpression = rawExpression;
		this.parameters = parameters;
	}

	/**
	 * Creates new {@link AggregationSpELExpression} for the given {@literal expressionString} and {@literal parameters}.
	 *
	 * @param expressionString must not be {@literal null}.
	 * @param parameters can be empty.
	 * @return new instance of {@link AggregationSpELExpression}.
	 */
	public static AggregationSpELExpression expressionOf(String expressionString, Object... parameters) {

		Assert.notNull(expressionString, "ExpressionString must not be null");
		return new AggregationSpELExpression(expressionString, parameters);
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {
		return (Document) TRANSFORMER.transform(rawExpression, context, parameters);
	}
}
