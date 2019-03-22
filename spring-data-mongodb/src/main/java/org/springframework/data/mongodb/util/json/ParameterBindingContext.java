/*
 * Copyright 2019 the original author or authors.
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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;

/**
 * Reusable context for binding parameters to an placeholder or a SpEL expression within a JSON structure. <br />
 * To be used along with {@link ParameterBindingDocumentCodec#decode(String, ParameterBindingContext)}.
 *
 * @author Christoph Strobl
 * @since 2.2
 */
@RequiredArgsConstructor
@Getter
public class ParameterBindingContext {

	private final ValueProvider valueProvider;
	private final SpelExpressionParser expressionParser;
	private final EvaluationContext evaluationContext;

	@Nullable
	public Object bindableValueForIndex(int index) {
		return valueProvider.getBindableValue(index);
	}

	@Nullable
	public Object evaluateExpression(String expressionString) {

		Expression expression = expressionParser.parseExpression(expressionString);
		return expression.getValue(this.evaluationContext, Object.class);
	}
}
