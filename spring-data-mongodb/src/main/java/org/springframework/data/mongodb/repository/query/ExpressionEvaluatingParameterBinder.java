/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.query;

import java.util.Collections;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.bson.BSON;
import org.springframework.data.mongodb.repository.query.StringBasedMongoQuery.ParameterBinding;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.mongodb.util.JSON;

/**
 * {@link ExpressionEvaluatingParameterBinder} allows to evaluate, convert and bind parameters to placholders within a
 * {@link String}.
 * 
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @since 1.9
 */
class ExpressionEvaluatingParameterBinder {

	private final SpelExpressionParser expressionParser;
	private final EvaluationContextProvider evaluationContextProvider;

	/**
	 * Creates new {@link ExpressionEvaluatingParameterBinder}
	 * 
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public ExpressionEvaluatingParameterBinder(SpelExpressionParser expressionParser,
			EvaluationContextProvider evaluationContextProvider) {

		Assert.notNull(expressionParser, "ExpressionParser must not be null!");
		Assert.notNull(evaluationContextProvider, "EvaluationContextProvider must not be null!");

		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;
	}

	/**
	 * Bind values provided by {@link MongoParameterAccessor} to placeholders in {@literal raw} while consisdering
	 * potential conversions and parameter types.
	 * 
	 * @param raw
	 * @param accessor
	 * @param bindingContext
	 * @return {@literal null} if given {@literal raw} value is empty.
	 */
	public String bind(String raw, MongoParameterAccessor accessor, BindingContext bindingContext) {

		if (!StringUtils.hasText(raw)) {
			return null;
		}

		return replacePlaceholders(raw, accessor, bindingContext);
	}

	/**
	 * Replaced the parameter place-holders with the actual parameter values from the given {@link ParameterBinding}s.
	 * 
	 * @param input
	 * @param accessor
	 * @param parameters
	 * @param bindings
	 * @return
	 */
	private String replacePlaceholders(String input, MongoParameterAccessor accessor, BindingContext bindingContext) {

		if (!bindingContext.hasBindings()) {
			return input;
		}

		boolean isCompletlyParameterizedQuery = input.matches("^\\?\\d+$");

		StringBuilder result = new StringBuilder(input);

		for (ParameterBinding binding : bindingContext.getBindings()) {

			String parameter = binding.getParameter();
			int idx = result.indexOf(parameter);

			if (idx != -1) {
				String valueForBinding = getParameterValueForBinding(accessor, bindingContext.getParameters(), binding);

				// if the value to bind is an object literal we need to remove the quoting around
				// the expression insertion point.
				boolean shouldPotentiallyRemoveQuotes = valueForBinding.startsWith("{") && !isCompletlyParameterizedQuery;

				int start = idx;
				int end = idx + parameter.length();

				if (shouldPotentiallyRemoveQuotes) {

					// is the insertion point actually surrounded by quotes?
					char beforeStart = result.charAt(start - 1);
					char afterEnd = result.charAt(end);

					if ((beforeStart == '\'' || beforeStart == '"') && (afterEnd == '\'' || afterEnd == '"')) {

						// skip preceeding and following quote
						start -= 1;
						end += 1;
					}
				}

				result.replace(start, end, valueForBinding);
			}
		}

		return result.toString();
	}

	/**
	 * Returns the serialized value to be used for the given {@link ParameterBinding}.
	 * 
	 * @param accessor
	 * @param parameters
	 * @param binding
	 * @return
	 */
	private String getParameterValueForBinding(MongoParameterAccessor accessor, MongoParameters parameters,
			ParameterBinding binding) {

		Object value = binding.isExpression() ? evaluateExpression(binding.getExpression(), parameters,
				accessor.getValues()) : accessor.getBindableValue(binding.getParameterIndex());

		if (value instanceof String && binding.isQuoted()) {
			return (String) value;
		}

		if (value instanceof byte[]) {

			String base64representation = DatatypeConverter.printBase64Binary((byte[]) value);
			if (!binding.isQuoted()) {
				return "{ '$binary' : '" + base64representation + "', '$type' : " + BSON.B_GENERAL + "}";
			}
			return base64representation;
		}

		return JSON.serialize(value);
	}

	/**
	 * Evaluates the given {@code expressionString}.
	 * 
	 * @param expressionString
	 * @param parameters
	 * @param parameterValues
	 * @return
	 */
	private Object evaluateExpression(String expressionString, MongoParameters parameters, Object[] parameterValues) {

		EvaluationContext evaluationContext = evaluationContextProvider.getEvaluationContext(parameters, parameterValues);
		Expression expression = expressionParser.parseExpression(expressionString);

		return expression.getValue(evaluationContext, Object.class);
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.9
	 */
	static class BindingContext {

		final MongoParameters parameters;
		final List<ParameterBinding> bindings;

		/**
		 * Creates new {@link BindingContext}.
		 * 
		 * @param parameters
		 * @param bindings
		 */
		public BindingContext(MongoParameters parameters, List<ParameterBinding> bindings) {

			this.parameters = parameters;
			this.bindings = bindings;
		}

		/**
		 * @return {@literal true} when list of bindings is not empty.
		 */
		boolean hasBindings() {
			return !CollectionUtils.isEmpty(bindings);
		}

		/**
		 * Get unmodifiable list of {@link ParameterBinding}s.
		 * 
		 * @return never {@literal null}.
		 */
		public List<ParameterBinding> getBindings() {
			return Collections.unmodifiableList(bindings);
		}

		/**
		 * Get the associated {@link MongoParameters}.
		 * 
		 * @return
		 */
		public MongoParameters getParameters() {
			return parameters;
		}

	}
}
