/*
 * Copyright 2015-2016 the original author or authors.
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

import lombok.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * @author Oliver Gierke
 * @author Mark Paluch
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
	 * Bind values provided by {@link MongoParameterAccessor} to placeholders in {@literal raw} while considering
	 * potential conversions and parameter types.
	 * 
	 * @param raw can be {@literal null} or empty.
	 * @param accessor must not be {@literal null}.
	 * @param bindingContext must not be {@literal null}.
	 * @return {@literal null} if given {@code raw} value is empty.
	 */
	public String bind(String raw, MongoParameterAccessor accessor, BindingContext bindingContext) {

		if (!StringUtils.hasText(raw)) {
			return null;
		}

		return replacePlaceholders(raw, accessor, bindingContext);
	}

	/**
	 * Replaced the parameter placeholders with the actual parameter values from the given {@link ParameterBinding}s.
	 * 
	 * @param input must not be {@literal null} or empty.
	 * @param accessor must not be {@literal null}.
	 * @param bindingContext must not be {@literal null}.
	 * @return
	 */
	private String replacePlaceholders(String input, MongoParameterAccessor accessor, BindingContext bindingContext) {

		if (!bindingContext.hasBindings()) {
			return input;
		}

		if (input.matches("^\\?\\d+$")) {
			return getParameterValueForBinding(accessor, bindingContext.getParameters(),
					bindingContext.getBindings().iterator().next());
		}

		Matcher matcher = createReplacementPattern(bindingContext.getBindings()).matcher(input);
		StringBuffer buffer = new StringBuffer();

		while (matcher.find()) {

			ParameterBinding binding = bindingContext.getBindingFor(extractPlaceholder(matcher.group()));
			String valueForBinding = getParameterValueForBinding(accessor, bindingContext.getParameters(), binding);

			// appendReplacement does not like unescaped $ sign and others, so we need to quote that stuff first
			matcher.appendReplacement(buffer, Matcher.quoteReplacement(valueForBinding));

			if (binding.isQuoted()) {
				postProcessQuotedBinding(buffer, valueForBinding);
			}
		}

		matcher.appendTail(buffer);
		return buffer.toString();
	}

	/**
	 * Sanitize String binding by replacing single quoted values with double quotes which prevents potential single quotes
	 * contained in replacement to interfere with the Json parsing. Also take care of complex objects by removing the
	 * quotation entirely.
	 *
	 * @param buffer the {@link StringBuffer} to operate upon.
	 * @param valueForBinding the actual binding value.
	 */
	private void postProcessQuotedBinding(StringBuffer buffer, String valueForBinding) {

		int quotationMarkIndex = buffer.length() - valueForBinding.length() - 1;
		char quotationMark = buffer.charAt(quotationMarkIndex);

		while (quotationMark != '\'' && quotationMark != '"') {

			quotationMarkIndex--;

			if (quotationMarkIndex < 0) {
				throw new IllegalArgumentException("Could not find opening quotes for quoted parameter");
			}

			quotationMark = buffer.charAt(quotationMarkIndex);
		}

		if (valueForBinding.startsWith("{")) { // remove quotation char before the complex object string

			buffer.deleteCharAt(quotationMarkIndex);

		} else {

			if (quotationMark == '\'') {
				buffer.replace(quotationMarkIndex, quotationMarkIndex + 1, "\"");
			}

			buffer.append("\"");
		}
	}

	/**
	 * Returns the serialized value to be used for the given {@link ParameterBinding}.
	 *
	 * @param accessor must not be {@literal null}.
	 * @param parameters
	 * @param binding must not be {@literal null}.
	 * @return
	 */
	private String getParameterValueForBinding(MongoParameterAccessor accessor, MongoParameters parameters,
			ParameterBinding binding) {

		Object value = binding.isExpression()
				? evaluateExpression(binding.getExpression(), parameters, accessor.getValues())
				: accessor.getBindableValue(binding.getParameterIndex());

		if (value instanceof String && binding.isQuoted()) {
			return ((String) value).startsWith("{") ? (String) value : ((String) value).replace("\"", "\\\"");
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
	 * @param expressionString must not be {@literal null} or empty.
	 * @param parameters must not be {@literal null}.
	 * @param parameterValues must not be {@literal null}.
	 * @return
	 */
	private Object evaluateExpression(String expressionString, MongoParameters parameters, Object[] parameterValues) {

		EvaluationContext evaluationContext = evaluationContextProvider.getEvaluationContext(parameters, parameterValues);
		Expression expression = expressionParser.parseExpression(expressionString);

		return expression.getValue(evaluationContext, Object.class);
	}

	/**
	 * Creates a replacement {@link Pattern} for all {@link ParameterBinding#getParameter() binding parameters} including
	 * a potentially trailing quotation mark.
	 *
	 * @param bindings
	 * @return
	 */
	private Pattern createReplacementPattern(List<ParameterBinding> bindings) {

		StringBuilder regex = new StringBuilder();

		for (ParameterBinding binding : bindings) {

			regex.append("|");
			regex.append(Pattern.quote(binding.getParameter()));
			regex.append("['\"]?"); // potential quotation char (as in { foo : '?0' }).
		}

		return Pattern.compile(regex.substring(1));
	}

	/**
	 * Extract the placeholder stripping any trailing trailing quotation mark that might have resulted from the
	 * {@link #createReplacementPattern(List) pattern} used.
	 *
	 * @param groupName The actual {@link Matcher#group() group}.
	 * @return
	 */
	private Placeholder extractPlaceholder(String groupName) {

		return !groupName.endsWith("'") && !groupName.endsWith("\"") ? //
				Placeholder.of(groupName, false) : //
				Placeholder.of(groupName.substring(0, groupName.length() - 1), true);
	}

	/**
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 1.9
	 */
	static class BindingContext {

		final MongoParameters parameters;
		final Map<Placeholder, ParameterBinding> bindings;

		/**
		 * Creates new {@link BindingContext}.
		 *
		 * @param parameters
		 * @param bindings
		 */
		public BindingContext(MongoParameters parameters, List<ParameterBinding> bindings) {

			this.parameters = parameters;
			this.bindings = mapBindings(bindings);
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
			return new ArrayList<ParameterBinding>(bindings.values());
		}

		/**
		 * Get the concrete {@link ParameterBinding} for a given {@literal placeholder}.
		 *
		 * @param placeholder must not be {@literal null}.
		 * @return
		 * @throws java.util.NoSuchElementException
		 * @since 1.10
		 */
		ParameterBinding getBindingFor(Placeholder placeholder) {

			if (!bindings.containsKey(placeholder)) {
				throw new NoSuchElementException(String.format("Could not to find binding for placeholder '%s'.", placeholder));
			}

			return bindings.get(placeholder);
		}

		/**
		 * Get the associated {@link MongoParameters}.
		 *
		 * @return
		 */
		public MongoParameters getParameters() {
			return parameters;
		}

		private static Map<Placeholder, ParameterBinding> mapBindings(List<ParameterBinding> bindings) {

			Map<Placeholder, ParameterBinding> map = new LinkedHashMap<Placeholder, ParameterBinding>(bindings.size(), 1);

			for (ParameterBinding binding : bindings) {
				map.put(Placeholder.of(binding.getParameter(), binding.isQuoted()), binding);
			}

			return map;
		}
	}

	/**
	 * Encapsulates a quoted/unquoted parameter placeholder.
	 *
	 * @author Mark Paluch
	 * @since 1.9
	 */
	@Value(staticConstructor = "of")
	static class Placeholder {

		private final String parameter;
		private final boolean quoted;

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return quoted ? String.format("'%s'", parameter) : parameter;
		}
	}
}
