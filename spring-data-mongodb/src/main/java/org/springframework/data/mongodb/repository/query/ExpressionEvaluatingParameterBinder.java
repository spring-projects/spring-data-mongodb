/*
 * Copyright 2015-2017 the original author or authors.
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

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.UtilityClass;

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

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * {@link ExpressionEvaluatingParameterBinder} allows to evaluate, convert and bind parameters to placeholders within a
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

		int parameterIndex = 0;
		while (matcher.find()) {

			Placeholder placeholder = extractPlaceholder(parameterIndex++, matcher);
			ParameterBinding binding = bindingContext.getBindingFor(placeholder);
			String valueForBinding = getParameterValueForBinding(accessor, bindingContext.getParameters(), binding);

			// appendReplacement does not like unescaped $ sign and others, so we need to quote that stuff first
			matcher.appendReplacement(buffer, Matcher.quoteReplacement(valueForBinding));
			if (StringUtils.hasText(placeholder.getSuffix())) {
				buffer.append(placeholder.getSuffix());
			}

			if (binding.isQuoted() || placeholder.isQuoted()) {
				postProcessQuotedBinding(buffer, valueForBinding,
						!binding.isExpression() ? accessor.getBindableValue(binding.getParameterIndex()) : null,
						binding.isExpression());
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
	 * @param raw the raw binding value
	 * @param isExpression {@literal true} if the binding value results from a SpEL expression.
	 */
	private void postProcessQuotedBinding(StringBuffer buffer, String valueForBinding, Object raw, boolean isExpression) {

		int quotationMarkIndex = buffer.length() - valueForBinding.length() - 1;
		char quotationMark = buffer.charAt(quotationMarkIndex);

		while (quotationMark != '\'' && quotationMark != '"') {

			quotationMarkIndex--;

			if (quotationMarkIndex < 0) {
				throw new IllegalArgumentException("Could not find opening quotes for quoted parameter");
			}

			quotationMark = buffer.charAt(quotationMarkIndex);
		}

		// remove quotation char before the complex object string
		if (valueForBinding.startsWith("{") && (raw instanceof DBObject || isExpression)) {

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

			if (binding.isExpression() && ((String) value).startsWith("{")) {
				return (String) value;
			}

			return QuotedString.unquote(JSON.serialize(value));
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
			regex.append("(" + Pattern.quote(binding.getParameter()) + ")");
			regex.append("(\\W?['\"])?"); // potential quotation char (as in { foo : '?0' }).
		}

		return Pattern.compile(regex.substring(1));
	}

	/**
	 * Extract the placeholder stripping any trailing trailing quotation mark that might have resulted from the
	 * {@link #createReplacementPattern(List) pattern} used.
	 *
	 * @param parameterIndex The actual parameter index.
	 * @param matcher The actual {@link Matcher}.
	 * @return
	 */
	private Placeholder extractPlaceholder(int parameterIndex, Matcher matcher) {

		if (matcher.groupCount() > 1) {

			String rawPlaceholder = matcher.group(parameterIndex * 2 + 1);
			String suffix = matcher.group(parameterIndex * 2 + 2);

			if (!StringUtils.hasText(rawPlaceholder)) {

				rawPlaceholder = matcher.group();
				suffix = "" + rawPlaceholder.charAt(rawPlaceholder.length() - 1);
				if (QuotedString.endsWithQuote(rawPlaceholder)) {
					rawPlaceholder = QuotedString.unquoteSuffix(rawPlaceholder);
				}
			}

			if (StringUtils.hasText(suffix)) {

				boolean quoted = QuotedString.endsWithQuote(suffix);

				return Placeholder.of(parameterIndex, rawPlaceholder, quoted,
						quoted ? QuotedString.unquoteSuffix(suffix) : suffix);
			}
		}

		return Placeholder.of(parameterIndex, matcher.group(), false, null);
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

			int parameterIndex = 0;
			for (ParameterBinding binding : bindings) {
				map.put(Placeholder.of(parameterIndex++, binding.getParameter(), binding.isQuoted(), null), binding);
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
	@EqualsAndHashCode(exclude = { "quoted", "suffix" })
	static class Placeholder {

		private int parameterIndex;
		private final String parameter;
		private final boolean quoted;
		private final String suffix;

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return quoted ? String.format("'%s'", parameter + (suffix != null ? suffix : ""))
					: parameter + (suffix != null ? suffix : "");
		}

	}

	/**
	 * Utility to handle quoted strings using single/double quotes.
	 *
	 * @author Mark Paluch
	 */
	@UtilityClass
	static class QuotedString {

		/**
		 * @param string
		 * @return {@literal true} if {@literal string} ends with a single/double quote.
		 */
		static boolean endsWithQuote(String string) {
			return string.endsWith("'") || string.endsWith("\"");
		}

		/**
		 * Remove trailing quoting from {@literal quoted}.
		 *
		 * @param quoted
		 * @return {@literal quoted} with removed quotes.
		 */
		public static String unquoteSuffix(String quoted) {
			return quoted.substring(0, quoted.length() - 1);
		}

		/**
		 * Remove leading and trailing quoting from {@literal quoted}.
		 *
		 * @param quoted
		 * @return {@literal quoted} with removed quotes.
		 */
		public static String unquote(String quoted) {
			return quoted.substring(1, quoted.length() - 1);
		}
	}
}
