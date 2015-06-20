/*
 * Copyright 2011-2015 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.util.JSON;

/**
 * Query to use a plain JSON String to create the {@link Query} to actually execute.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
public class StringBasedMongoQuery extends AbstractMongoQuery {

	private static final String COUND_AND_DELETE = "Manually defined query for %s cannot be both a count and delete query at the same time!";
	private static final Logger LOG = LoggerFactory.getLogger(StringBasedMongoQuery.class);
	private static final ParameterBindingParser BINDING_PARSER = ParameterBindingParser.INSTANCE;

	private final String query;
	private final String fieldSpec;
	private final boolean isCountQuery;
	private final boolean isDeleteQuery;
	private final List<ParameterBinding> queryParameterBindings;
	private final List<ParameterBinding> fieldSpecParameterBindings;
	private final SpelExpressionParser expressionParser;
	private final EvaluationContextProvider evaluationContextProvider;

	/**
	 * Creates a new {@link StringBasedMongoQuery} for the given {@link MongoQueryMethod} and {@link MongoOperations}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public StringBasedMongoQuery(MongoQueryMethod method, MongoOperations mongoOperations,
			SpelExpressionParser expressionParser, EvaluationContextProvider evaluationContextProvider) {
		this(method.getAnnotatedQuery(), method, mongoOperations, expressionParser, evaluationContextProvider);
	}

	/**
	 * Creates a new {@link StringBasedMongoQuery} for the given {@link String}, {@link MongoQueryMethod},
	 * {@link MongoOperations}, {@link SpelExpressionParser} and {@link EvaluationContextProvider}.
	 *
	 * @param query must not be {@literal null}.
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 */
	public StringBasedMongoQuery(String query, MongoQueryMethod method, MongoOperations mongoOperations,
			SpelExpressionParser expressionParser, EvaluationContextProvider evaluationContextProvider) {

		super(method, mongoOperations);

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(expressionParser, "SpelExpressionParser must not be null!");

		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;

		this.queryParameterBindings = new ArrayList<ParameterBinding>();
		this.query = BINDING_PARSER.parseAndCollectParameterBindingsFromQueryIntoBindings(query,
				this.queryParameterBindings);

		this.fieldSpecParameterBindings = new ArrayList<ParameterBinding>();
		this.fieldSpec = BINDING_PARSER.parseAndCollectParameterBindingsFromQueryIntoBindings(
				method.getFieldSpecification(), this.fieldSpecParameterBindings);

		this.isCountQuery = method.hasAnnotatedQuery() ? method.getQueryAnnotation().count() : false;
		this.isDeleteQuery = method.hasAnnotatedQuery() ? method.getQueryAnnotation().delete() : false;

		if (isCountQuery && isDeleteQuery) {
			throw new IllegalArgumentException(String.format(COUND_AND_DELETE, method));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#createQuery(org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {

		String queryString = replacePlaceholders(query, accessor, queryParameterBindings);

		Query query = null;

		if (fieldSpec != null) {
			String fieldString = replacePlaceholders(fieldSpec, accessor, fieldSpecParameterBindings);
			query = new BasicQuery(queryString, fieldString);
		} else {
			query = new BasicQuery(queryString);
		}

		query.with(accessor.getSort());

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Created query %s", query.getQueryObject()));
		}

		return query;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isCountQuery()
	 */
	@Override
	protected boolean isCountQuery() {
		return isCountQuery;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isDeleteQuery()
	 */
	@Override
	protected boolean isDeleteQuery() {
		return this.isDeleteQuery;
	}

	/**
	 * Replaced the parameter place-holders with the actual parameter values from the given {@link ParameterBinding}s.
	 * 
	 * @param input
	 * @param accessor
	 * @param bindings
	 * @return
	 */
	private String replacePlaceholders(String input, ConvertingParameterAccessor accessor, List<ParameterBinding> bindings) {

		if (bindings.isEmpty()) {
			return input;
		}

		boolean isCompletlyParameterizedQuery = input.matches("^\\?\\d+$");

		StringBuilder result = new StringBuilder(input);

		for (ParameterBinding binding : bindings) {

			String parameter = binding.getParameter();
			int idx = result.indexOf(parameter);

			if (idx != -1) {
				String valueForBinding = getParameterValueForBinding(accessor, binding);

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
	 * @param binding
	 * @return
	 */
	private String getParameterValueForBinding(ConvertingParameterAccessor accessor, ParameterBinding binding) {

		Object value = binding.isExpression() ? evaluateExpression(binding.getExpression(), accessor.getValues())
				: accessor.getBindableValue(binding.getParameterIndex());

		if (value instanceof String && binding.isQuoted()) {
			return (String) value;
		}

		return JSON.serialize(value);
	}

	/**
	 * Evaluates the given {@code expressionString}.
	 * 
	 * @param expressionString
	 * @param parameterValues
	 * @return
	 */
	private Object evaluateExpression(String expressionString, Object[] parameterValues) {

		EvaluationContext evaluationContext = evaluationContextProvider.getEvaluationContext(getQueryMethod()
				.getParameters(), parameterValues);
		Expression expression = expressionParser.parseExpression(expressionString);
		return expression.getValue(evaluationContext, Object.class);
	}

	/**
	 * A parser that extracts the parameter bindings from a given query string.
	 * 
	 * @author Thomas Darimont
	 */
	private static enum ParameterBindingParser {

		INSTANCE;

		private static final String EXPRESSION_PARAM_QUOTE = "'";
		private static final String EXPRESSION_PARAM_PREFIX = "?expr";
		private static final String INDEX_BASED_EXPRESSION_PARAM_START = "?#{";
		private static final String NAME_BASED_EXPRESSION_PARAM_START = ":#{";
		private static final char CURRLY_BRACE_OPEN = '{';
		private static final char CURRLY_BRACE_CLOSE = '}';
		private static final String PARAMETER_PREFIX = "_param_";
		private static final String PARSEABLE_PARAMETER = "\"" + PARAMETER_PREFIX + "$1\"";
		private static final Pattern PARAMETER_BINDING_PATTERN = Pattern.compile("\\?(\\d+)");
		private static final Pattern PARSEABLE_BINDING_PATTERN = Pattern.compile("\"?" + PARAMETER_PREFIX + "(\\d+)\"?");

		private final static int PARAMETER_INDEX_GROUP = 1;

		/**
		 * Returns a list of {@link ParameterBinding}s found in the given {@code input} or an
		 * {@link Collections#emptyList()}.
		 * 
		 * @param input can be {@literal null} or empty.
		 * @param bindings must not be {@literal null}.
		 * @return
		 */
		public String parseAndCollectParameterBindingsFromQueryIntoBindings(String input, List<ParameterBinding> bindings) {

			if (!StringUtils.hasText(input)) {
				return input;
			}

			Assert.notNull(bindings, "Parameter bindings must not be null!");

			String transformedInput = transformQueryAndCollectExpressionParametersIntoBindings(input, bindings);
			String parseableInput = makeParameterReferencesParseable(transformedInput);

			collectParameterReferencesIntoBindings(bindings, JSON.parse(parseableInput));

			return transformedInput;
		}

		private String transformQueryAndCollectExpressionParametersIntoBindings(String input,
				List<ParameterBinding> bindings) {

			StringBuilder result = new StringBuilder();

			int startIndex = 0;
			int currentPos = 0;
			int exprIndex = 0;

			while (currentPos < input.length()) {
				int indexOfExpressionParameter = input.indexOf(INDEX_BASED_EXPRESSION_PARAM_START, currentPos);

				if (indexOfExpressionParameter < 0) {
					indexOfExpressionParameter = input.indexOf(NAME_BASED_EXPRESSION_PARAM_START, currentPos);
				}

				if (indexOfExpressionParameter < 0) {
					// no expression parameter found
					break;
				}

				int exprStart = indexOfExpressionParameter + 3;
				currentPos = exprStart;

				// eat parameter expression
				int curlyBraceOpenCnt = 1;
				while (curlyBraceOpenCnt > 0) {
					char c = input.charAt(currentPos++);
					switch (c) {
						case CURRLY_BRACE_OPEN:
							curlyBraceOpenCnt++;
							break;
						case CURRLY_BRACE_CLOSE:
							curlyBraceOpenCnt--;
							break;
						default:
							;
					}
				}

				result.append(input.subSequence(startIndex, indexOfExpressionParameter));
				result.append(EXPRESSION_PARAM_QUOTE).append(EXPRESSION_PARAM_PREFIX).append(exprIndex)
						.append(EXPRESSION_PARAM_QUOTE);
				bindings.add(new ParameterBinding(exprIndex, true, input.substring(exprStart, currentPos - 1)));

				startIndex = currentPos;

				exprIndex++;
			}

			result.append(input.subSequence(currentPos, input.length()));

			return result.toString();
		}

		private String makeParameterReferencesParseable(String input) {

			Matcher matcher = PARAMETER_BINDING_PATTERN.matcher(input);
			String parseableInput = matcher.replaceAll(PARSEABLE_PARAMETER);

			return parseableInput;
		}

		private void collectParameterReferencesIntoBindings(List<ParameterBinding> bindings, Object value) {

			if (value instanceof String) {

				String string = ((String) value).trim();
				potentiallyAddBinding(string, bindings);

			} else if (value instanceof Pattern) {

				String string = ((Pattern) value).toString().trim();
				Matcher valueMatcher = PARSEABLE_BINDING_PATTERN.matcher(string);

				while (valueMatcher.find()) {

					int paramIndex = Integer.parseInt(valueMatcher.group(PARAMETER_INDEX_GROUP));

					/*
					 * The pattern is used as a direct parameter replacement, e.g. 'field': ?1, 
					 * therefore we treat it as not quoted to remain backwards compatible.
					 */
					boolean quoted = !string.equals(PARAMETER_PREFIX + paramIndex);

					bindings.add(new ParameterBinding(paramIndex, quoted));
				}

			} else if (value instanceof DBRef) {

				DBRef dbref = (DBRef) value;

				potentiallyAddBinding(dbref.getCollectionName(), bindings);
				potentiallyAddBinding(dbref.getId().toString(), bindings);

			} else if (value instanceof DBObject) {

				DBObject dbo = (DBObject) value;

				for (String field : dbo.keySet()) {
					collectParameterReferencesIntoBindings(bindings, field);
					collectParameterReferencesIntoBindings(bindings, dbo.get(field));
				}
			}
		}

		private void potentiallyAddBinding(String source, List<ParameterBinding> bindings) {

			Matcher valueMatcher = PARSEABLE_BINDING_PATTERN.matcher(source);

			while (valueMatcher.find()) {

				int paramIndex = Integer.parseInt(valueMatcher.group(PARAMETER_INDEX_GROUP));
				boolean quoted = (source.startsWith("'") && source.endsWith("'"))
						|| (source.startsWith("\"") && source.endsWith("\""));

				bindings.add(new ParameterBinding(paramIndex, quoted));
			}
		}
	}

	/**
	 * A generic parameter binding with name or position information.
	 * 
	 * @author Thomas Darimont
	 */
	private static class ParameterBinding {

		private final int parameterIndex;
		private final boolean quoted;
		private final String expression;

		/**
		 * Creates a new {@link ParameterBinding} with the given {@code parameterIndex} and {@code quoted} information.
		 * 
		 * @param parameterIndex
		 * @param quoted whether or not the parameter is already quoted.
		 */
		public ParameterBinding(int parameterIndex, boolean quoted) {
			this(parameterIndex, quoted, null);
		}

		public ParameterBinding(int parameterIndex, boolean quoted, String expression) {

			this.parameterIndex = parameterIndex;
			this.quoted = quoted;
			this.expression = expression;
		}

		public boolean isQuoted() {
			return quoted;
		}

		public int getParameterIndex() {
			return parameterIndex;
		}

		public String getParameter() {
			return "?" + (isExpression() ? "expr" : "") + parameterIndex;
		}

		public String getExpression() {
			return expression;
		}

		public boolean isExpression() {
			return this.expression != null;
		}
	}
}
