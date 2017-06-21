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
package org.springframework.data.mongodb.core.query;

import java.util.regex.Pattern;

import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.util.StringMatcher;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.8
 */
public enum MongoRegexCreator {

	INSTANCE;

	private static final Pattern PUNCTATION_PATTERN = Pattern.compile("\\p{Punct}");

	/**
	 * Creates a regular expression String to be used with {@code $regex}.
	 * 
	 * @param source the plain String
	 * @param type
	 * @return {@literal source} when {@literal source} or {@literal type} is {@literal null}.
	 * @deprecated use the {@link MongoRegexCreator#toRegularExpression(String, StringMatcher)} instead
	 */
	@Deprecated
	public String toRegularExpression(String source, Type type) {

		return toRegularExpression(source, convert(type));
	}

	/**
	 * Creates a regular expression String to be used with {@code $regex}.
	 *
	 * @param source the plain String
	 * @param matcherType the type of matching to perform
	 * @return {@literal source} when {@literal source} or {@literal matcherType} is {@literal null}.
	 */
	public String toRegularExpression(String source, StringMatcher matcherType) {

		if (matcherType == null || source == null) {
			return source;
		}

		String regex = prepareAndEscapeStringBeforeApplyingLikeRegex(source, matcherType);

		switch (matcherType) {
			case STARTING:
				regex = "^" + regex;
				break;
			case ENDING:
				regex = regex + "$";
				break;
			case CONTAINING:
				regex = ".*" + regex + ".*";
				break;
			case EXACT:
				regex = "^" + regex + "$";
			default:
		}

		return regex;
	}

	private String prepareAndEscapeStringBeforeApplyingLikeRegex(String source, StringMatcher matcherType) {

		if (StringMatcher.REGEX == matcherType) {
			return source;
		}

		if (StringMatcher.LIKE != matcherType) {
			return PUNCTATION_PATTERN.matcher(source).find() ? Pattern.quote(source) : source;
		}

		if (source.equals("*")) {
			return ".*";
		}

		StringBuilder sb = new StringBuilder();

		boolean leadingWildcard = source.startsWith("*");
		boolean trailingWildcard = source.endsWith("*");

		String valueToUse = source.substring(leadingWildcard ? 1 : 0,
				trailingWildcard ? source.length() - 1 : source.length());

		if (PUNCTATION_PATTERN.matcher(valueToUse).find()) {
			valueToUse = Pattern.quote(valueToUse);
		}

		if (leadingWildcard) {
			sb.append(".*");
		}
		sb.append(valueToUse);
		if (trailingWildcard) {
			sb.append(".*");
		}

		return sb.toString();
	}

	private StringMatcher convert(Type type) {

		if (type == null)
			return null;

		switch (type) {

			case NOT_LIKE:
			case LIKE:
				return StringMatcher.LIKE;
			case STARTING_WITH:
				return StringMatcher.STARTING;
			case ENDING_WITH:
				return StringMatcher.ENDING;
			case NOT_CONTAINING:
			case CONTAINING:
				return StringMatcher.CONTAINING;
			case REGEX:
				return StringMatcher.REGEX;
			case SIMPLE_PROPERTY:
			case NEGATING_SIMPLE_PROPERTY:
				return StringMatcher.EXACT;
			case BETWEEN:
			case IS_NOT_NULL:
			case IS_NULL:
			case LESS_THAN:
			case LESS_THAN_EQUAL:
			case GREATER_THAN:
			case GREATER_THAN_EQUAL:
			case BEFORE:
			case AFTER:
			case EXISTS:
			case TRUE:
			case FALSE:
			case NOT_IN:
			case IN:
			case NEAR:
			case WITHIN:
			case IS_NOT_EMPTY:
			case IS_EMPTY:
				return StringMatcher.DEFAULT;
		}
		throw new IllegalStateException("Execution should never reach this position.");
	}

}
