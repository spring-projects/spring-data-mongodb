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
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
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
	 */
	public String toRegularExpression(String source, Type type) {

		if (type == null || source == null) {
			return source;
		}

		String regex = prepareAndEscapeStringBeforeApplyingLikeRegex(source, type);

		switch (type) {
			case STARTING_WITH:
				regex = "^" + regex;
				break;
			case ENDING_WITH:
				regex = regex + "$";
				break;
			case CONTAINING:
			case NOT_CONTAINING:
				regex = ".*" + regex + ".*";
				break;
			case SIMPLE_PROPERTY:
			case NEGATING_SIMPLE_PROPERTY:
				regex = "^" + regex + "$";
			default:
		}

		return regex;
	}

	private String prepareAndEscapeStringBeforeApplyingLikeRegex(String source, Type type) {

		if (ObjectUtils.nullSafeEquals(Type.REGEX, type)) {
			return source;
		}

		if (!ObjectUtils.nullSafeEquals(Type.LIKE, type) && !ObjectUtils.nullSafeEquals(Type.NOT_LIKE, type)) {
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

}
