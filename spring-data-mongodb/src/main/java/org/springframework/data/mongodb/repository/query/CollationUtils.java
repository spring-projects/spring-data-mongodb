/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Internal utility class to help avoid duplicate code required in both the reactive and the sync {@link Collation}
 * support offered by repositories.
 *
 * @author Christoph Strobl
 * @since 2.2
 */
abstract class CollationUtils {

	private static final ParameterBindingDocumentCodec CODEC = new ParameterBindingDocumentCodec();
	private static final Pattern PARAMETER_BINDING_PATTERN = Pattern.compile("\\?(\\d+)");

	private CollationUtils() {
	}

	/**
	 * Compute the {@link Collation} by inspecting the {@link ConvertingParameterAccessor#getCollation() parameter
	 * accessor} or parsing a potentially given {@literal collationExpression}.
	 *
	 * @param collationExpression
	 * @param accessor
	 * @param parameters
	 * @param expressionParser
	 * @param evaluationContextProvider
	 * @return can be {@literal null} if neither {@link ConvertingParameterAccessor#getCollation()} nor
	 *         {@literal collationExpression} are present.
	 */
	@Nullable
	static Collation computeCollation(@Nullable String collationExpression, ConvertingParameterAccessor accessor,
			MongoParameters parameters, SpelExpressionParser expressionParser,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {

		if (accessor.getCollation() != null) {
			return accessor.getCollation();
		}

		if (!StringUtils.hasText(collationExpression)) {
			return null;
		}

		if (StringUtils.trimLeadingWhitespace(collationExpression).startsWith("{")) {

			ParameterBindingContext bindingContext = ParameterBindingContext.forExpressions(accessor::getBindableValue,
					expressionParser, dependencies -> evaluationContextProvider.getEvaluationContext(parameters,
							accessor.getValues(), dependencies));

			return Collation.from(CODEC.decode(collationExpression, bindingContext));
		}

		Matcher matcher = PARAMETER_BINDING_PATTERN.matcher(collationExpression);
		if (!matcher.find()) {
			return Collation.parse(collationExpression);
		}

		String placeholder = matcher.group();
		Object placeholderValue = accessor.getBindableValue(computeParameterIndex(placeholder));

		if (collationExpression.startsWith("?")) {

			if (placeholderValue instanceof String) {
				return Collation.parse(placeholderValue.toString());
			}
			if (placeholderValue instanceof Locale) {
				return Collation.of((Locale) placeholderValue);
			}
			if (placeholderValue instanceof Document) {
				return Collation.from((Document) placeholderValue);
			}
			throw new IllegalArgumentException(String.format("Collation must be a String, Locale or Document but was %s",
					ObjectUtils.nullSafeClassName(placeholderValue)));
		}

		return Collation.parse(collationExpression.replace(placeholder, placeholderValue.toString()));
	}

	private static int computeParameterIndex(String parameter) {
		return NumberUtils.parseNumber(parameter.replace("?", ""), Integer.class);
	}
}
