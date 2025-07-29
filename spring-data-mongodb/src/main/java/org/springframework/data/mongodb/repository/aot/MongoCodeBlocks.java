/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import java.util.regex.Pattern;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.aot.AggregationBlocks.AggregationCodeBlockBuilder;
import org.springframework.data.mongodb.repository.aot.AggregationBlocks.AggregationExecutionCodeBlockBuilder;
import org.springframework.data.mongodb.repository.aot.DeleteBlocks.DeleteExecutionCodeBlockBuilder;
import org.springframework.data.mongodb.repository.aot.GeoBlocks.GeoNearCodeBlockBuilder;
import org.springframework.data.mongodb.repository.aot.GeoBlocks.GeoNearExecutionCodeBlockBuilder;
import org.springframework.data.mongodb.repository.aot.QueryBlocks.QueryCodeBlockBuilder;
import org.springframework.data.mongodb.repository.aot.QueryBlocks.QueryExecutionCodeBlockBuilder;
import org.springframework.data.mongodb.repository.aot.UpdateBlocks.UpdateCodeBlockBuilder;
import org.springframework.data.mongodb.repository.aot.UpdateBlocks.UpdateExecutionCodeBlockBuilder;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

/**
 * {@link CodeBlock} generator for common tasks.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
class MongoCodeBlocks {

	private static final Pattern PARAMETER_BINDING_PATTERN = Pattern.compile("\\?(\\d+)");
	private static final Pattern EXPRESSION_BINDING_PATTERN = Pattern.compile("[\\?:][#$]\\{.*\\}");
	private static final Pattern VALUE_EXPRESSION_PATTERN = Pattern.compile("^#\\{.*}$");

	/**
	 * Builder for generating query parsing {@link CodeBlock}.
	 *
	 * @param context
	 * @param queryMethod
	 * @return new instance of {@link QueryCodeBlockBuilder}.
	 */
	static QueryCodeBlockBuilder queryBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {

		return new QueryCodeBlockBuilder(context, queryMethod);
	}

	/**
	 * Builder for generating finder query execution {@link CodeBlock}.
	 *
	 * @param context
	 * @param queryMethod
	 * @return
	 */
	static QueryExecutionCodeBlockBuilder queryExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {

		return new QueryExecutionCodeBlockBuilder(context, queryMethod);
	}

	/**
	 * Builder for generating delete execution {@link CodeBlock}.
	 *
	 * @param context
	 * @param queryMethod
	 * @return
	 */
	static DeleteExecutionCodeBlockBuilder deleteExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {

		return new DeleteExecutionCodeBlockBuilder(context, queryMethod);
	}

	/**
	 * Builder for generating update parsing {@link CodeBlock}.
	 *
	 * @param context
	 * @param queryMethod
	 * @return
	 */
	static UpdateCodeBlockBuilder updateBlockBuilder(AotQueryMethodGenerationContext context) {

		return new UpdateCodeBlockBuilder(context);
	}

	/**
	 * Builder for generating update execution {@link CodeBlock}.
	 *
	 * @param context
	 * @param queryMethod
	 * @return
	 */
	static UpdateExecutionCodeBlockBuilder updateExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {

		return new UpdateExecutionCodeBlockBuilder(context, queryMethod);
	}

	/**
	 * Builder for generating aggregation (pipeline) parsing {@link CodeBlock}.
	 *
	 * @param context
	 * @param queryMethod
	 * @return
	 */
	static AggregationCodeBlockBuilder aggregationBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {
		return new AggregationCodeBlockBuilder(context, queryMethod);
	}

	/**
	 * Builder for generating aggregation execution {@link CodeBlock}.
	 *
	 * @param context
	 * @param queryMethod
	 * @return
	 */
	static AggregationExecutionCodeBlockBuilder aggregationExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {
		return new AggregationExecutionCodeBlockBuilder(context, queryMethod);
	}

	/**
	 * Builder for generating {@link org.springframework.data.mongodb.core.query.NearQuery} {@link CodeBlock}.
	 *
	 * @param context
	 * @param queryMethod
	 * @return
	 */
	static GeoNearCodeBlockBuilder geoNearBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {

		return new GeoNearCodeBlockBuilder(context, queryMethod);
	}

	/**
	 * Builder for generating {@link org.springframework.data.mongodb.core.query.NearQuery} execution {@link CodeBlock}
	 * that can return {@link org.springframework.data.geo.GeoResults}.
	 *
	 * @param context
	 * @return
	 */
	static GeoNearExecutionCodeBlockBuilder geoNearExecutionBlockBuilder(AotQueryMethodGenerationContext context) {

		return new GeoNearExecutionCodeBlockBuilder(context);
	}

	static CodeBlock asDocument(String source, String argNames) {
		return asDocument(source, CodeBlock.of("$L", argNames));
	}

	static CodeBlock asDocument(String source, CodeBlock arguments) {

		Builder builder = CodeBlock.builder();
		if (!StringUtils.hasText(source)) {
			builder.add("new $T()", Document.class);
		} else if (containsPlaceholder(source)) {
			if (arguments.isEmpty()) {
				builder.add("bindParameters(ExpressionMarker.class.getEnclosingMethod(), $S)", source);
			} else {
				builder.add("bindParameters(ExpressionMarker.class.getEnclosingMethod(), $S, $L)", source, arguments);
			}
		} else {
			builder.add("parse($S)", source);
		}
		return builder.build();
	}

	static CodeBlock renderExpressionToDocument(@Nullable String source, String variableName, String argNames) {

		Builder builder = CodeBlock.builder();
		if (!StringUtils.hasText(source)) {
			builder.addStatement("$1T $2L = new $1T()", Document.class, variableName);
		} else if (containsPlaceholder(source)) {
			builder.add("$T $L = bindParameters(ExpressionMarker.class.getEnclosingMethod(), $S$L);\n", Document.class,
					variableName, source, argNames);
		} else {
			builder.addStatement("$1T $2L = parse($3S)", Document.class, variableName, source);
		}
		return builder.build();
	}

	static CodeBlock evaluateNumberPotentially(String value, Class<? extends Number> targetType,
			AotQueryMethodGenerationContext context) {
		try {
			Number number = NumberUtils.parseNumber(value, targetType);
			return CodeBlock.of("$L", number);
		} catch (IllegalArgumentException e) {

			String parameterNames = StringUtils.collectionToDelimitedString(context.getAllParameterNames(), ", ");

			if (StringUtils.hasText(parameterNames)) {
				parameterNames = ", " + parameterNames;
			} else {
				parameterNames = "";
			}

			Builder builder = CodeBlock.builder();
			builder.add("($T) evaluate(ExpressionMarker.class.getEnclosingMethod(), $S$L)", targetType, value,
					parameterNames);
			return builder.build();
		}
	}

	static boolean containsPlaceholder(String source) {
		return containsIndexedPlaceholder(source) || containsNamedPlaceholder(source);
	}

	static boolean containsExpression(String source) {
		return VALUE_EXPRESSION_PATTERN.matcher(source).find();
	}

	static boolean containsNamedPlaceholder(String source) {
		return EXPRESSION_BINDING_PATTERN.matcher(source).find();
	}

	static boolean containsIndexedPlaceholder(String source) {
		return PARAMETER_BINDING_PATTERN.matcher(source).find();
	}

	static void appendReadPreference(AotQueryMethodGenerationContext context, Builder builder, String queryVariableName) {

		MergedAnnotation<ReadPreference> readPreferenceAnnotation = context.getAnnotation(ReadPreference.class);
		String readPreference = readPreferenceAnnotation.isPresent() ? readPreferenceAnnotation.getString("value") : null;

		if (StringUtils.hasText(readPreference)) {
			builder.addStatement("$L.withReadPreference($T.valueOf($S))", queryVariableName, com.mongodb.ReadPreference.class,
					readPreference);
		}
	}
}
