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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
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
	static UpdateCodeBlockBuilder updateBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {

		return new UpdateCodeBlockBuilder(context, queryMethod);
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
	 * @param queryMethod
	 * @return
	 */
	static GeoNearExecutionCodeBlockBuilder geoNearExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {

		return new GeoNearExecutionCodeBlockBuilder(context, queryMethod);
	}

	static CodeBlock renderExpressionToDocument(@Nullable String source, String variableName,
			Map<String, CodeBlock> arguments) {

		Builder builder = CodeBlock.builder();
		if (!StringUtils.hasText(source)) {
			builder.addStatement("$1T $2L = new $1T()", Document.class, variableName);
		} else if (!containsPlaceholder(source)) {
			builder.addStatement("$1T $2L = $1T.parse($3S)", Document.class, variableName, source);
		} else {
			builder.add("$T $L = bindParameters($S, ", Document.class, variableName, source);
			if (containsNamedPlaceholder(source)) {
				renderArgumentMap(arguments);
			} else {
				builder.add(renderArgumentArray(arguments));
			}
			builder.add(");\n");
		}
		return builder.build();
	}

	static CodeBlock renderArgumentMap(Map<String, CodeBlock> arguments) {

		Builder builder = CodeBlock.builder();
		builder.add("$T.of(", Map.class);
		Iterator<Entry<String, CodeBlock>> iterator = arguments.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, CodeBlock> next = iterator.next();
			builder.add("$S, ", next.getKey());
			builder.add(next.getValue());
			if (iterator.hasNext()) {
				builder.add(", ");
			}
		}
		builder.add(")");
		return builder.build();
	}

	static CodeBlock renderArgumentArray(Map<String, CodeBlock> arguments) {

		Builder builder = CodeBlock.builder();
		builder.add("new $T[]{ ", Object.class);
		Iterator<CodeBlock> iterator = arguments.values().iterator();
		while (iterator.hasNext()) {
			builder.add(iterator.next());
			if (iterator.hasNext()) {
				builder.add(", ");
			} else {
				builder.add(" ");
			}
		}
		builder.add("}");
		return builder.build();
	}

	static boolean containsPlaceholder(String source) {
		return containsIndexedPlaceholder(source) || containsNamedPlaceholder(source);
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
