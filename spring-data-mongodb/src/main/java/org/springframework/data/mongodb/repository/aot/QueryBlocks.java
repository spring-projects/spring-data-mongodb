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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.bson.Document;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.annotation.Collation;
import org.springframework.data.mongodb.core.geo.GeoJson;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.repository.Hint;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.query.MongoParameters.MongoParameter;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.SlicedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.util.ClassUtils;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @since 5.0
 */
class QueryBlocks {

	@NullUnmarked
	static class QueryExecutionCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private QueryInteraction query;

		QueryExecutionCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;
		}

		QueryExecutionCodeBlockBuilder forQuery(QueryInteraction query) {

			this.query = query;
			return this;
		}

		CodeBlock build() {

			String mongoOpsRef = context.fieldNameOf(MongoOperations.class);

			Builder builder = CodeBlock.builder();

			boolean isProjecting = context.getReturnedType().isProjecting();
			Class<?> domainType = context.getRepositoryInformation().getDomainType();
			Object actualReturnType = queryMethod.getParameters().hasDynamicProjection() || isProjecting
					? TypeName.get(context.getActualReturnType().getType())
					: domainType;

			builder.add("\n");

			if (queryMethod.getParameters().hasDynamicProjection()) {
				builder.addStatement("$T<$T> $L = $L.query($T.class).as($L)", FindWithQuery.class, actualReturnType,
						context.localVariable("finder"), mongoOpsRef, domainType, context.getDynamicProjectionParameterName());
			} else if (isProjecting) {
				builder.addStatement("$T<$T> $L = $L.query($T.class).as($T.class)", FindWithQuery.class, actualReturnType,
						context.localVariable("finder"), mongoOpsRef, domainType, actualReturnType);
			} else {

				builder.addStatement("$T<$T> $L = $L.query($T.class)", FindWithQuery.class, actualReturnType,
						context.localVariable("finder"), mongoOpsRef, domainType);
			}

			String terminatingMethod;

			if (queryMethod.isCollectionQuery() || queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
				terminatingMethod = "all()";
			} else if (query.isCount()) {
				terminatingMethod = "count()";
			} else if (query.isExists()) {
				terminatingMethod = "exists()";
			} else if (queryMethod.isStreamQuery()) {
				terminatingMethod = "stream()";
			} else {
				terminatingMethod = Optional.class.isAssignableFrom(context.getReturnType().toClass()) ? "one()" : "oneValue()";
			}

			if (queryMethod.isPageQuery()) {
				builder.addStatement("return new $T($L, $L).execute($L)", PagedExecution.class, context.localVariable("finder"),
						context.getPageableParameterName(), query.name());
			} else if (queryMethod.isSliceQuery()) {
				builder.addStatement("return new $T($L, $L).execute($L)", SlicedExecution.class,
						context.localVariable("finder"), context.getPageableParameterName(), query.name());
			} else if (queryMethod.isScrollQuery()) {

				String scrollPositionParameterName = context.getScrollPositionParameterName();

				builder.addStatement("return $L.matching($L).scroll($L)", context.localVariable("finder"), query.name(),
						scrollPositionParameterName);
			} else {
				if (query.isCount() && !ClassUtils.isAssignable(Long.class, context.getActualReturnType().getRawClass())) {

					Class<?> returnType = ClassUtils.resolvePrimitiveIfNecessary(queryMethod.getReturnedObjectType());
					builder.addStatement("return $T.convertNumberToTargetClass($L.matching($L).$L, $T.class)", NumberUtils.class,
							context.localVariable("finder"), query.name(), terminatingMethod, returnType);

				} else {
					builder.addStatement("return $L.matching($L).$L", context.localVariable("finder"), query.name(),
							terminatingMethod);
				}
			}

			return builder.build();
		}
	}

	@NullUnmarked
	static class QueryCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;

		private QueryInteraction source;
		private final Map<String, CodeBlock> arguments;
		private String queryVariableName;

		QueryCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;

			this.arguments = new LinkedHashMap<>();
			this.queryMethod = queryMethod;
			collectArguments(context);

		}

		private void collectArguments(AotQueryMethodGenerationContext context) {

			for (MongoParameter parameter : queryMethod.getParameters().getBindableParameters()) {
				String parameterName = context.getParameterName(parameter.getIndex());
				if (ClassUtils.isAssignable(GeoJson.class, parameter.getType())) {

					// renders as generic $geometry, thus can be handled by the converter when parsing
					arguments.put(parameterName, CodeBlock.of(parameterName));
				} else if (ClassUtils.isAssignable(Circle.class, parameter.getType())
						|| ClassUtils.isAssignable(Sphere.class, parameter.getType())) {

					// $center | $centerSphere : [ [ <x>, <y> ], <radius> ]
					arguments.put(parameterName, CodeBlock.builder().add(
							"$1T.of($1T.of($2L.getCenter().getX(), $2L.getCenter().getY()), $2L.getRadius().getNormalizedValue())",
							List.class, parameterName).build());
				} else if (ClassUtils.isAssignable(Box.class, parameter.getType())) {

					// $box: [ [ <x1>, <y1> ], [ <x2>, <y2> ] ]
					arguments.put(parameterName, CodeBlock.builder().add(
							"$1T.of($1T.of($2L.getFirst().getX(), $2L.getFirst().getY()), $1T.of($2L.getSecond().getX(), $2L.getSecond().getY()))",
							List.class, parameterName).build());
				} else if (ClassUtils.isAssignable(Polygon.class, parameter.getType())) {

					// $polygon: [ [ <x1> , <y1> ], [ <x2> , <y2> ], [ <x3> , <y3> ], ... ]
					String localVar = context.localVariable("_p");
					arguments.put(parameterName,
							CodeBlock.builder().add("$1L.getPoints().stream().map($2L -> $3T.of($2L.getX(), $2L.getY())).toList()",
									parameterName, localVar, List.class).build());
				} else {
					arguments.put(parameterName, CodeBlock.of(parameterName));
				}
			}
		}

		QueryCodeBlockBuilder filter(QueryInteraction query) {

			this.source = query;
			return this;
		}

		QueryCodeBlockBuilder usingQueryVariableName(String queryVariableName) {

			this.queryVariableName = queryVariableName;
			return this;
		}

		CodeBlock build() {

			Builder builder = CodeBlock.builder();

			builder.add(buildJustTheQuery());

			if (StringUtils.hasText(source.getQuery().getFieldsString())) {

				VariableSnippet fields = Snippet.declare(builder).variable(Document.class, context.localVariable("fields"))
						.of(MongoCodeBlocks.asDocument(source.getQuery().getFieldsString(), arguments));
				builder.addStatement("$L.setFieldsObject($L)", queryVariableName, fields.getVariableName());
			}

			String sortParameter = context.getSortParameterName();
			if (StringUtils.hasText(sortParameter)) {
				builder.addStatement("$L.with($L)", queryVariableName, sortParameter);
			} else if (StringUtils.hasText(source.getQuery().getSortString())) {

				VariableSnippet sort = Snippet.declare(builder).variable(Document.class, context.localVariable("sort"))
						.of(MongoCodeBlocks.asDocument(source.getQuery().getSortString(), arguments));
				builder.addStatement("$L.setSortObject($L)", queryVariableName, sort.getVariableName());
			}

			String limitParameter = context.getLimitParameterName();
			if (StringUtils.hasText(limitParameter)) {
				builder.addStatement("$L.limit($L)", queryVariableName, limitParameter);
			} else if (context.getPageableParameterName() == null && source.getQuery().isLimited()) {
				builder.addStatement("$L.limit($L)", queryVariableName, source.getQuery().getLimit());
			}

			String pageableParameter = context.getPageableParameterName();
			if (StringUtils.hasText(pageableParameter) && !queryMethod.isPageQuery() && !queryMethod.isSliceQuery()) {
				builder.addStatement("$L.with($L)", queryVariableName, pageableParameter);
			}

			MergedAnnotation<Hint> hintAnnotation = context.getAnnotation(Hint.class);
			String hint = hintAnnotation.isPresent() ? hintAnnotation.getString("value") : null;

			if (StringUtils.hasText(hint)) {
				builder.addStatement("$L.withHint($S)", queryVariableName, hint);
			}

			MongoCodeBlocks.appendReadPreference(context, builder, queryVariableName);

			MergedAnnotation<Meta> metaAnnotation = context.getAnnotation(Meta.class);
			if (metaAnnotation.isPresent()) {

				long maxExecutionTimeMs = metaAnnotation.getLong("maxExecutionTimeMs");
				if (maxExecutionTimeMs != -1) {
					builder.addStatement("$L.maxTimeMsec($L)", queryVariableName, maxExecutionTimeMs);
				}

				int cursorBatchSize = metaAnnotation.getInt("cursorBatchSize");
				if (cursorBatchSize != 0) {
					builder.addStatement("$L.cursorBatchSize($L)", queryVariableName, cursorBatchSize);
				}

				String comment = metaAnnotation.getString("comment");
				if (StringUtils.hasText(comment)) {
					builder.addStatement("$L.comment($S)", queryVariableName, comment);
				}
			}

			MergedAnnotation<Collation> collationAnnotation = context.getAnnotation(Collation.class);
			if (collationAnnotation.isPresent()) {

				String collationString = collationAnnotation.getString("value");
				if (StringUtils.hasText(collationString)) {
					if (!MongoCodeBlocks.containsPlaceholder(collationString)) {
						builder.addStatement("$L.collation($T.parse($S))", queryVariableName,
								org.springframework.data.mongodb.core.query.Collation.class, collationString);
					} else {
						builder.add("$L.collation(collationOf(evaluate($S, ", queryVariableName, collationString);
						builder.add(MongoCodeBlocks.renderArgumentMap(arguments));
						builder.add(")));\n");
					}
				}
			}

			return builder.build();
		}

		CodeBlock buildJustTheQuery() {

			Builder builder = CodeBlock.builder();
			builder.add("\n");

			Snippet.declare(builder).variable(BasicQuery.class, this.queryVariableName).of(renderExpressionToQuery());
			return builder.build();
		}

		private CodeBlock renderExpressionToQuery() {

			String source = this.source.getQuery().getQueryString();
			if (!StringUtils.hasText(source)) {
				return CodeBlock.of("new $T(new $T())", BasicQuery.class, Document.class);
			}
			if (!MongoCodeBlocks.containsPlaceholder(source)) {
				return CodeBlock.of("new $T($T.parse($S))", BasicQuery.class, Document.class, source);
			}
			Builder builder = CodeBlock.builder();
			builder.add("createQuery($S, ", source);
			if (MongoCodeBlocks.containsNamedPlaceholder(source)) {
				builder.add(MongoCodeBlocks.renderArgumentMap(arguments));
			} else {
				builder.add(MongoCodeBlocks.renderArgumentArray(arguments));
			}
			builder.add(")");
			return builder.build();
		}
	}
}
