/*
 * Copyright 2025-present the original author or authors.
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

import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.annotation.Collation;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.DiskUse;
import org.springframework.data.mongodb.repository.Hint;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.SlicedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.repository.aot.generate.MethodReturn;
import org.springframework.data.util.Lazy;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
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

			MethodReturn methodReturn = context.getMethodReturn();
			String mongoOpsRef = context.fieldNameOf(MongoOperations.class);

			Builder builder = CodeBlock.builder();

			boolean isProjecting = context.getReturnedType().isProjecting();
			Class<?> domainType = context.getRepositoryInformation().getDomainType();
			Object actualReturnType = queryMethod.getParameters().hasDynamicProjection() || isProjecting
					? methodReturn.getActualTypeName()
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
				if (query.getQuery().isLimited()) {
					terminatingMethod = Optional.class.isAssignableFrom(methodReturn.toClass()) ? "first()"
							: "firstValue()";
				} else {
					terminatingMethod = Optional.class.isAssignableFrom(methodReturn.toClass()) ? "one()"
							: "oneValue()";
				}
			}

			if (queryMethod.isPageQuery()) {

				builder.addStatement("return new $T($L, $L).execute($L)", PagedExecution.class, context.localVariable("finder"),
						context.getPageableParameterName(), query.name());
			} else if (queryMethod.isSliceQuery()) {
				builder.addStatement("return new $T($L, $L).execute($L)", SlicedExecution.class,
						context.localVariable("finder"), context.getPageableParameterName(), query.name());
			} else if (queryMethod.isScrollQuery()) {

				String scrollPositionParameterName = context.getScrollPositionParameterName();

				if (scrollPositionParameterName != null) {

					builder.addStatement("return $L.matching($L).scroll($L)", context.localVariable("finder"), query.name(),
							scrollPositionParameterName);
				} else {
					String pageableParameterName = context.getPageableParameterName();
					if (pageableParameterName != null) {
						builder.addStatement("return $L.matching($L).scroll($L.toScrollPosition())",
								context.localVariable("finder"), query.name(), pageableParameterName);
					} else {
						builder.addStatement("return $L.matching($L).scroll($T.initial())", context.localVariable("finder"),
								query.name(), ScrollPosition.class);
					}
				}
			} else {

				if (query.isCount() && !ClassUtils.isAssignable(Long.class, methodReturn.getActualReturnClass())) {

					Class<?> returnType = ClassUtils.resolvePrimitiveIfNecessary(queryMethod.getReturnedObjectType());
					builder.addStatement("return $T.convertNumberToTargetClass($L.matching($L).$L, $T.class)", NumberUtils.class,
							context.localVariable("finder"), query.name(), terminatingMethod, returnType);

				} else {

					CodeBlock resultBlock = CodeBlock.of("$L.matching($L).$L", context.localVariable("finder"), query.name(),
							terminatingMethod);

					builder.addStatement("return $L", MongoCodeBlocks.potentiallyWrapStreamable(methodReturn, resultBlock));
				}
			}

			return builder.build();
		}
	}

	@NullUnmarked
	static class QueryCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private final Lazy<CodeBlock> queryParameters;

		private QueryInteraction source;
		private String queryVariableName;

		QueryCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;
			this.queryParameters = Lazy.of(this::queryParametersCodeBlock);
		}

		CodeBlock queryParametersCodeBlock() {

			List<String> allParameterNames = context.getAllParameterNames();

			if (allParameterNames.isEmpty()) {
				return CodeBlock.builder().build();
			}

			CodeBlock.Builder formatted = CodeBlock.builder();
			boolean containsArrayParameter = false;
			for (int i = 0; i < allParameterNames.size(); i++) {

				String parameterName = allParameterNames.get(i);
				Class<?> parameterType = context.getMethodParameter(parameterName).getParameterType();
				if (source.getQuery().isRegexPlaceholderAt(i) && parameterType == String.class) {
					String regexOptions = source.getQuery().getRegexOptions(i);

					if (StringUtils.hasText(regexOptions)) {
						formatted.add(CodeBlock.of("toRegex($L)", parameterName));
					} else {
						formatted.add(CodeBlock.of("toRegex($L, $S)", parameterName, regexOptions));
					}
				} else {
					formatted.add(CodeBlock.of("$L", parameterName));
				}

				if (i + 1 < allParameterNames.size()) {
					formatted.add(", ");
				}

				if (!containsArrayParameter && parameterType != null && parameterType.isArray()) {
					containsArrayParameter = true;
				}
			}

			// wrap single array argument to avoid problems with vargs when calling method
			if (containsArrayParameter && allParameterNames.size() == 1) {
				return CodeBlock.of("new $T[] { $L }", Object.class, formatted.build());
			}

			return formatted.build();
		}

		public CodeBlock getQueryParameters() {
			return queryParameters.get();
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
						.of(MongoCodeBlocks.asDocument(context.getExpressionMarker(), source.getQuery().getFieldsString(),
								queryParameters.get()));
				builder.addStatement("$L.setFieldsObject($L)", queryVariableName, fields.getVariableName());
			}

			if (StringUtils.hasText(source.getQuery().getSortString())) {

				VariableSnippet sort = Snippet.declare(builder).variable(Document.class, context.localVariable("sort"))
						.of(MongoCodeBlocks.asDocument(context.getExpressionMarker(), source.getQuery().getSortString(),
								getQueryParameters()));
				builder.addStatement("$L.setSortObject($L)", queryVariableName, sort.getVariableName());
			}

			String sortParameter = context.getSortParameterName();
			if (StringUtils.hasText(sortParameter)) {
				builder.addStatement("$L.with($L)", queryVariableName, sortParameter);
			}

			String limitParameter = context.getLimitParameterName();
			if (StringUtils.hasText(limitParameter)) {
				builder.addStatement("$L.limit($L)", queryVariableName, limitParameter);
			} else if (source.getQuery().isLimited()) {
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

				String allowDiskUse = metaAnnotation.getString("allowDiskUse");
				if (StringUtils.hasText(allowDiskUse)) {
					DiskUse diskUse = DiskUse.of(allowDiskUse);
					builder.addStatement("$L.diskUse($T.$L)", queryVariableName, DiskUse.class, diskUse.name());
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

						if (getQueryParameters().isEmpty()) {
							builder.addStatement("$L.collation(collationOf(evaluate($L, $S)))", queryVariableName,
									context.getExpressionMarker().enclosingMethod(), collationString);
						} else {
							builder.addStatement("$L.collation(collationOf(evaluate($L, $S, $L)))", queryVariableName,
									context.getExpressionMarker().enclosingMethod(), collationString, getQueryParameters());
						}
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
			} else if (MongoCodeBlocks.containsPlaceholder(source)) {
				Builder builder = CodeBlock.builder();
				if (getQueryParameters().isEmpty()) {
					builder.add("createQuery($L, $S)", context.getExpressionMarker().enclosingMethod(), source);
				} else {
					builder.add("createQuery($L, $S, $L)", context.getExpressionMarker().enclosingMethod(), source,
							getQueryParameters());
				}
				return builder.build();
			} else {
				return CodeBlock.of("new $T(parse($S))", BasicQuery.class, source);
			}
		}
	}
}
