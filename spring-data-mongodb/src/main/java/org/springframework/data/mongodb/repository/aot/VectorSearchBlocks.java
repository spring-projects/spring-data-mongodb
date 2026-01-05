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

import org.bson.Document;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.core.TypeInformation;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation.SearchType;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.mongodb.repository.aot.Snippet.BuilderStyleBuilder;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.VectorSearchExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.util.StringUtils;

/**
 * Code blocks for building vector search operations in AOT processing for MongoDB repositories.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
class VectorSearchBlocks {

	@NullUnmarked
	static class VectorSearchQueryCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private final VectorSearch vectorSearchAnnotation;
		private String searchQueryVariableName;
		private AotStringQuery filter;
		private final String searchPath;

		VectorSearchQueryCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod,
				String searchPath) {

			this.context = context;
			this.queryMethod = queryMethod;
			this.vectorSearchAnnotation = queryMethod.getRequiredVectorSearchAnnotation();
			this.searchPath = searchPath;
		}

		VectorSearchQueryCodeBlockBuilder withFilter(AotStringQuery filter) {
			this.filter = filter;
			return this;
		}

		VectorSearchQueryCodeBlockBuilder usingVariableName(String searchQueryVariableName) {

			this.searchQueryVariableName = searchQueryVariableName;
			return this;
		}

		CodeBlock build() {

			Builder builder = CodeBlock.builder();

			String vectorParameterName = context.getVectorParameterName();

			String indexName = vectorSearchAnnotation.indexName();
			SearchType searchType = vectorSearchAnnotation.searchType();

			ExpressionSnippet limit = getLimitExpression();

			if (limit.requiresEvaluation() && !StringUtils.hasText(vectorSearchAnnotation.numCandidates())
					&& (searchType == VectorSearchOperation.SearchType.ANN
							|| searchType == VectorSearchOperation.SearchType.DEFAULT)) {

				VariableSnippet variableBlock = limit.as(VariableSnippet::create)
						.variableName(context.localVariable("limitToUse"));
				variableBlock.renderDeclaration(builder);
				limit = variableBlock;
			}

			BuilderStyleBuilder vectorSearchOperationBuilder = Snippet.declare(builder)
					.variableBuilder(VectorSearchOperation.class, context.localVariable("$vectorSearch"))
					.as("$T.vectorSearch($S).path($S).vector($L).limit($L)", Aggregation.class, indexName, searchPath,
							vectorParameterName, limit.code());

			if (!searchType.equals(SearchType.DEFAULT)) {
				vectorSearchOperationBuilder.call("searchType").with("$T.$L", SearchType.class, searchType.name());
			}

			ExpressionSnippet numCandidates = getNumCandidatesExpression(searchType, limit);
			if (!numCandidates.isEmpty()) {
				vectorSearchOperationBuilder.call("numCandidates").with(numCandidates);
			}

			vectorSearchOperationBuilder.call("withSearchScore").with("\"__score__\"");

			if (StringUtils.hasText(context.getScoreParameterName())) {
				vectorSearchOperationBuilder.call("withFilterBySore").with("$1L -> { $1L.gt($2L.getValue()); }",
						context.localVariable("criteria"), context.getScoreParameterName());
			} else if (StringUtils.hasText(context.getScoreRangeParameterName())) {
				vectorSearchOperationBuilder.call("withFilterBySore")
						.with("scoreBetween($1L.getLowerBound(), $1L.getUpperBound())", context.getScoreRangeParameterName());
			}

			VariableSnippet vectorSearchOperation = vectorSearchOperationBuilder.variable();
			getFilter(vectorSearchOperation.getVariableName()).appendTo(builder);

			VariableSnippet sortStage = getSort().as(VariableSnippet::create).variableName(context.localVariable("$sort"));
			sortStage.renderDeclaration(builder);

			builder.add("\n");

			VariableSnippet aggregationPipeline = Snippet.declare(builder)
					.variable(AggregationPipeline.class, searchQueryVariableName).as("new $T($T.of($L, $L))",
							AggregationPipeline.class, List.class, vectorSearchOperation.getVariableName(), sortStage.code());

			String scoringFunctionVar = context.localVariable("scoringFunction");
			builder.add("$1T $2L = ", ScoringFunction.class, scoringFunctionVar);
			if (StringUtils.hasText(context.getScoreParameterName())) {
				builder.add("$L.getFunction();\n", context.getScoreParameterName());
			} else if (StringUtils.hasText(context.getScoreRangeParameterName())) {
				builder.add("scoringFunction($L);\n", context.getScoreRangeParameterName());
			} else {
				builder.add("$1T.unspecified();\n", ScoringFunction.class);
			}

			builder.addStatement(
					"return ($5T) new $1T($2L, $3T.class, $2L.getCollectionName($3T.class), $4T.of($5T.class), $6L, $7L).execute(null)",
					VectorSearchExecution.class, context.fieldNameOf(MongoOperations.class),
					context.getRepositoryInformation().getDomainType(), TypeInformation.class,
					queryMethod.getReturnType().getType(), aggregationPipeline.getVariableName(), scoringFunctionVar);
			return builder.build();
		}

		private ExpressionSnippet getSort() {

			if (!filter.isSorted()) {
				return new ExpressionSnippet(
						CodeBlock.of("$T.sort($T.Direction.DESC, $S)", Aggregation.class, Sort.class, "__score__"));
			}

			Builder builder = CodeBlock.builder();
			String ctx = context.localVariable("ctx");
			String mappedSort = context.localVariable("mappedSort");
			builder.add("($T) ($L) -> {\n", AggregationOperation.class, ctx);
			builder.indent();

			builder.add("$1T $4L = $5L.getMappedObject(parse($2S), $3T.class);\n", Document.class, filter.getSortString(),
					context.getMethodReturn().getActualClassName(), mappedSort, ctx);
			builder.add("return new $1T($2S, $3L.append(\"__score__\", -1));\n", Document.class, "$sort", mappedSort);
			builder.unindent();
			builder.add("}");

			return new ExpressionSnippet(builder.build());
		}

		private Snippet getFilter(String vectorSearchVar) {

			if (!StringUtils.hasText(filter.getQueryString())) {
				return ExpressionSnippet.empty();
			}

			Builder builder = CodeBlock.builder();
			String filterVar = context.localVariable("filter");
			builder.add(MongoCodeBlocks.queryBlockBuilder(context, queryMethod).usingQueryVariableName("filter")
					.filter(new QueryInteraction(this.filter, false, false, false)).buildJustTheQuery());
			builder.addStatement("$1L = $1L.filter($2L.getQueryObject())", vectorSearchVar, filterVar);
			builder.add("\n");

			return new ExpressionSnippet(builder.build());
		}

		private ExpressionSnippet getNumCandidatesExpression(SearchType searchType, ExpressionSnippet limit) {

			String numCandidates = vectorSearchAnnotation.numCandidates();

			if (StringUtils.hasText(numCandidates)) {
				if (MongoCodeBlocks.containsPlaceholder(numCandidates) || MongoCodeBlocks.containsExpression(numCandidates)) {
					return new ExpressionSnippet(
							MongoCodeBlocks.evaluateNumberPotentially(numCandidates, Integer.class, context), true);
				} else {
					return new ExpressionSnippet(CodeBlock.of("$L", numCandidates));
				}
			}

			if (searchType == VectorSearchOperation.SearchType.ANN
					|| searchType == VectorSearchOperation.SearchType.DEFAULT) {

				Builder builder = CodeBlock.builder();

				if (StringUtils.hasText(context.getLimitParameterName())) {
					builder.add("$L.max() * 20", context.getLimitParameterName());
				} else if (filter.isLimited()) {
					builder.add("$L", filter.getLimit() * 20);
				} else {
					builder.add("$L * 20", limit.code());
				}

				return new ExpressionSnippet(builder.build());
			}

			return ExpressionSnippet.empty();
		}

		private ExpressionSnippet getLimitExpression() {

			if (StringUtils.hasText(context.getLimitParameterName())) {
				return new ExpressionSnippet(CodeBlock.of("$L", context.getLimitParameterName()));
			}

			if (filter.isLimited()) {
				return new ExpressionSnippet(CodeBlock.of("$L", filter.getLimit()));
			}

			String limit = vectorSearchAnnotation.limit();

			if (StringUtils.hasText(limit)) {

				if (MongoCodeBlocks.containsPlaceholder(limit) || MongoCodeBlocks.containsExpression(limit)) {
					return new ExpressionSnippet(MongoCodeBlocks.evaluateNumberPotentially(limit, Integer.class, context),
							true);
				} else {
					return new ExpressionSnippet(CodeBlock.of("$L", limit));
				}
			}
			return new ExpressionSnippet(CodeBlock.of("$T.unlimited()", Limit.class));
		}
	}
}
