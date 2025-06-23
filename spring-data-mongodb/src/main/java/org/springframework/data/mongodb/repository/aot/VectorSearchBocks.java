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

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation.SearchType;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.VectorSearchExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.util.TypeInformation;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @since 5.0
 */
class VectorSearchBocks {

	static class VectorSearchQueryCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private String searchQueryVariableName;
		private StringQuery filter;
		private final Map<String, CodeBlock> arguments;

		VectorSearchQueryCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;
			this.arguments = new LinkedHashMap<>();
			context.getBindableParameterNames().forEach(it -> arguments.put(it, CodeBlock.of(it)));
		}

		VectorSearchQueryCodeBlockBuilder usingVariableName(String searchQueryVariableName) {

			this.searchQueryVariableName = searchQueryVariableName;
			return this;
		}

		CodeBlock build() {

			Builder builder = CodeBlock.builder();

			String vectorParameterName = context.getVectorParameterName();

			MergedAnnotation<VectorSearch> annotation = context.getAnnotation(VectorSearch.class);
			String searchPath = annotation.getString("path");
			String indexName = annotation.getString("indexName");
			String numCandidates = annotation.getString("numCandidates");
			SearchType searchType = annotation.getEnum("searchType", SearchType.class);
			String limit = annotation.getString("limit");

			if (!StringUtils.hasText(searchPath)) { // FIXME: somehow duplicate logic of AnnotatedQueryFactory

				Field[] declaredFields = context.getRepositoryInformation().getDomainType().getDeclaredFields();
				for (Field field : declaredFields) {
					if (Vector.class.isAssignableFrom(field.getType())) {
						searchPath = field.getName();
						break;
					}
				}

			}

			String vectorSearchVar = context.localVariable("$vectorSearch");
			builder.add("$T $L = $T.vectorSearch($S).path($S).vector($L)", VectorSearchOperation.class, vectorSearchVar,
					Aggregation.class, indexName, searchPath, vectorParameterName);

			if (StringUtils.hasText(context.getLimitParameterName())) {
					builder.add(".limit($L);\n", context.getLimitParameterName());
			} else if (filter.isLimited()) {
				builder.add(".limit($L);\n", filter.getLimit());
			} else if (StringUtils.hasText(limit)) {
				if (MongoCodeBlocks.containsPlaceholder(limit) || MongoCodeBlocks.containsExpression(limit)) {
					builder.add(".limit(");
					builder.add(MongoCodeBlocks.evaluateNumberPotentially(limit, Integer.class, arguments));
					builder.add(");\n");
				} else {
					builder.add(".limit($L);\n", limit);
				}
			} else {
				builder.add(".limit($T.unlimited());\n", Limit.class);
			}

			if (!searchType.equals(SearchType.DEFAULT)) {
				builder.addStatement("$1L = $1L.searchType($2T.$3L)", vectorSearchVar, SearchType.class, searchType.name());
			}

			if (StringUtils.hasText(numCandidates)) {
				builder.add("$1L = $1L.numCandidates(", vectorSearchVar);
				builder.add(MongoCodeBlocks.evaluateNumberPotentially(numCandidates, Integer.class, arguments));
				builder.add(");\n");
			} else if (searchType == VectorSearchOperation.SearchType.ANN
					|| searchType == VectorSearchOperation.SearchType.DEFAULT) {

				builder.add(
						"// MongoDB: We recommend that you specify a number at least 20 times higher than the number of documents to return\n");
				if (StringUtils.hasText(context.getLimitParameterName())) {
					builder.addStatement("$1L = $1L.numCandidates($2L.max() * 20)", vectorSearchVar,
							context.getLimitParameterName());
				} else if (StringUtils.hasText(limit)) {
					if (MongoCodeBlocks.containsPlaceholder(limit) || MongoCodeBlocks.containsExpression(limit)) {

						builder.add("$1L = $1L.numCandidates((", vectorSearchVar);
						builder.add(MongoCodeBlocks.evaluateNumberPotentially(limit, Integer.class, arguments));
						builder.add(") * 20);\n");
					} else {
						builder.addStatement("$1L = $1L.numCandidates($2L * 20)", vectorSearchVar, limit);
					}
				} else {
					builder.addStatement("$1L = $1L.numCandidates($2L)", vectorSearchVar, filter.getLimit() * 20);
				}
			}

			builder.addStatement("$1L = $1L.withSearchScore(\"__score__\")", vectorSearchVar);
			if (StringUtils.hasText(context.getScoreParameterName())) {

				String scoreCriteriaVar = context.localVariable("criteria");
				builder.addStatement("$1L = $1L.withFilterBySore($2L -> { $2L.gt($3L.getValue()); })", vectorSearchVar,
						scoreCriteriaVar, context.getScoreParameterName());
			} else if (StringUtils.hasText(context.getScoreRangeParameterName())) {
				builder.addStatement("$1L = $1L.withFilterBySore(scoreBetween($2L.getLowerBound(), $2L.getUpperBound()))",
						vectorSearchVar, context.getScoreRangeParameterName());
			}

			if (StringUtils.hasText(filter.getQueryString())) {

				String filterVar = context.localVariable("filter");
				builder.add(MongoCodeBlocks.queryBlockBuilder(context, queryMethod).usingQueryVariableName("filter")
						.filter(new QueryInteraction(this.filter, false, false, false)).buildJustTheQuery());
				builder.addStatement("$1L = $1L.filter($2L.getQueryObject())", vectorSearchVar, filterVar);
				builder.add("\n");
			}


			String sortStageVar = context.localVariable("$sort");
			if(filter.isSorted()) {

				builder.add("$T $L = (_ctx) -> {\n", AggregationOperation.class, sortStageVar);
				builder.indent();

				builder.addStatement("$1T _mappedSort = _ctx.getMappedObject($1T.parse($2S), $3T.class)", Document.class, filter.getSortString(), context.getActualReturnType().getType());
				builder.addStatement("return new $T($S, _mappedSort.append(\"__score__\", -1))", Document.class, "$sort");
				builder.unindent();
				builder.add("};");

			} else {
				builder.addStatement("var $L = $T.sort($T.Direction.DESC, $S)", sortStageVar, Aggregation.class, Sort.class, "__score__");
			}
			builder.add("\n");

			builder.addStatement("$1T $2L = new $1T($3T.of($4L, $5L))", AggregationPipeline.class, searchQueryVariableName,
					List.class, vectorSearchVar, sortStageVar);

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
					queryMethod.getReturnType().getType(), searchQueryVariableName, scoringFunctionVar);
			return builder.build();
		}

		public VectorSearchQueryCodeBlockBuilder withFilter(StringQuery filter) {
			this.filter = filter;
			return this;
		}
	}
}
