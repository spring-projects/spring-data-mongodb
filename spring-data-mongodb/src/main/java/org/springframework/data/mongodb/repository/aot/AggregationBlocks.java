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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.bson.Document;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.repository.Hint;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Code blocks for building aggregation pipelines and execution statements for MongoDB repositories.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
class AggregationBlocks {

	@NullUnmarked
	static class AggregationExecutionCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final SimpleTypeHolder simpleTypeHolder;
		private final MongoQueryMethod queryMethod;
		private String aggregationVariableName;

		AggregationExecutionCodeBlockBuilder(AotQueryMethodGenerationContext context, SimpleTypeHolder simpleTypeHolder,
				MongoQueryMethod queryMethod) {

			this.context = context;
			this.simpleTypeHolder = simpleTypeHolder;
			this.queryMethod = queryMethod;
		}

		AggregationExecutionCodeBlockBuilder referencing(String aggregationVariableName) {

			this.aggregationVariableName = aggregationVariableName;
			return this;
		}

		CodeBlock build() {

			String mongoOpsRef = context.fieldNameOf(MongoOperations.class);
			Builder builder = CodeBlock.builder();

			builder.add("\n");

			Class<?> outputType = getOutputType(simpleTypeHolder, queryMethod);

			if (ReflectionUtils.isVoid(queryMethod.getReturnedObjectType())) {
				builder.addStatement("$L.aggregate($L, $T.class)", mongoOpsRef, aggregationVariableName, outputType);
				return builder.build();
			}

			if (ClassUtils.isAssignable(AggregationResults.class, context.getMethod().getReturnType())) {
				builder.addStatement("return $L.aggregate($L, $T.class)", mongoOpsRef, aggregationVariableName, outputType);
				return builder.build();
			}

			if (outputType == Document.class) {

				Class<?> returnType = ClassUtils.resolvePrimitiveIfNecessary(queryMethod.getReturnedObjectType());

				if (queryMethod.isStreamQuery()) {

					VariableSnippet results = Snippet.declare(builder)
							.variable(ResolvableType.forClassWithGenerics(Stream.class, Document.class),
									context.localVariable("results"))
							.as("$L.aggregateStream($L, $T.class)", mongoOpsRef, aggregationVariableName, outputType);

					builder.addStatement("return $1L.map(it -> ($2T) convertSimpleRawResult($2T.class, it))",
							results.getVariableName(), returnType);
				} else {

					VariableSnippet results = Snippet.declare(builder)
							.variable(AggregationResults.class, context.localVariable("results"))
							.as("$L.aggregate($L, $T.class)", mongoOpsRef, aggregationVariableName, outputType);

					if (!queryMethod.isCollectionQuery()) {
						builder.addStatement(
								"return $1T.<$2T>firstElement(convertSimpleRawResults($2T.class, $3L.getMappedResults()))",
								CollectionUtils.class, returnType, results.getVariableName());
					} else {
						builder.addStatement("return convertSimpleRawResults($T.class, $L.getMappedResults())", returnType,
								results.getVariableName());
					}
				}
			} else {
				if (queryMethod.isSliceQuery()) {

					VariableSnippet results = Snippet.declare(builder)
							.variable(AggregationResults.class, context.localVariable("results"))
							.as("$L.aggregate($L, $T.class)", mongoOpsRef, aggregationVariableName, outputType);

					VariableSnippet hasNext = Snippet.declare(builder).variable("hasNext").as(
							"$L.getMappedResults().size() > $L.getPageSize()", results.getVariableName(),
							context.getPageableParameterName());

					builder.addStatement(
							"return new $1T<>($2L ? $3L.getMappedResults().subList(0, $4L.getPageSize()) : $3L.getMappedResults(), $4L, $2L)",
							SliceImpl.class, hasNext.getVariableName(), results.getVariableName(),
							context.getPageableParameterName());
				} else {

					if (queryMethod.isStreamQuery()) {
						builder.addStatement("return $L.aggregateStream($L, $T.class)", mongoOpsRef, aggregationVariableName,
								outputType);
					} else {
						CodeBlock codeBlock = CodeBlock.of("$L.aggregate($L, $T.class).getMappedResults()", mongoOpsRef,
								aggregationVariableName, outputType);

						builder.addStatement("return $L",
								MongoCodeBlocks.potentiallyWrapStreamable(context.getMethodReturn(), codeBlock));
					}
				}
			}

			return builder.build();
		}

	}

	private static Class<?> getOutputType(SimpleTypeHolder simpleTypeHolder, MongoQueryMethod queryMethod) {

		Class<?> outputType = queryMethod.getReturnedObjectType();

		if (simpleTypeHolder.isSimpleType(outputType)) {
			return Document.class;
		}

		if (ClassUtils.isAssignable(AggregationResults.class, outputType)
				&& queryMethod.getReturnType().getComponentType() != null) {
			return queryMethod.getReturnType().getComponentType().getType();
		}

		return outputType;
	}

	@NullUnmarked
	static class AggregationCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final SimpleTypeHolder simpleTypeHolder;
		private final MongoQueryMethod queryMethod;
		private final String parameterNames;

		private AggregationInteraction source;

		private String aggregationVariableName;
		private boolean pipelineOnly;

		AggregationCodeBlockBuilder(AotQueryMethodGenerationContext context, SimpleTypeHolder simpleTypeHolder,
				MongoQueryMethod queryMethod) {

			this.context = context;
			this.simpleTypeHolder = simpleTypeHolder;
			this.queryMethod = queryMethod;
			this.parameterNames = StringUtils.collectionToDelimitedString(context.getAllParameterNames(), ", ");
		}

		AggregationCodeBlockBuilder stages(AggregationInteraction aggregation) {

			this.source = aggregation;
			return this;
		}

		AggregationCodeBlockBuilder usingAggregationVariableName(String aggregationVariableName) {

			this.aggregationVariableName = aggregationVariableName;
			return this;
		}

		AggregationCodeBlockBuilder pipelineOnly(boolean pipelineOnly) {

			this.pipelineOnly = pipelineOnly;
			return this;
		}

		CodeBlock build() {

			Builder builder = CodeBlock.builder();
			builder.add("\n");

			String pipelineName = context.localVariable(aggregationVariableName + (pipelineOnly ? "" : "Pipeline"));
			builder.add(pipeline(pipelineName));

			if (!pipelineOnly) {

				Class<?> domainType = context.getRepositoryInformation().getDomainType();
				Snippet.declare(builder)
						.variable(ResolvableType.forClassWithGenerics(TypedAggregation.class, domainType), aggregationVariableName)
						.as("$T.newAggregation($T.class, $L.getOperations())", Aggregation.class, domainType, pipelineName);

				builder.add(aggregationOptions(aggregationVariableName));
			}

			return builder.build();
		}

		private CodeBlock pipeline(String pipelineVariableName) {

			String sortParameter = context.getSortParameterName();
			String limitParameter = context.getLimitParameterName();
			String pageableParameter = context.getPageableParameterName();

			Builder builder = CodeBlock.builder();
			builder.add(aggregationStages(context.localVariable("stages"), source.stages()));

			if (StringUtils.hasText(sortParameter)) {
				Class<?> outputType = getOutputType(simpleTypeHolder, queryMethod);
				builder.add(sortingStage(sortParameter, outputType));
			}

			if (StringUtils.hasText(limitParameter)) {
				builder.add(limitingStage(limitParameter));
			}

			if (StringUtils.hasText(pageableParameter)) {
				builder.add(pagingStage(pageableParameter, queryMethod.isSliceQuery()));
			}

			builder.addStatement("$T $L = createPipeline($L)", AggregationPipeline.class, pipelineVariableName,
					context.localVariable("stages"));

			return builder.build();
		}

		private CodeBlock aggregationOptions(String aggregationVariableName) {

			Builder builder = CodeBlock.builder();
			List<CodeBlock> options = new ArrayList<>(5);

			if (ReflectionUtils.isVoid(queryMethod.getReturnedObjectType())) {
				options.add(CodeBlock.of(".skipOutput()"));
			}

			MergedAnnotation<Hint> hintAnnotation = context.getAnnotation(Hint.class);
			String hint = hintAnnotation.isPresent() ? hintAnnotation.getString("value") : null;
			if (StringUtils.hasText(hint)) {
				options.add(CodeBlock.of(".hint($S)", hint));
			}

			MergedAnnotation<ReadPreference> readPreferenceAnnotation = context.getAnnotation(ReadPreference.class);
			String readPreference = readPreferenceAnnotation.isPresent() ? readPreferenceAnnotation.getString("value") : null;
			if (StringUtils.hasText(readPreference)) {
				options.add(CodeBlock.of(".readPreference($T.valueOf($S))", com.mongodb.ReadPreference.class, readPreference));
			}

			if (queryMethod.hasAnnotatedCollation()) {
				options.add(CodeBlock.of(".collation($T.parse($S))", Collation.class, queryMethod.getAnnotatedCollation()));
			}

			if (!options.isEmpty()) {

				Builder optionsBuilder = CodeBlock.builder();
				optionsBuilder.add("$1T $2L = $1T.builder()\n", AggregationOptions.class,
						context.localVariable("aggregationOptions"));
				optionsBuilder.indent();
				for (CodeBlock optionBlock : options) {
					optionsBuilder.add(optionBlock);
					optionsBuilder.add("\n");
				}
				optionsBuilder.add(".build();\n");
				optionsBuilder.unindent();
				builder.add(optionsBuilder.build());

				builder.addStatement("$1L = $1L.withOptions($2L)", aggregationVariableName,
						context.localVariable("aggregationOptions"));
			}
			return builder.build();
		}

		private CodeBlock aggregationStages(String stageListVariableName, Collection<String> stages) {

			Builder builder = CodeBlock.builder();
			builder.addStatement("$T<$T> $L = new $T($L)", List.class, Object.class, stageListVariableName, ArrayList.class,
					stages.size());
			int stageCounter = 0;

			for (String stage : stages) {

				VariableSnippet stageSnippet = Snippet.declare(builder)
						.variable(Document.class, context.localVariable("stage_%s".formatted(stageCounter)))
						.of(MongoCodeBlocks.asDocument(context.getExpressionMarker(), stage, parameterNames));
				builder.addStatement("$L.add($L)", stageListVariableName, stageSnippet.getVariableName());

				stageCounter++;
			}

			return builder.build();
		}

		private CodeBlock sortingStage(String sortProvider, Class<?> outputType) {

			Builder builder = CodeBlock.builder();

			builder.beginControlFlow("if ($L.isSorted())", sortProvider);
			builder.addStatement("$1T $2L = new $1T()", Document.class, context.localVariable("sortDocument"));
			builder.beginControlFlow("for ($T $L : $L)", Order.class, context.localVariable("order"), sortProvider);
			builder.addStatement("$1L.append($2L.getProperty(), $2L.isAscending() ? 1 : -1);",
					context.localVariable("sortDocument"), context.localVariable("order"));
			builder.endControlFlow();

			if (outputType == Document.class || simpleTypeHolder.isSimpleType(outputType)
					|| ClassUtils.isAssignable(context.getRepositoryInformation().getDomainType(), outputType)) {
				builder.addStatement("$L.add(new $T($S, $L))", context.localVariable("stages"), Document.class, "$sort",
						context.localVariable("sortDocument"));
			} else {
				builder.addStatement("$L.add(($T) _ctx -> new $T($S, _ctx.getMappedObject($L, $T.class)))",
						context.localVariable("stages"), AggregationOperation.class, Document.class, "$sort",
						context.localVariable("sortDocument"), outputType);
			}

			builder.endControlFlow();

			return builder.build();
		}

		private CodeBlock pagingStage(String pageableProvider, boolean slice) {

			Builder builder = CodeBlock.builder();

			builder.add(sortingStage(pageableProvider + ".getSort()", getOutputType(simpleTypeHolder, queryMethod)));

			builder.beginControlFlow("if ($L.isPaged())", pageableProvider);
			builder.beginControlFlow("if ($L.getOffset() > 0)", pageableProvider);
			builder.addStatement("$L.add($T.skip($L.getOffset()))", context.localVariable("stages"), Aggregation.class,
					pageableProvider);
			builder.endControlFlow();
			if (slice) {
				builder.addStatement("$L.add($T.limit($L.getPageSize() + 1))", context.localVariable("stages"),
						Aggregation.class, pageableProvider);
			} else {
				builder.addStatement("$L.add($T.limit($L.getPageSize()))", context.localVariable("stages"), Aggregation.class,
						pageableProvider);
			}
			builder.endControlFlow();

			return builder.build();
		}

		private CodeBlock limitingStage(String limitProvider) {

			Builder builder = CodeBlock.builder();

			builder.beginControlFlow("if ($L.isLimited())", limitProvider);
			builder.addStatement("$L.add($T.limit($L.max()))", context.localVariable("stages"), Aggregation.class,
					limitProvider);
			builder.endControlFlow();

			return builder.build();
		}

	}
}
