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
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.bson.Document;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.ExecutableRemoveOperation.ExecutableRemove;
import org.springframework.data.mongodb.core.ExecutableUpdateOperation.ExecutableUpdate;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.repository.Hint;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.DeleteExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.SlicedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * {@link CodeBlock} generator for common tasks.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
class MongoCodeBlocks {

	private static final Pattern PARAMETER_BINDING_PATTERN = Pattern.compile("\\?(\\d+)");

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

	@NullUnmarked
	static class DeleteExecutionCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private String queryVariableName;

		DeleteExecutionCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;
		}

		DeleteExecutionCodeBlockBuilder referencing(String queryVariableName) {

			this.queryVariableName = queryVariableName;
			return this;
		}

		CodeBlock build() {

			String mongoOpsRef = context.fieldNameOf(MongoOperations.class);
			Builder builder = CodeBlock.builder();

			Class<?> domainType = context.getRepositoryInformation().getDomainType();
			boolean isProjecting = context.getActualReturnType() != null
					&& !ObjectUtils.nullSafeEquals(TypeName.get(domainType), context.getActualReturnType());

			Object actualReturnType = isProjecting ? context.getActualReturnType().getType() : domainType;

			builder.add("\n");
			builder.addStatement("$T<$T> $L = $L.remove($T.class)", ExecutableRemove.class, domainType,
					context.localVariable("remover"), mongoOpsRef, domainType);

			DeleteExecution.Type type = DeleteExecution.Type.FIND_AND_REMOVE_ALL;
			if (!queryMethod.isCollectionQuery()) {
				if (!ClassUtils.isPrimitiveOrWrapper(context.getMethod().getReturnType())) {
					type = DeleteExecution.Type.FIND_AND_REMOVE_ONE;
				} else {
					type = DeleteExecution.Type.ALL;
				}
			}

			actualReturnType = ClassUtils.isPrimitiveOrWrapper(context.getMethod().getReturnType())
					? TypeName.get(context.getMethod().getReturnType())
					: queryMethod.isCollectionQuery() ? context.getReturnTypeName() : actualReturnType;

			if (ClassUtils.isVoidType(context.getMethod().getReturnType())) {
				builder.addStatement("new $T($L, $T.$L).execute($L)", DeleteExecution.class, context.localVariable("remover"),
						DeleteExecution.Type.class, type.name(), queryVariableName);
			} else if (context.getMethod().getReturnType() == Optional.class) {
				builder.addStatement("return $T.ofNullable(($T) new $T($L, $T.$L).execute($L))", Optional.class,
						actualReturnType, DeleteExecution.class, context.localVariable("remover"), DeleteExecution.Type.class,
						type.name(), queryVariableName);
			} else {
				builder.addStatement("return ($T) new $T($L, $T.$L).execute($L)", actualReturnType, DeleteExecution.class,
						context.localVariable("remover"), DeleteExecution.Type.class, type.name(), queryVariableName);
			}

			return builder.build();
		}
	}

	@NullUnmarked
	static class UpdateExecutionCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private String queryVariableName;
		private String updateVariableName;

		UpdateExecutionCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;
		}

		UpdateExecutionCodeBlockBuilder withFilter(String queryVariableName) {

			this.queryVariableName = queryVariableName;
			return this;
		}

		UpdateExecutionCodeBlockBuilder referencingUpdate(String updateVariableName) {

			this.updateVariableName = updateVariableName;
			return this;
		}

		CodeBlock build() {

			String mongoOpsRef = context.fieldNameOf(MongoOperations.class);
			Builder builder = CodeBlock.builder();

			builder.add("\n");

			String updateReference = updateVariableName;
			Class<?> domainType = context.getRepositoryInformation().getDomainType();
			builder.addStatement("$T<$T> $L = $L.update($T.class)", ExecutableUpdate.class, domainType,
					context.localVariable("updater"), mongoOpsRef, domainType);

			Class<?> returnType = ClassUtils.resolvePrimitiveIfNecessary(queryMethod.getReturnedObjectType());
			if (ReflectionUtils.isVoid(returnType)) {
				builder.addStatement("$L.matching($L).apply($L).all()", context.localVariable("updater"), queryVariableName,
						updateReference);
			} else if (ClassUtils.isAssignable(Long.class, returnType)) {
				builder.addStatement("return $L.matching($L).apply($L).all().getModifiedCount()",
						context.localVariable("updater"), queryVariableName, updateReference);
			} else {
				builder.addStatement("$T $L = $L.matching($L).apply($L).all().getModifiedCount()", Long.class,
						context.localVariable("modifiedCount"), context.localVariable("updater"), queryVariableName,
						updateReference);
				builder.addStatement("return $T.convertNumberToTargetClass($L, $T.class)", NumberUtils.class,
						context.localVariable("modifiedCount"), returnType);
			}

			return builder.build();
		}
	}

	@NullUnmarked
	static class AggregationExecutionCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private String aggregationVariableName;

		AggregationExecutionCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;
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

			Class<?> outputType = queryMethod.getReturnedObjectType();
			if (MongoSimpleTypes.HOLDER.isSimpleType(outputType)) {
				outputType = Document.class;
			} else if (ClassUtils.isAssignable(AggregationResults.class, outputType)) {
				outputType = queryMethod.getReturnType().getComponentType().getType();
			}

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

					builder.addStatement("$T<$T> $L = $L.aggregateStream($L, $T.class)", Stream.class, Document.class,
							context.localVariable("results"), mongoOpsRef, aggregationVariableName, outputType);

					builder.addStatement("return $L.map(it -> ($T) convertSimpleRawResult($T.class, it))",
							context.localVariable("results"), returnType, returnType);
				} else {

					builder.addStatement("$T $L = $L.aggregate($L, $T.class)", AggregationResults.class,
							context.localVariable("results"), mongoOpsRef, aggregationVariableName, outputType);

					if (!queryMethod.isCollectionQuery()) {
						builder.addStatement("return $T.<$T>firstElement(convertSimpleRawResults($T.class, $L.getMappedResults()))",
								CollectionUtils.class, returnType, returnType, context.localVariable("results"));
					} else {
						builder.addStatement("return convertSimpleRawResults($T.class, $L.getMappedResults())", returnType,
								context.localVariable("results"));
					}
				}
			} else {
				if (queryMethod.isSliceQuery()) {
					builder.addStatement("$T $L = $L.aggregate($L, $T.class)", AggregationResults.class,
							context.localVariable("results"), mongoOpsRef, aggregationVariableName, outputType);
					builder.addStatement("boolean $L = $L.getMappedResults().size() > $L.getPageSize()",
							context.localVariable("hasNext"), context.localVariable("results"), context.getPageableParameterName());
					builder.addStatement(
							"return new $T<>($L ? $L.getMappedResults().subList(0, $L.getPageSize()) : $L.getMappedResults(), $L, $L)",
							SliceImpl.class, context.localVariable("hasNext"), context.localVariable("results"),
							context.getPageableParameterName(), context.localVariable("results"), context.getPageableParameterName(),
							context.localVariable("hasNext"));
				} else {

					if (queryMethod.isStreamQuery()) {
						builder.addStatement("return $L.aggregateStream($L, $T.class)", mongoOpsRef, aggregationVariableName,
								outputType);
					} else {

						builder.addStatement("return $L.aggregate($L, $T.class).getMappedResults()", mongoOpsRef,
								aggregationVariableName, outputType);
					}
				}
			}

			return builder.build();
		}
	}

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
	static class AggregationCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;

		private AggregationInteraction source;
		private final List<String> arguments;
		private String aggregationVariableName;
		private boolean pipelineOnly;

		AggregationCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;
			this.arguments = context.getBindableParameterNames();
			this.queryMethod = queryMethod;
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

			CodeBlock.Builder builder = CodeBlock.builder();
			builder.add("\n");

			String pipelineName = context.localVariable(aggregationVariableName + (pipelineOnly ? "" : "Pipeline"));
			builder.add(pipeline(pipelineName));

			if (!pipelineOnly) {

				builder.addStatement("$T<$T> $L = $T.newAggregation($T.class, $L.getOperations())", TypedAggregation.class,
						context.getRepositoryInformation().getDomainType(), aggregationVariableName, Aggregation.class,
						context.getRepositoryInformation().getDomainType(), pipelineName);

				builder.add(aggregationOptions(aggregationVariableName));
			}

			return builder.build();
		}

		private CodeBlock pipeline(String pipelineVariableName) {

			String sortParameter = context.getSortParameterName();
			String limitParameter = context.getLimitParameterName();
			String pageableParameter = context.getPageableParameterName();

			boolean mightBeSorted = StringUtils.hasText(sortParameter);
			boolean mightBeLimited = StringUtils.hasText(limitParameter);
			boolean mightBePaged = StringUtils.hasText(pageableParameter);

			int stageCount = source.stages().size();
			if (mightBeSorted) {
				stageCount++;
			}
			if (mightBeLimited) {
				stageCount++;
			}
			if (mightBePaged) {
				stageCount += 3;
			}

			Builder builder = CodeBlock.builder();
			builder.add(aggregationStages(context.localVariable("stages"), source.stages(), stageCount, arguments));

			if (mightBeSorted) {
				builder.add(sortingStage(sortParameter));
			}

			if (mightBeLimited) {
				builder.add(limitingStage(limitParameter));
			}

			if (mightBePaged) {
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
				optionsBuilder.add("$T $L = $T.builder()\n", AggregationOptions.class,
						context.localVariable("aggregationOptions"), AggregationOptions.class);
				optionsBuilder.indent();
				for (CodeBlock optionBlock : options) {
					optionsBuilder.add(optionBlock);
					optionsBuilder.add("\n");
				}
				optionsBuilder.add(".build();\n");
				optionsBuilder.unindent();
				builder.add(optionsBuilder.build());

				builder.addStatement("$L = $L.withOptions($L)", aggregationVariableName, aggregationVariableName,
						context.localVariable("aggregationOptions"));
			}
			return builder.build();
		}

		private CodeBlock aggregationStages(String stageListVariableName, Iterable<String> stages, int stageCount,
				List<String> arguments) {

			Builder builder = CodeBlock.builder();
			builder.addStatement("$T<$T> $L = new $T($L)", List.class, Object.class, stageListVariableName, ArrayList.class,
					stageCount);
			int stageCounter = 0;

			for (String stage : stages) {
				String stageName = context.localVariable("stage_%s".formatted(stageCounter++));
				builder.add(renderExpressionToDocument(stage, stageName, arguments));
				builder.addStatement("$L.add($L)", context.localVariable("stages"), stageName);
			}

			return builder.build();
		}

		private CodeBlock sortingStage(String sortProvider) {

			Builder builder = CodeBlock.builder();

			builder.beginControlFlow("if ($L.isSorted())", sortProvider);
			builder.addStatement("$T $L = new $T()", Document.class, context.localVariable("sortDocument"), Document.class);
			builder.beginControlFlow("for ($T $L : $L)", Order.class, context.localVariable("order"), sortProvider);
			builder.addStatement("$L.append($L.getProperty(), $L.isAscending() ? 1 : -1);",
					context.localVariable("sortDocument"), context.localVariable("order"), context.localVariable("order"));
			builder.endControlFlow();
			builder.addStatement("stages.add(new $T($S, $L))", Document.class, "$sort",
					context.localVariable("sortDocument"));
			builder.endControlFlow();

			return builder.build();
		}

		private CodeBlock pagingStage(String pageableProvider, boolean slice) {

			Builder builder = CodeBlock.builder();

			builder.add(sortingStage(pageableProvider + ".getSort()"));

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

	@NullUnmarked
	static class QueryCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;

		private QueryInteraction source;
		private final List<String> arguments;
		private String queryVariableName;

		QueryCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {

			this.context = context;
			this.arguments = context.getBindableParameterNames();
			this.queryMethod = queryMethod;
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

			CodeBlock.Builder builder = CodeBlock.builder();

			builder.add("\n");
			builder.add(renderExpressionToQuery(source.getQuery().getQueryString(), queryVariableName));

			if (StringUtils.hasText(source.getQuery().getFieldsString())) {

				builder.add(renderExpressionToDocument(source.getQuery().getFieldsString(), "fields", arguments));
				builder.addStatement("$L.setFieldsObject(fields)", queryVariableName);
			}

			String sortParameter = context.getSortParameterName();
			if (StringUtils.hasText(sortParameter)) {
				builder.addStatement("$L.with($L)", queryVariableName, sortParameter);
			} else if (StringUtils.hasText(source.getQuery().getSortString())) {

				builder.add(renderExpressionToDocument(source.getQuery().getSortString(), "sort", arguments));
				builder.addStatement("$L.setSortObject(sort)", queryVariableName);
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

			MergedAnnotation<ReadPreference> readPreferenceAnnotation = context.getAnnotation(ReadPreference.class);
			String readPreference = readPreferenceAnnotation.isPresent() ? readPreferenceAnnotation.getString("value") : null;

			if (StringUtils.hasText(readPreference)) {
				builder.addStatement("$L.withReadPreference($T.valueOf($S))", queryVariableName,
						com.mongodb.ReadPreference.class, readPreference);
			}

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
				if (StringUtils.hasText("comment")) {
					builder.addStatement("$L.comment($S)", queryVariableName, comment);
				}
			}

			// TODO: Meta annotation: Disk usage

			return builder.build();
		}

		private CodeBlock renderExpressionToQuery(@Nullable String source, String variableName) {

			Builder builder = CodeBlock.builder();
			if (!StringUtils.hasText(source)) {

				builder.addStatement("$T $L = new $T(new $T())", BasicQuery.class, variableName, BasicQuery.class,
						Document.class);
			} else if (!containsPlaceholder(source)) {
				builder.addStatement("$T $L = new $T($T.parse($S))", BasicQuery.class, variableName, BasicQuery.class,
						Document.class, source);
			} else {
				builder.addStatement("$T $L = createQuery($S, new $T[]{ $L })", BasicQuery.class, variableName, source,
						Object.class, StringUtils.collectionToDelimitedString(arguments, ", "));
			}

			return builder.build();
		}
	}

	@NullUnmarked
	static class UpdateCodeBlockBuilder {

		private UpdateInteraction source;
		private List<String> arguments;
		private String updateVariableName;

		public UpdateCodeBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
			this.arguments = context.getBindableParameterNames();
		}

		public UpdateCodeBlockBuilder update(UpdateInteraction update) {
			this.source = update;
			return this;
		}

		public UpdateCodeBlockBuilder usingUpdateVariableName(String updateVariableName) {
			this.updateVariableName = updateVariableName;
			return this;
		}

		CodeBlock build() {

			CodeBlock.Builder builder = CodeBlock.builder();

			builder.add("\n");
			String tmpVariableName = updateVariableName + "Document";
			builder.add(renderExpressionToDocument(source.getUpdate().getUpdateString(), tmpVariableName, arguments));
			builder.addStatement("$T $L = new $T($L)", BasicUpdate.class, updateVariableName, BasicUpdate.class,
					tmpVariableName);

			return builder.build();
		}
	}

	private static CodeBlock renderExpressionToDocument(@Nullable String source, String variableName,
			List<String> arguments) {

		Builder builder = CodeBlock.builder();
		if (!StringUtils.hasText(source)) {
			builder.addStatement("$T $L = new $T()", Document.class, variableName, Document.class);
		} else if (!containsPlaceholder(source)) {
			builder.addStatement("$T $L = $T.parse($S)", Document.class, variableName, Document.class, source);
		} else {
			builder.addStatement("$T $L = bindParameters($S, new $T[]{ $L })", Document.class, variableName, source,
					Object.class, StringUtils.collectionToDelimitedString(arguments, ", "));
		}
		return builder.build();
	}

	private static boolean containsPlaceholder(String source) {
		return PARAMETER_BINDING_PATTERN.matcher(source).find();
	}
}
