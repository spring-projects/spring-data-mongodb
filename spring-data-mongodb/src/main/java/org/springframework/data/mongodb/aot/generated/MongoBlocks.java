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
package org.springframework.data.mongodb.aot.generated;

import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.bson.Document;
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
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.DeleteExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.SlicedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.javapoet.ClassName;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public class MongoBlocks {

	private static final Pattern PARAMETER_BINDING_PATTERN = Pattern.compile("\\?(\\d+)");

	static QueryBlockBuilder queryBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
		return new QueryBlockBuilder(context, queryMethod);
	}

	static UpdateBlockBuilder updateBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
		return new UpdateBlockBuilder(context, queryMethod);
	}

	static AggregationBlockBuilder aggregationBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {
		return new AggregationBlockBuilder(context, queryMethod);
	}

	static QueryExecutionBlockBuilder queryExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {
		return new QueryExecutionBlockBuilder(context, queryMethod);
	}

	static DeleteExecutionBuilder deleteExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {
		return new DeleteExecutionBuilder(context, queryMethod);
	}

	static UpdateExecutionBuilder updateExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {
		return new UpdateExecutionBuilder(context, queryMethod);
	}

	static AggregationExecutionBuilder aggregationExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {
		return new AggregationExecutionBuilder(context, queryMethod);
	}

	static class DeleteExecutionBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private String queryVariableName;

		public DeleteExecutionBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
			this.context = context;
			this.queryMethod = queryMethod;
		}

		public DeleteExecutionBuilder referencing(String queryVariableName) {
			this.queryVariableName = queryVariableName;
			return this;
		}

		public CodeBlock build() {

			String mongoOpsRef = context.fieldNameOf(MongoOperations.class);
			Builder builder = CodeBlock.builder();

			boolean isProjecting = context.getActualReturnType() != null
					&& !ObjectUtils.nullSafeEquals(TypeName.get(context.getRepositoryInformation().getDomainType()),
							context.getActualReturnType());

			Object actualReturnType = isProjecting ? context.getActualReturnType().getType()
					: context.getRepositoryInformation().getDomainType();

			builder.add("\n");
			builder.addStatement("$T<$T> remover = $L.remove($T.class)", ExecutableRemove.class,
					context.getRepositoryInformation().getDomainType(), mongoOpsRef,
					context.getRepositoryInformation().getDomainType());

			DeleteExecution.Type type = DeleteExecution.Type.FIND_AND_REMOVE_ALL;
			if (!queryMethod.isCollectionQuery()) {
				if (!ClassUtils.isPrimitiveOrWrapper(context.getMethod().getReturnType())) {
					type = DeleteExecution.Type.FIND_AND_REMOVE_ONE;
				} else {
					type = DeleteExecution.Type.ALL;
				}
			}

			actualReturnType = ClassUtils.isPrimitiveOrWrapper(context.getMethod().getReturnType())
					? ClassName.get(context.getMethod().getReturnType())
					: queryMethod.isCollectionQuery() ? context.getReturnTypeName() : actualReturnType;

			builder.addStatement("return ($T) new $T(remover, $T.$L).execute($L)", actualReturnType, DeleteExecution.class,
					DeleteExecution.Type.class, type.name(), queryVariableName);

			return builder.build();
		}
	}

	static class UpdateExecutionBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private String queryVariableName;
		private String updateVariableName;

		public UpdateExecutionBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
			this.context = context;
			this.queryMethod = queryMethod;
		}

		public UpdateExecutionBuilder withFilter(String queryVariableName) {
			this.queryVariableName = queryVariableName;
			return this;
		}

		public UpdateExecutionBuilder referencingUpdate(String updateVariableName) {
			this.updateVariableName = updateVariableName;
			return this;
		}

		public CodeBlock build() {

			String mongoOpsRef = context.fieldNameOf(MongoOperations.class);
			Builder builder = CodeBlock.builder();

			builder.add("\n");
			builder.addStatement("$T<$T> updater = $L.update($T.class)", ExecutableUpdate.class,
					context.getRepositoryInformation().getDomainType(), mongoOpsRef,
					context.getRepositoryInformation().getDomainType());

			Class<?> returnType = ClassUtils.resolvePrimitiveIfNecessary(queryMethod.getReturnedObjectType());
			if (ClassUtils.isAssignable(Long.class, returnType)) {
				builder.addStatement("return updater.matching($L).apply($L).all().getModifiedCount()", queryVariableName,
						updateVariableName);
			} else {
				builder.addStatement("$T modifiedCount = updater.matching($L).apply($L).all().getModifiedCount()", Long.class,
						queryVariableName, updateVariableName);
				builder.addStatement("return $T.convertNumberToTargetClass(modifiedCount, $T.class)", NumberUtils.class,
						returnType);
			}

			return builder.build();
		}
	}

	static class AggregationExecutionBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private String aggregationVariableName;

		public AggregationExecutionBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
			this.context = context;
			this.queryMethod = queryMethod;
		}

		public AggregationExecutionBuilder referencing(String aggregationVariableName) {
			this.aggregationVariableName = aggregationVariableName;
			return this;
		}

		public CodeBlock build() {

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

				builder.addStatement("$T results = $L.aggregate($L, $T.class)", AggregationResults.class, mongoOpsRef,
						aggregationVariableName, outputType);
				if (!queryMethod.isCollectionQuery()) {
					builder.addStatement(
							"return $T.<$T>firstElement(convertSimpleRawResults($T.class, results.getMappedResults()))",
							CollectionUtils.class, returnType, returnType);
				} else {
					builder.addStatement("return convertSimpleRawResults($T.class, results.getMappedResults())", returnType);
				}
			} else {
				if (queryMethod.isSliceQuery()) {
					builder.addStatement("$T results = $L.aggregate($L, $T.class)", AggregationResults.class, mongoOpsRef,
							aggregationVariableName, outputType);
					builder.addStatement("boolean hasNext = results.getMappedResults().size() > $L.getPageSize()",
							context.getPageableParameterName());
					builder.addStatement(
							"return new $T<>(hasNext ? results.getMappedResults().subList(0, $L.getPageSize()) : results.getMappedResults(), $L, hasNext)",
							SliceImpl.class, context.getPageableParameterName(), context.getPageableParameterName());
				} else {
					builder.addStatement("return $L.aggregate($L, $T.class).getMappedResults()", mongoOpsRef,
							aggregationVariableName, outputType);
				}
			}

			return builder.build();
		}
	}

	static class QueryExecutionBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private StringAotQuery query;

		public QueryExecutionBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
			this.context = context;
			this.queryMethod = queryMethod;
		}

		CodeBlock build() {

			String mongoOpsRef = context.fieldNameOf(MongoOperations.class);

			Builder builder = CodeBlock.builder();

			boolean isProjecting = context.getReturnedType().isProjecting();
			Object actualReturnType = isProjecting ? context.getActualReturnType().getType()
					: context.getRepositoryInformation().getDomainType();

			builder.add("\n");

			if (isProjecting) {
				builder.addStatement("$T<$T> finder = $L.query($T.class).as($T.class)", FindWithQuery.class, actualReturnType,
						mongoOpsRef, context.getRepositoryInformation().getDomainType(), actualReturnType);
			} else {

				builder.addStatement("$T<$T> finder = $L.query($T.class)", FindWithQuery.class, actualReturnType, mongoOpsRef,
						context.getRepositoryInformation().getDomainType());
			}

			String terminatingMethod;

			if (queryMethod.isCollectionQuery() || queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
				terminatingMethod = "all()";
			} else if (query.isCountQuery()) {
				terminatingMethod = "count()";
			} else if (query.isExists()) {
				terminatingMethod = "exists()";
			} else {
				terminatingMethod = Optional.class.isAssignableFrom(context.getReturnType().toClass()) ? "one()" : "oneValue()";
			}

			if (queryMethod.isPageQuery()) {
				builder.addStatement("return new $T(finder, $L).execute($L)", PagedExecution.class,
						context.getPageableParameterName(), query.name());
			} else if (queryMethod.isSliceQuery()) {
				builder.addStatement("return new $T(finder, $L).execute($L)", SlicedExecution.class,
						context.getPageableParameterName(), query.name());
			} else {
				builder.addStatement("return finder.matching($L).$L", query.name(), terminatingMethod);
			}

			return builder.build();
		}

		public QueryExecutionBlockBuilder forQuery(StringAotQuery query) {
			this.query = query;
			return this;
		}
	}

	static class AggregationBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;

		private StringAotAggregation source;
		private List<String> arguments;
		private String aggregationVariableName;

		public AggregationBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
			this.context = context;
			this.arguments = Arrays.stream(context.getMethod().getParameters()).map(Parameter::getName)
					.collect(Collectors.toList());

			// ParametersSource parametersSource = ParametersSource.of(repositoryInformation, metadata.getRepositoryMethod());
			// this.argumentSource = new MongoParameters(parametersSource, false);

			this.queryMethod = queryMethod;
		}

		public AggregationBlockBuilder stages(StringAotAggregation aggregation) {
			this.source = aggregation;
			return this;
		}

		public AggregationBlockBuilder usingAggregationVariableName(String aggregationVariableName) {
			this.aggregationVariableName = aggregationVariableName;
			return this;
		}

		CodeBlock build() {

			CodeBlock.Builder builder = CodeBlock.builder();
			builder.add("\n");

			builder.addStatement("$T<$T> stages = new $T()", List.class, Object.class, ArrayList.class);
			int stageCounter = 0;
			for (String stage : source.stages()) {
				String stageName = "stage_%s".formatted(stageCounter++);
				builder.add(renderExpressionToDocument(stage, stageName, arguments));
				builder.addStatement("stages.add($L)", stageName);
			}

			{
				String sortParameter = context.getSortParameterName();
				if (StringUtils.hasText(sortParameter)) {

					builder.beginControlFlow("if($L.isSorted())", sortParameter);
					builder.addStatement("$T sortDocument = new $T()", Document.class, Document.class);
					builder.beginControlFlow("for ($T order : $L)", Order.class, sortParameter);
					builder.addStatement("sortDocument.append(order.getProperty(), order.isAscending() ? 1 : -1);");
					builder.endControlFlow();
					builder.addStatement("stages.add(new $T($S, sortDocument))", Document.class, "$sort");
					builder.endControlFlow();
				}

				String limitParameter = context.getLimitParameterName();
				if (StringUtils.hasText(limitParameter)) {
					builder.beginControlFlow("if($L.isLimited())", limitParameter);
					builder.addStatement("stages.add($T.limit($L.max()))", Aggregation.class, limitParameter);
					builder.endControlFlow();
				}

				String pageableParameter = context.getPageableParameterName();
				if (StringUtils.hasText(pageableParameter)) {

					builder.beginControlFlow("if($L.getSort().isSorted())", pageableParameter);
					builder.addStatement("$T sortDocument = new $T()", Document.class, Document.class);
					builder.beginControlFlow("for ($T order : $L.getSort())", Order.class, pageableParameter);
					builder.addStatement("sortDocument.append(order.getProperty(), order.isAscending() ? 1 : -1);");
					builder.endControlFlow();
					builder.addStatement("stages.add(new $T($S, sortDocument))", Document.class, "$sort");
					builder.endControlFlow();

					builder.beginControlFlow("if($L.isPaged())", pageableParameter);
					builder.beginControlFlow("if($L.getOffset() > 0)", pageableParameter);
					builder.addStatement("stages.add($T.skip($L.getOffset()))", Aggregation.class, pageableParameter);
					builder.endControlFlow();
					if (queryMethod.isSliceQuery()) {
						builder.addStatement("stages.add($T.limit($L.getPageSize() + 1))", Aggregation.class, pageableParameter);
					} else {
						builder.addStatement("stages.add($T.limit($L.getPageSize()))", Aggregation.class, pageableParameter);
					}
					builder.endControlFlow();
				}

			}

			String pipelineName = aggregationVariableName + "Pipeline";
			builder.addStatement("$T $L = createPipeline(stages)", AggregationPipeline.class, pipelineName);

			builder.addStatement("$T<$T> $L = $T.newAggregation($T.class, $L.getOperations())", TypedAggregation.class,
					context.getRepositoryInformation().getDomainType(), aggregationVariableName, Aggregation.class,
					context.getRepositoryInformation().getDomainType(), pipelineName);

			List<CodeBlock> hints = new ArrayList<>(5);
			if (ReflectionUtils.isVoid(queryMethod.getReturnedObjectType())) {
				hints.add(CodeBlock.of(".skipOutput()"));
			}
			{
				MergedAnnotation<Hint> hintAnnotation = context.getAnnotation(Hint.class);
				String hint = hintAnnotation.isPresent() ? hintAnnotation.getString("value") : null;
				if (StringUtils.hasText(hint)) {
					hints.add(CodeBlock.of(".hint($S)", hint));
				}
			}
			{
				MergedAnnotation<ReadPreference> readPreferenceAnnotation = context.getAnnotation(ReadPreference.class);
				String readPreference = readPreferenceAnnotation.isPresent() ? readPreferenceAnnotation.getString("value")
						: null;
				if (StringUtils.hasText(readPreference)) {
					hints.add(CodeBlock.of(".readPreference($T.valueOf($S))", com.mongodb.ReadPreference.class, readPreference));
				}
			}
			{
				if (queryMethod.hasAnnotatedCollation()) {
					hints.add(CodeBlock.of(".collation($T.parse($S))", Collation.class, queryMethod.getAnnotatedCollation()));
				}
			}

			if (!hints.isEmpty()) {

				Builder optionsBuilder = CodeBlock.builder();
				optionsBuilder.add("$T aggregationOptions = $T.builder()\n", AggregationOptions.class,
						AggregationOptions.class);
				optionsBuilder.indent();
				for (CodeBlock optionBlock : hints) {
					optionsBuilder.add(optionBlock);
					optionsBuilder.add("\n");
				}
				optionsBuilder.add(".build();\n");
				optionsBuilder.unindent();
				builder.add(optionsBuilder.build());

				builder.addStatement("$L = $L.withOptions(aggregationOptions)", aggregationVariableName,
						aggregationVariableName);
			}

			return builder.build();
		}
	}

	static class QueryBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;

		private StringAotQuery source;
		private List<String> arguments;
		private String queryVariableName;

		public QueryBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
			this.context = context;
			this.arguments = Arrays.stream(context.getMethod().getParameters()).map(Parameter::getName)
					.collect(Collectors.toList());

			// ParametersSource parametersSource = ParametersSource.of(repositoryInformation, metadata.getRepositoryMethod());
			// this.argumentSource = new MongoParameters(parametersSource, false);

			this.queryMethod = queryMethod;
		}

		public QueryBlockBuilder filter(StringAotQuery query) {
			this.source = query;
			return this;
		}

		public QueryBlockBuilder usingQueryVariableName(String queryVariableName) {
			this.queryVariableName = queryVariableName;
			return this;
		}

		CodeBlock build() {

			CodeBlock.Builder builder = CodeBlock.builder();

			builder.add("\n");
			builder.add(renderExpressionToQuery(source.query.getQueryString(), queryVariableName));

			if (StringUtils.hasText(source.query.getFieldsString())) {
				builder.add(renderExpressionToDocument(source.query.getFieldsString(), "fields", arguments));
				builder.addStatement("$L.setFieldsObject(fields)", queryVariableName);
			}

			String sortParameter = context.getSortParameterName();
			if (StringUtils.hasText(sortParameter)) {
				builder.addStatement("$L.with($L)", queryVariableName, sortParameter);
			} else if (StringUtils.hasText(source.query.getSortString())) {

				builder.add(renderExpressionToDocument(source.query.getSortString(), "sort", arguments));
				builder.addStatement("$L.setSortObject(sort)", queryVariableName);
			}

			String limitParameter = context.getLimitParameterName();
			if (StringUtils.hasText(limitParameter)) {
				builder.addStatement("$L.limit($L)", queryVariableName, limitParameter);
			} else if (context.getPageableParameterName() == null && source.query.isLimited()) {
				builder.addStatement("$L.limit($L)", queryVariableName, source.query.getLimit());
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

			// TODO: all the meta stuff

			return builder.build();
		}

		private CodeBlock renderExpressionToQuery(@Nullable String source, String variableName) {

			Builder builder = CodeBlock.builder();
			if (!StringUtils.hasText(source)) {
				builder.addStatement("$T $L = new $T(new $T())", BasicQuery.class, variableName, BasicQuery.class,
						Document.class);
			} else if (!containsPlaceholder(source)) {

				String tmpVarName = "%sString".formatted(variableName);
				builder.addStatement("String $L = $S", tmpVarName, source);

				builder.addStatement("$T $L = new $T($T.parse($L))", BasicQuery.class, variableName, BasicQuery.class,
						Document.class, tmpVarName);
			} else {

				String tmpVarName = "%sString".formatted(variableName);
				builder.addStatement("String $L = $S", tmpVarName, source);
				builder.addStatement("$T $L = createQuery($L, new $T[]{ $L })", BasicQuery.class, variableName, tmpVarName,
						Object.class, StringUtils.collectionToDelimitedString(arguments, ", "));
			}

			return builder.build();
		}
	}

	static class UpdateBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;

		private StringAotUpdate source;
		private List<String> arguments;
		private String updateVariableName;

		public UpdateBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
			this.context = context;
			this.arguments = Arrays.stream(context.getMethod().getParameters()).map(Parameter::getName)
					.collect(Collectors.toList());

			// ParametersSource parametersSource = ParametersSource.of(repositoryInformation, metadata.getRepositoryMethod());
			// this.argumentSource = new MongoParameters(parametersSource, false);

			this.queryMethod = queryMethod;
		}

		public UpdateBlockBuilder update(StringAotUpdate update) {
			this.source = update;
			return this;
		}

		public UpdateBlockBuilder usingUpdateVariableName(String updateVariableName) {
			this.updateVariableName = updateVariableName;
			return this;
		}

		CodeBlock build() {

			CodeBlock.Builder builder = CodeBlock.builder();

			builder.add("\n");
			String tmpVarName = updateVariableName + "Definition";
			builder.add(
					renderExpressionToDocument(source.update.getUpdateString(), updateVariableName + "Definition", arguments));
			builder.addStatement("$T $L = new $T($L)", BasicUpdate.class, updateVariableName, BasicUpdate.class, tmpVarName);

			return builder.build();
		}
	}

	private static CodeBlock renderExpressionToDocument(@Nullable String source, String variableName,
			List<String> arguments) {
		Builder builder = CodeBlock.builder();
		if (!StringUtils.hasText(source)) {
			builder.addStatement("$T $L = new $T()", Document.class, variableName, Document.class);
		} else if (!containsPlaceholder(source)) {

			String tmpVarName = "%sString".formatted(variableName);
			builder.addStatement("String $L = $S", tmpVarName, source);
			builder.addStatement("$T $L = $T.parse($L)", Document.class, variableName, Document.class, tmpVarName);
		} else {

			String tmpVarName = "%sString".formatted(variableName);
			builder.addStatement("String $L = $S", tmpVarName, source);
			builder.addStatement("$T $L = bindParameters($L, new $T[]{ $L })", Document.class, variableName, tmpVarName,
					Object.class, StringUtils.collectionToDelimitedString(arguments, ", "));
		}
		return builder.build();
	}

	private static boolean containsPlaceholder(String source) {
		return PARAMETER_BINDING_PATTERN.matcher(source).find();
	}
}
