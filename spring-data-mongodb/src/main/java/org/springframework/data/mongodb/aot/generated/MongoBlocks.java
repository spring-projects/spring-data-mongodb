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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.bson.Document;

import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.data.mongodb.BindableMongoExpression;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.ExecutableRemoveOperation.ExecutableRemove;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.repository.Hint;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.DeleteExecutionX;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.DeleteExecutionX.Type;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.SlicedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.javapoet.ClassName;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
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

	static QueryExecutionBlockBuilder queryExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {
		return new QueryExecutionBlockBuilder(context, queryMethod);
	}

	static DeleteExecutionBuilder deleteExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			MongoQueryMethod queryMethod) {
		return new DeleteExecutionBuilder(context, queryMethod);
	}

	static class DeleteExecutionBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		String queryVariableName;

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
					context.getRepositoryInformation().getDomainType(),
					mongoOpsRef, context.getRepositoryInformation().getDomainType());

			Type type = Type.FIND_AND_REMOVE_ALL;
			if (!queryMethod.isCollectionQuery()) {
				if (!ClassUtils.isPrimitiveOrWrapper(context.getMethod().getReturnType())) {
					type = Type.FIND_AND_REMOVE_ONE;
				} else {
					type = Type.ALL;
				}
			}

			actualReturnType = ClassUtils.isPrimitiveOrWrapper(context.getMethod().getReturnType())
					? ClassName.get(context.getMethod().getReturnType())
					: queryMethod.isCollectionQuery() ? context.getReturnTypeName() : actualReturnType;

			builder.addStatement("return ($T) new $T(remover, $T.$L).execute($L)", actualReturnType, DeleteExecutionX.class,
					DeleteExecutionX.Type.class, type.name(), queryVariableName);

			return builder.build();
		}
	}

	static class QueryExecutionBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;
		private String queryVariableName;
		private boolean count, exists;

		public QueryExecutionBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
			this.context = context;
			this.queryMethod = queryMethod;
		}

		QueryExecutionBlockBuilder referencing(String queryVariableName) {

			this.queryVariableName = queryVariableName;
			return this;
		}

		QueryExecutionBlockBuilder count(boolean count) {
			this.count = count;
			return this;
		}

		QueryExecutionBlockBuilder exists(boolean exists) {
			this.exists = exists;
			return this;
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
			} else if (count) {
				terminatingMethod = "count()";

			} else if (exists) {
				terminatingMethod = "exists()";
			} else {
				terminatingMethod = Optional.class.isAssignableFrom(context.getReturnType().toClass()) ? "one()" : "oneValue()";
			}

			if (queryMethod.isPageQuery()) {
				builder.addStatement("return new $T(finder, $L).execute($L)", PagedExecution.class,
						context.getPageableParameterName(), queryVariableName);
			} else if (queryMethod.isSliceQuery()) {
				builder.addStatement("return new $T(finder, $L).execute($L)", SlicedExecution.class,
						context.getPageableParameterName(), queryVariableName);
			} else {
				builder.addStatement("return finder.matching($L).$L", queryVariableName, terminatingMethod);
			}

			return builder.build();

		}
	}

	static class QueryBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final MongoQueryMethod queryMethod;

		StringQuery source;
		List<String> arguments;
		private String queryVariableName;

		public QueryBlockBuilder(AotQueryMethodGenerationContext context, MongoQueryMethod queryMethod) {
			this.context = context;
			this.arguments = Arrays.stream(context.getMethod().getParameters()).map(Parameter::getName)
					.collect(Collectors.toList());

			// ParametersSource parametersSource = ParametersSource.of(repositoryInformation, metadata.getRepositoryMethod());
			// this.argumentSource = new MongoParameters(parametersSource, false);

			this.queryMethod = queryMethod;
		}

		public QueryBlockBuilder filter(StringQuery query) {
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
			String queryDocumentVariableName = "%sDocument".formatted(queryVariableName);
			builder.add(renderExpressionToDocument(source.getQueryString(), queryVariableName));
			builder.addStatement("$T $L = new $T($L)", BasicQuery.class, queryVariableName, BasicQuery.class,
					queryDocumentVariableName);

			if (StringUtils.hasText(source.getFieldsString())) {
				builder.add(renderExpressionToDocument(source.getFieldsString(), "fields"));
				builder.addStatement("$L.setFieldsObject(fieldsDocument)", queryVariableName);
			}

			String sortParameter = context.getSortParameterName();
			if (StringUtils.hasText(sortParameter)) {

				builder.addStatement("$L.with($L)", queryVariableName, sortParameter);
			} else if (StringUtils.hasText(source.getSortString())) {

				builder.add(renderExpressionToDocument(source.getSortString(), "sort"));
				builder.addStatement("$L.setSortObject(sortDocument)", queryVariableName);
			}

			String limitParameter = context.getLimitParameterName();
			if (StringUtils.hasText(limitParameter)) {
				builder.addStatement("$L.limit($L)", queryVariableName, limitParameter);
			} else if (context.getPageableParameterName() == null && source.isLimited()) {
				builder.addStatement("$L.limit($L)", queryVariableName, source.getLimit());
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

		private CodeBlock renderExpressionToDocument(@Nullable String source, String variableName) {

			Builder builder = CodeBlock.builder();
			if (!StringUtils.hasText(source)) {
				builder.addStatement("$T $L = new $T()", Document.class, "%sDocument".formatted(variableName), Document.class);
			} else if (!containsPlaceholder(source)) {
				builder.addStatement("$T $L = $T.parse($S)", Document.class, "%sDocument".formatted(variableName),
						Document.class, source);
			} else {

				String mongoOpsRef = context.fieldNameOf(MongoOperations.class);
				String tmpVarName = "%sString".formatted(variableName);

				builder.addStatement("String $L = $S", tmpVarName, source);
				builder.addStatement("$T $L = new $T($L, $L.getConverter(), new $T[]{ $L }).toDocument()", Document.class,
						"%sDocument".formatted(variableName), BindableMongoExpression.class, tmpVarName, mongoOpsRef, Object.class,
						StringUtils.collectionToDelimitedString(arguments, ", "));
			}

			return builder.build();
		}

		private boolean containsPlaceholder(String source) {
			return PARAMETER_BINDING_PATTERN.matcher(source).find();
		}

	}
}
