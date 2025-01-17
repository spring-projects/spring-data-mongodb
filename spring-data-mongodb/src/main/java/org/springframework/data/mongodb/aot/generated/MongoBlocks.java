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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.mongodb.BindableMongoExpression;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.repository.Hint;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.SlicedExecution;
import org.springframework.data.repository.aot.generate.AotRepositoryMethodGenerationContext;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public class MongoBlocks {

	private static final Pattern PARAMETER_BINDING_PATTERN = Pattern.compile("\\?(\\d+)");

	public static QueryBlockBuilder queryBlockBuilder(AotRepositoryMethodGenerationContext context) {
		return new QueryBlockBuilder(context);
	}

	public static QueryExecutionBlockBuilder queryExecutionBlockBuilder(AotRepositoryMethodGenerationContext context) {
		return new QueryExecutionBlockBuilder(context);
	}

	static class QueryExecutionBlockBuilder {

		AotRepositoryMethodGenerationContext context;

		public QueryExecutionBlockBuilder(AotRepositoryMethodGenerationContext context) {
			this.context = context;
		}

		CodeBlock build(String queryVariableName) {

			String mongoOpsRef = context.fieldNameOf(MongoOperations.class);

			Builder builder = CodeBlock.builder();

			boolean isProjecting = context.getActualReturnType() != null
					&& !ObjectUtils.nullSafeEquals(TypeName.get(context.getRepositoryInformation().getDomainType()),
							context.getActualReturnType());
			Object actualReturnType = isProjecting ? context.getActualReturnType()
					: context.getRepositoryInformation().getDomainType();

			builder.add("\n");
			if (isProjecting) {
				builder.addStatement("$T<$T> finder = $L.query($T.class).as($T.class)", FindWithQuery.class, actualReturnType,
						mongoOpsRef, context.getRepositoryInformation().getDomainType(), actualReturnType);
			} else {
				builder.addStatement("$T<$T> finder = $L.query($T.class)", FindWithQuery.class, actualReturnType, mongoOpsRef,
						context.getRepositoryInformation().getDomainType());
			}

			String terminatingMethod = "all()";
			if (context.returnsSingleValue()) {

				if (context.returnsOptionalValue()) {
					terminatingMethod = "one()";
				} else if (context.isCountMethod()) {
					terminatingMethod = "count()";
				} else if (context.isExistsMethod()) {
					terminatingMethod = "exists()";
				} else {
					terminatingMethod = "oneValue()";
				}
			}

			if (context.returnsPage()) {
				// builder.addStatement("return finder.$L", terminatingMethod);
				builder.addStatement("return new $T(finder, $L).execute($L)", PagedExecution.class,
						context.getPageableParameterName(), queryVariableName);
			} else if (context.returnsSlice()) {
				builder.addStatement("return new $T(finder, $L).execute($L)", SlicedExecution.class,
						context.getPageableParameterName(), queryVariableName);
			} else {
				builder.addStatement("return finder.matching($L).$L", queryVariableName, terminatingMethod);
			}

			return builder.build();

		}

	}

	static class QueryBlockBuilder {

		AotRepositoryMethodGenerationContext context;
		StringQuery source;
		List<String> arguments;
		
		public QueryBlockBuilder(AotRepositoryMethodGenerationContext context) {
			this.context = context;
			this.arguments = Arrays.stream(context.getMethod().getParameters()).map(Parameter::getName)
					.collect(Collectors.toList());

			// ParametersSource parametersSource = ParametersSource.of(repositoryInformation, metadata.getRepositoryMethod());
			// this.argumentSource = new MongoParameters(parametersSource, false);

		}

		public QueryBlockBuilder filter(StringQuery query) {
			this.source = query;
			return this;
		}

		CodeBlock build(String queryVariableName) {

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
			if (StringUtils.hasText(pageableParameter) && !context.returnsPage() && !context.returnsSlice()) {
				builder.addStatement("$L.with($L)", queryVariableName, pageableParameter);
			}

			String hint = context.annotationValue(Hint.class, "value");

			if (StringUtils.hasText(hint)) {
				builder.addStatement("$L.withHint($S)", queryVariableName, hint);
			}

			String readPreference = context.annotationValue(ReadPreference.class, "value");
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
