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
import java.util.stream.Collectors;

import org.springframework.data.mongodb.BindableMongoExpression;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.repository.Hint;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.SlicedExecution;
import org.springframework.data.repository.aot.generate.AotRepositoryMethodBuilder.MethodGenerationMetadata;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public class MongoBlocks {

	public static QueryBlockBuilder queryBlockBuilder(RepositoryInformation repositoryInformation,
			MethodGenerationMetadata metadata) {
		return new QueryBlockBuilder(repositoryInformation, metadata);
	}

	public static QueryExecutionBlockBuilder queryExecutionBlockBuilder(RepositoryInformation repositoryInformation,
			MethodGenerationMetadata metadata) {
		return new QueryExecutionBlockBuilder(repositoryInformation, metadata);
	}

	static class QueryExecutionBlockBuilder {

		RepositoryInformation repositoryInformation;
		MethodGenerationMetadata metadata;

		public QueryExecutionBlockBuilder(RepositoryInformation repositoryInformation, MethodGenerationMetadata metadata) {
			this.repositoryInformation = repositoryInformation;
			this.metadata = metadata;
		}

		CodeBlock build(String queryVariableName) {

			String mongoOpsRef = metadata.fieldNameOf(MongoOperations.class);

			Builder builder = CodeBlock.builder();

			boolean isProjecting = metadata.getActualReturnType() != null && !ObjectUtils
					.nullSafeEquals(TypeName.get(repositoryInformation.getDomainType()), metadata.getActualReturnType());
			Object actualReturnType = isProjecting ? metadata.getActualReturnType() : repositoryInformation.getDomainType();

			if (isProjecting) {
				// builder.addStatement("$T<$T> finder = $L.query($T.class).as($T.class).matching($L)", TerminatingFind.class,
				// actualReturnType, mongoOpsRef, repositoryInformation.getDomainType(), actualReturnType, queryVariableName);
				builder.addStatement("$T<$T> finder = $L.query($T.class).as($T.class)", FindWithQuery.class, actualReturnType,
						mongoOpsRef, repositoryInformation.getDomainType(), actualReturnType);
			} else {
				// builder.addStatement("$T<$T> finder = $L.query($T.class).matching($L)", TerminatingFind.class,
				// actualReturnType,
				// mongoOpsRef, repositoryInformation.getDomainType(), queryVariableName);
				builder.addStatement("$T<$T> finder = $L.query($T.class)", FindWithQuery.class, actualReturnType, mongoOpsRef,
						repositoryInformation.getDomainType());
			}

			String terminatingMethod = "all()";
			if (metadata.returnsSingleValue()) {
				if (metadata.returnsOptionalValue()) {
					terminatingMethod = "one()";
				} else {
					terminatingMethod = "oneValue()";
				}
			}

			if (metadata.returnsPage()) {
				// builder.addStatement("return finder.$L", terminatingMethod);
				builder.addStatement("return new $T(finder, $L).execute($L)", PagedExecution.class,
						metadata.getPageableParameterName(), queryVariableName);
			} else if (metadata.returnsSlice()) {
				builder.addStatement("return new $T(finder, $L).execute($L)", SlicedExecution.class,
						metadata.getPageableParameterName(), queryVariableName);
			} else {
				builder.addStatement("return finder.matching($L).$L", queryVariableName, terminatingMethod);
				// builder.addStatement("return $T.getPage(finder.$L, $L, () -> finder.count())", PageableExecutionUtils.class,
				// terminatingMethod,
				// metadata.getPageableParameterName());
			}

			// new MongoQueryExecution.PagedExecution(finder, page).execute(query);

			return builder.build();

		}

	}

	static class QueryBlockBuilder {

		MethodGenerationMetadata metadata;
		String queryString;
		List<String> arguments;
		// MongoParameters argumentSource;

		public QueryBlockBuilder(RepositoryInformation repositoryInformation, MethodGenerationMetadata metadata) {
			this.metadata = metadata;
			this.arguments = Arrays.stream(metadata.getRepositoryMethod().getParameters()).map(Parameter::getName)
					.collect(Collectors.toList());

			// ParametersSource parametersSource = ParametersSource.of(repositoryInformation, metadata.getRepositoryMethod());
			// this.argumentSource = new MongoParameters(parametersSource, false);

		}

		public QueryBlockBuilder filter(String filter) {
			this.queryString = filter;
			return this;
		}

		CodeBlock build(String queryVariableName) {

			String mongoOpsRef = metadata.fieldNameOf(MongoOperations.class);

			CodeBlock.Builder builder = CodeBlock.builder();

			builder.addStatement("$T filter = new $T($S, $L.getConverter(), new $T[]{ $L })", BindableMongoExpression.class,
					BindableMongoExpression.class, queryString, mongoOpsRef, Object.class,
					StringUtils.collectionToCommaDelimitedString(arguments));
			builder.addStatement("$T $L = new $T(filter.toDocument())",
					org.springframework.data.mongodb.core.query.Query.class, queryVariableName, BasicQuery.class);

			String sortParameter = metadata.getSortParameterName();
			if (StringUtils.hasText(sortParameter)) {
				builder.addStatement("$L.with($L)", queryVariableName, sortParameter);
			}

			String limitParameter = metadata.getLimitParameterName();
			if (StringUtils.hasText(limitParameter)) {
				builder.addStatement("$L.limit($L)", queryVariableName, limitParameter);
			}

			String pageableParameter = metadata.getPageableParameterName();
			if (StringUtils.hasText(pageableParameter)) {
				builder.addStatement("$L.with($L)", queryVariableName, pageableParameter);
			}

			String hint = metadata.annotationValue(Hint.class, "value");

			if (StringUtils.hasText(hint)) {
				builder.addStatement("$L.withHint($S)", queryVariableName, hint);
			}

			String readPreference = metadata.annotationValue(ReadPreference.class, "value");
			if (StringUtils.hasText(readPreference)) {
				builder.addStatement("$L.withReadPreference($T.valueOf($S))", queryVariableName,
						com.mongodb.ReadPreference.class, readPreference);
			}

			// TODO: all the meta stuff

			return builder.build();
		}

	}
}
