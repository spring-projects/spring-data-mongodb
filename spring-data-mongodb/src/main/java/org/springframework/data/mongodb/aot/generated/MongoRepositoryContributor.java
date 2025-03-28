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

import java.lang.reflect.Method;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.mongodb.aot.generated.MongoBlocks.QueryBlockBuilder;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.MethodContributor;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.TypeName;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @since 2025/01
 */
public class MongoRepositoryContributor extends RepositoryContributor {

	private final AotQueryCreator queryCreator;
	private final MongoMappingContext mappingContext;

	public MongoRepositoryContributor(AotRepositoryContext repositoryContext) {
		super(repositoryContext);
		this.queryCreator = new AotQueryCreator();
		this.mappingContext = new MongoMappingContext();
	}

	@Override
	protected void customizeConstructor(AotRepositoryConstructorBuilder constructorBuilder) {
		constructorBuilder.addParameter("operations", TypeName.get(MongoOperations.class));
	}

	@Override
	protected @Nullable MethodContributor<? extends QueryMethod> contributeQueryMethod(Method method,
			RepositoryInformation repositoryInformation) {

		if (AnnotatedElementUtils.hasAnnotation(method, Aggregation.class)) {
			return null;
		}

		Query queryAnnotation = AnnotatedElementUtils.findMergedAnnotation(method, Query.class);
		if (queryAnnotation != null) {
			if (StringUtils.hasText(queryAnnotation.value())
					&& Pattern.compile("[\\?:][#$]\\{.*\\}").matcher(queryAnnotation.value()).find()) {
				return null;
			}
		}

		MongoQueryMethod queryMethod = new MongoQueryMethod(method, repositoryInformation, getProjectionFactory(),
				mappingContext);

		return MethodContributor.forQueryMethod(queryMethod).contribute(context -> {
			CodeBlock.Builder builder = CodeBlock.builder();

			boolean count, delete, exists;
			StringQuery query;
			if (queryAnnotation != null && StringUtils.hasText(queryAnnotation.value())) {
				query = new StringQuery(queryAnnotation.value());
				count = queryAnnotation.count();
				delete = queryAnnotation.delete();
				exists = queryAnnotation.exists();

			} else {
				PartTree partTree = new PartTree(context.getMethod().getName(),
						context.getRepositoryInformation().getDomainType());
				query = queryCreator.createQuery(partTree, context.getMethod().getParameterCount());
				count = partTree.isCountProjection();
				delete = partTree.isDelete();
				exists = partTree.isExistsProjection();

			}

			if (queryAnnotation != null && StringUtils.hasText(queryAnnotation.sort())) {
				query.sort(queryAnnotation.sort());
			}
			if (queryAnnotation != null && StringUtils.hasText(queryAnnotation.fields())) {
				query.fields(queryAnnotation.fields());
			}

			writeStringQuery(context, builder, count, delete, exists, query, queryMethod);

			return builder.build();
		});
	}

	private static void writeStringQuery(AotQueryMethodGenerationContext context, CodeBlock.Builder body, boolean count,
			boolean delete, boolean exists, StringQuery query, MongoQueryMethod queryMethod) {

		body.add(context.codeBlocks().logDebug("invoking [%s]".formatted(context.getMethod().getName())));
		QueryBlockBuilder queryBlockBuilder = MongoBlocks.queryBlockBuilder(context, queryMethod).filter(query);

		if (delete) {

			String deleteQueryVariableName = "deleteQuery";
			body.add(queryBlockBuilder.usingQueryVariableName(deleteQueryVariableName).build());
			body.add(
					MongoBlocks.deleteExecutionBlockBuilder(context, queryMethod).referencing(deleteQueryVariableName).build());
		} else {

			String filterQueryVariableName = "filterQuery";
			body.add(queryBlockBuilder.usingQueryVariableName(filterQueryVariableName).build());
			body.add(MongoBlocks.queryExecutionBlockBuilder(context, queryMethod).exists(exists).count(count)
					.referencing(filterQueryVariableName).build());
		}
	}

}
