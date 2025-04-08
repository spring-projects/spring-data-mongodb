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
import java.util.Map;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.mongodb.aot.generated.MongoBlocks.QueryBlockBuilder;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.aot.MongoAotRepositoryFragmentSupport;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryFragmentMetadata;
import org.springframework.data.repository.aot.generate.MethodContributor;
import org.springframework.data.repository.aot.generate.QueryMetadata;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.TypeName;
import org.springframework.javapoet.TypeSpec.Builder;
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
	protected void customizeClass(RepositoryInformation information, AotRepositoryFragmentMetadata metadata,
			Builder builder) {
		builder.superclass(TypeName.get(MongoAotRepositoryFragmentSupport.class));
	}

	@Override
	protected void customizeConstructor(AotRepositoryConstructorBuilder constructorBuilder) {

		constructorBuilder.addParameter("operations", TypeName.get(MongoOperations.class));
		constructorBuilder.addParameter("context", TypeName.get(RepositoryFactoryBeanSupport.FragmentCreationContext.class),
				false);

		constructorBuilder.customize((repositoryInformation, builder) -> {
			builder.addStatement("super(operations, context)");
		});
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

		StringAotQuery query = createStringQuery(repositoryInformation, queryMethod, queryAnnotation,
				method.getParameterCount());

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(query).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();
			writeStringQuery(context, builder, query, queryMethod);
			return builder.build();
		});
	}

	private StringAotQuery createStringQuery(RepositoryInformation repositoryInformation, QueryMethod queryMethod,
			Query queryAnnotation, int parameterCount) {

		StringAotQuery query;
		if (queryAnnotation != null && StringUtils.hasText(queryAnnotation.value())) {
			query = new StringAotQuery(new StringQuery(queryAnnotation.value()), queryAnnotation.count(),
					queryAnnotation.delete(), queryAnnotation.exists());
		} else {
			PartTree partTree = new PartTree(queryMethod.getName(), repositoryInformation.getDomainType());

			query = new StringAotQuery(queryCreator.createQuery(partTree, parameterCount), partTree.isCountProjection(),
					partTree.isDelete(), partTree.isExistsProjection());
		}

		if (queryAnnotation != null && StringUtils.hasText(queryAnnotation.sort())) {
			query = query.withSort(queryAnnotation.sort());
		}
		if (queryAnnotation != null && StringUtils.hasText(queryAnnotation.fields())) {
			query = query.withFields(queryAnnotation.fields());
		}

		return query;
	}

	private static void writeStringQuery(AotQueryMethodGenerationContext context, CodeBlock.Builder body,
			StringAotQuery query, MongoQueryMethod queryMethod) {

		body.add(context.codeBlocks().logDebug("invoking [%s]".formatted(context.getMethod().getName())));
		QueryBlockBuilder queryBlockBuilder = MongoBlocks.queryBlockBuilder(context, queryMethod).filter(query);

		body.add(queryBlockBuilder.usingQueryVariableName(query.name()).build());
		if (query.isDeleteQuery()) {
			body.add(MongoBlocks.deleteExecutionBlockBuilder(context, queryMethod).referencing(query.name()).build());
		} else {
			body.add(MongoBlocks.queryExecutionBlockBuilder(context, queryMethod).forQuery(query).build());
		}
	}

}
