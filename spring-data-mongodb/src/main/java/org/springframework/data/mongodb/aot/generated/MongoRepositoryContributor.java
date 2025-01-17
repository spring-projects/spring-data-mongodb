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

import java.util.regex.Pattern;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.mongodb.aot.generated.MongoBlocks.QueryBlockBuilder;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryMethodBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryMethodGenerationContext;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.javapoet.MethodSpec.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @since 2025/01
 */
public class MongoRepositoryContributor extends RepositoryContributor {

	private AotQueryCreator queryCreator;

	public MongoRepositoryContributor(AotRepositoryContext repositoryContext) {
		super(repositoryContext);
		this.queryCreator = new AotQueryCreator();
	}

	@Override
	protected void customizeConstructor(AotRepositoryConstructorBuilder constructorBuilder) {
		constructorBuilder.addParameter("operations", TypeName.get(MongoOperations.class));
	}

	@Override
	protected AotRepositoryMethodBuilder contributeRepositoryMethod(
			AotRepositoryMethodGenerationContext generationContext) {

		// TODO: do not generate stuff for spel expressions

		if (AnnotatedElementUtils.hasAnnotation(generationContext.getMethod(), Aggregation.class)) {
			return null;
		}
		{
			Query queryAnnotation = AnnotatedElementUtils.findMergedAnnotation(generationContext.getMethod(), Query.class);
			if (queryAnnotation != null) {
				if (StringUtils.hasText(queryAnnotation.value())
						&& Pattern.compile("[\\?:][#$]\\{.*\\}").matcher(queryAnnotation.value()).find()) {
					return null;
				}
			}
		}

		// so the rest should work
		return new AotRepositoryMethodBuilder(generationContext).customize((context, body) -> {

			Query queryAnnotation = AnnotatedElementUtils.findMergedAnnotation(context.getMethod(), Query.class);
			StringQuery query;
			if (queryAnnotation != null && StringUtils.hasText(queryAnnotation.value())) {
				query = new StringQuery(queryAnnotation.value());

			} else {
				PartTree partTree = new PartTree(context.getMethod().getName(),
						context.getRepositoryInformation().getDomainType());
				query = queryCreator.createQuery(partTree, context.getMethod().getParameterCount());
			}

			if (queryAnnotation != null && StringUtils.hasText(queryAnnotation.sort())) {
				query.sort(queryAnnotation.sort());
			}
			if (queryAnnotation != null && StringUtils.hasText(queryAnnotation.fields())) {
				query.fields(queryAnnotation.fields());
			}

			writeStringQuery(context, body, query);
		});
	}

	private static void writeStringQuery(AotRepositoryMethodGenerationContext context, Builder body, StringQuery query) {

		body.addCode(context.codeBlocks().logDebug("invoking [%s]".formatted(context.getMethod().getName())));
		QueryBlockBuilder queryBlockBuilder = MongoBlocks.queryBlockBuilder(context).filter(query);

		if (context.isDeleteMethod()) {

			String deleteQueryVariableName = "deleteQuery";
			body.addCode(queryBlockBuilder.usingQueryVariableName(deleteQueryVariableName).build());
			body.addCode(MongoBlocks.deleteExecutionBlockBuilder(context).referencing(deleteQueryVariableName).build());
		} else {

			String filterQueryVariableName = "filterQuery";
			body.addCode(queryBlockBuilder.usingQueryVariableName(filterQueryVariableName).build());
			body.addCode(MongoBlocks.queryExecutionBlockBuilder(context).referencing(filterQueryVariableName).build());
		}
	}

	private static void userAnnotatedQuery(AotRepositoryMethodGenerationContext context, Builder body, Query query) {
		writeStringQuery(context, body, new StringQuery(query.value()));
	}
}
