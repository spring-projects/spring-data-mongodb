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

import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.aggregationBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.aggregationExecutionBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.deleteExecutionBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.queryBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.queryExecutionBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.updateBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.updateExecutionBlockBuilder;

import java.lang.reflect.Method;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.QueryCodeBlockBuilder;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryFragmentMetadata;
import org.springframework.data.repository.aot.generate.MethodContributor;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.TypeName;
import org.springframework.javapoet.TypeSpec.Builder;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * MongoDB specific {@link RepositoryContributor}.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
public class MongoRepositoryContributor extends RepositoryContributor {

	private static final Log logger = LogFactory.getLog(RepositoryContributor.class);

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
	@SuppressWarnings("NullAway")
	protected @Nullable MethodContributor<? extends QueryMethod> contributeQueryMethod(Method method,
			RepositoryInformation repositoryInformation) {

		MongoQueryMethod queryMethod = new MongoQueryMethod(method, repositoryInformation, getProjectionFactory(),
				mappingContext);

		if (backoff(queryMethod)) {
			return null;
		}

		try {
			if (queryMethod.hasAnnotatedAggregation()) {

				AggregationInteraction aggregation = new AggregationInteraction(queryMethod.getAnnotatedAggregation());
				return aggregationMethodContributor(queryMethod, aggregation);
			}

			QueryInteraction query = createStringQuery(repositoryInformation, queryMethod,
					AnnotatedElementUtils.findMergedAnnotation(method, Query.class), method.getParameterCount());

			if (queryMethod.hasAnnotatedQuery()) {
				if (StringUtils.hasText(queryMethod.getAnnotatedQuery())
						&& Pattern.compile("[\\?:][#$]\\{.*\\}").matcher(queryMethod.getAnnotatedQuery()).find()) {

					if (logger.isDebugEnabled()) {
						logger.debug(
								"Skipping AOT generation for [%s]. SpEL expressions are not supported".formatted(method.getName()));
					}
					return MethodContributor.forQueryMethod(queryMethod).metadataOnly(query);
				}
			}

			if (query.isDelete()) {
				return deleteMethodContributor(queryMethod, query);
			}

			if (queryMethod.isModifyingQuery()) {

				Update updateSource = queryMethod.getUpdateSource();
				if (StringUtils.hasText(updateSource.value())) {
					UpdateInteraction update = new UpdateInteraction(query, new StringUpdate(updateSource.value()));
					return updateMethodContributor(queryMethod, update);
				}
				if (!ObjectUtils.isEmpty(updateSource.pipeline())) {
					AggregationUpdateInteraction update = new AggregationUpdateInteraction(query, updateSource.pipeline());
					return aggregationUpdateMethodContributor(queryMethod, update);
				}
			}

			return queryMethodContributor(queryMethod, query);
		} catch (RuntimeException codeGenerationError) {
			if (logger.isErrorEnabled()) {
				logger.error("Failed to generate code for [%s] [%s]".formatted(repositoryInformation.getRepositoryInterface(),
						method.getName()), codeGenerationError);
			}
		}
		return null;
	}

	@SuppressWarnings("NullAway")
	private QueryInteraction createStringQuery(RepositoryInformation repositoryInformation, MongoQueryMethod queryMethod,
			@Nullable Query queryAnnotation, int parameterCount) {

		QueryInteraction query;
		if (queryMethod.hasAnnotatedQuery() && queryAnnotation != null) {
			query = new QueryInteraction(new StringQuery(queryMethod.getAnnotatedQuery()), queryAnnotation.count(),
					queryAnnotation.delete(), queryAnnotation.exists());
		} else {

			PartTree partTree = new PartTree(queryMethod.getName(), repositoryInformation.getDomainType());
			query = new QueryInteraction(queryCreator.createQuery(partTree, parameterCount), partTree.isCountProjection(),
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

	private static boolean backoff(MongoQueryMethod method) {

		boolean skip = method.isGeoNearQuery() || method.isScrollQuery() || method.isStreamQuery()
				|| method.isSearchQuery();

		if (skip && logger.isDebugEnabled()) {
			logger.debug("Skipping AOT generation for [%s]. Method is either geo-near, streaming, search or scrolling query"
					.formatted(method.getName()));
		}
		return skip;
	}

	private static MethodContributor<MongoQueryMethod> aggregationMethodContributor(MongoQueryMethod queryMethod,
			AggregationInteraction aggregation) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(aggregation).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			builder.add(aggregationBlockBuilder(context, queryMethod).stages(aggregation)
					.usingAggregationVariableName("aggregation").build());
			builder.add(aggregationExecutionBlockBuilder(context, queryMethod).referencing("aggregation").build());

			return builder.build();
		});
	}

	private static MethodContributor<MongoQueryMethod> updateMethodContributor(MongoQueryMethod queryMethod,
			UpdateInteraction update) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(update).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();
			builder.add(context.codeBlocks().logDebug("invoking [%s]".formatted(context.getMethod().getName())));

			// update filter
			String filterVariableName = update.name();
			builder.add(queryBlockBuilder(context, queryMethod).filter(update.getFilter())
					.usingQueryVariableName(filterVariableName).build());

			// update definition
			String updateVariableName = "updateDefinition";
			builder.add(
					updateBlockBuilder(context, queryMethod).update(update).usingUpdateVariableName(updateVariableName).build());

			builder.add(updateExecutionBlockBuilder(context, queryMethod).withFilter(filterVariableName)
					.referencingUpdate(updateVariableName).build());
			return builder.build();
		});
	}

	private static MethodContributor<MongoQueryMethod> aggregationUpdateMethodContributor(MongoQueryMethod queryMethod,
			AggregationUpdateInteraction update) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(update).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();
			builder.add(context.codeBlocks().logDebug("invoking [%s]".formatted(context.getMethod().getName())));

			// update filter
			String filterVariableName = update.name();
			QueryCodeBlockBuilder queryCodeBlockBuilder = queryBlockBuilder(context, queryMethod).filter(update.getFilter());
			builder.add(queryCodeBlockBuilder.usingQueryVariableName(filterVariableName).build());

			// update definition
			String updateVariableName = "updateDefinition";
			builder.add(aggregationBlockBuilder(context, queryMethod).stages(update)
					.usingAggregationVariableName(updateVariableName).pipelineOnly(true).build());

			builder.addStatement("$T aggregationUpdate = $T.from($L.getOperations())", AggregationUpdate.class,
					AggregationUpdate.class, updateVariableName);

			builder.add(updateExecutionBlockBuilder(context, queryMethod).withFilter(filterVariableName)
					.referencingUpdate("aggregationUpdate").build());
			return builder.build();
		});
	}

	private static MethodContributor<MongoQueryMethod> deleteMethodContributor(MongoQueryMethod queryMethod,
			QueryInteraction query) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(query).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();
			builder.add(context.codeBlocks().logDebug("invoking [%s]".formatted(context.getMethod().getName())));
			QueryCodeBlockBuilder queryCodeBlockBuilder = queryBlockBuilder(context, queryMethod).filter(query);

			builder.add(queryCodeBlockBuilder.usingQueryVariableName(query.name()).build());
			builder.add(deleteExecutionBlockBuilder(context, queryMethod).referencing(query.name()).build());
			return builder.build();
		});
	}

	private static MethodContributor<MongoQueryMethod> queryMethodContributor(MongoQueryMethod queryMethod,
			QueryInteraction query) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(query).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();
			builder.add(context.codeBlocks().logDebug("invoking [%s]".formatted(context.getMethod().getName())));
			QueryCodeBlockBuilder queryCodeBlockBuilder = queryBlockBuilder(context, queryMethod).filter(query);

			builder.add(queryCodeBlockBuilder.usingQueryVariableName(query.name()).build());
			builder.add(queryExecutionBlockBuilder(context, queryMethod).forQuery(query).build());
			return builder.build();
		});
	}
}
