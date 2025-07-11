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
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.geoNearBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.geoNearExecutionBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.queryBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.queryExecutionBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.updateBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.updateExecutionBlockBuilder;
import static org.springframework.data.mongodb.repository.aot.QueryBlocks.QueryCodeBlockBuilder;

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotRepositoryClassBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.MethodContributor;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.TypeName;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * MongoDB specific {@link RepositoryContributor}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 5.0
 */
public class MongoRepositoryContributor extends RepositoryContributor {

	private static final Log logger = LogFactory.getLog(MongoRepositoryContributor.class);

	private final AotQueryCreator queryCreator;
	private final MongoMappingContext mappingContext;

	public MongoRepositoryContributor(AotRepositoryContext repositoryContext) {

		super(repositoryContext);
		this.queryCreator = new AotQueryCreator();
		this.mappingContext = new MongoMappingContext();
	}

	@Override
	protected void customizeClass(AotRepositoryClassBuilder classBuilder) {
		classBuilder.customize(builder -> builder.superclass(TypeName.get(MongoAotRepositoryFragmentSupport.class)));
	}

	@Override
	protected void customizeConstructor(AotRepositoryConstructorBuilder constructorBuilder) {

		constructorBuilder.addParameter("operations", TypeName.get(MongoOperations.class));
		constructorBuilder.addParameter("context", TypeName.get(RepositoryFactoryBeanSupport.FragmentCreationContext.class),
				false);

		constructorBuilder.customize((builder) -> {
			builder.addStatement("super(operations, context)");
		});
	}

	@Override
	@SuppressWarnings("NullAway")
	protected @Nullable MethodContributor<? extends QueryMethod> contributeQueryMethod(Method method) {

		MongoQueryMethod queryMethod = new MongoQueryMethod(method, getRepositoryInformation(), getProjectionFactory(),
				mappingContext);

		if (backoff(queryMethod)) {
			return null;
		}

		if (queryMethod.hasAnnotatedAggregation()) {
			AggregationInteraction aggregation = new AggregationInteraction(queryMethod.getAnnotatedAggregation());
			return aggregationMethodContributor(queryMethod, aggregation);
		}

		QueryInteraction query = createStringQuery(getRepositoryInformation(), queryMethod,
				AnnotatedElementUtils.findMergedAnnotation(method, Query.class), method);

		if (queryMethod.isSearchQuery() || method.isAnnotationPresent(VectorSearch.class)) {

			VectorSearch vectorSearch = AnnotatedElementUtils.findMergedAnnotation(method, VectorSearch.class);
			return searchMethodContributor(queryMethod, new SearchInteraction(getRepositoryInformation().getDomainType(),
					vectorSearch, query.getQuery(), queryMethod.getParameters()));
		}

		if (queryMethod.isGeoNearQuery() || (queryMethod.getParameters().getMaxDistanceIndex() != -1
				&& queryMethod.getReturnType().isCollectionLike())) {
			NearQueryInteraction near = new NearQueryInteraction(query, queryMethod.getParameters());
			return nearQueryMethodContributor(queryMethod, near);
		}

		if (query.isDelete()) {
			return deleteMethodContributor(queryMethod, query);
		}

		if (queryMethod.isModifyingQuery()) {

			int updateIndex = queryMethod.getParameters().getUpdateIndex();
			if (updateIndex != -1) {

				UpdateInteraction update = new UpdateInteraction(query, null, updateIndex);
				return updateMethodContributor(queryMethod, update);
			} else {

				Update updateSource = queryMethod.getUpdateSource();
				if (StringUtils.hasText(updateSource.value())) {
					UpdateInteraction update = new UpdateInteraction(query, new StringUpdate(updateSource.value()), null);
					return updateMethodContributor(queryMethod, update);
				}

				if (!ObjectUtils.isEmpty(updateSource.pipeline())) {
					AggregationUpdateInteraction update = new AggregationUpdateInteraction(query, updateSource.pipeline());
					return aggregationUpdateMethodContributor(queryMethod, update);
				}
			}
		}

		return queryMethodContributor(queryMethod, query);
	}

	@SuppressWarnings("NullAway")
	private QueryInteraction createStringQuery(RepositoryInformation repositoryInformation, MongoQueryMethod queryMethod,
			@Nullable Query queryAnnotation, Method source) {

		QueryInteraction query;
		if (queryMethod.hasAnnotatedQuery() && queryAnnotation != null) {
			query = new QueryInteraction(new StringQuery(queryMethod.getAnnotatedQuery()), queryAnnotation.count(),
					queryAnnotation.delete(), queryAnnotation.exists());
		} else {

			PartTree partTree = new PartTree(queryMethod.getName(), repositoryInformation.getDomainType());
			query = new QueryInteraction(queryCreator.createQuery(partTree, queryMethod, source),
					partTree.isCountProjection(), partTree.isDelete(), partTree.isExistsProjection());
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

		// TODO: namedQuery, Regex queries, queries accepting Shapes (e.g. within) or returning arrays.
		boolean skip = method.getReturnType().getType().isArray();

		if (skip && logger.isDebugEnabled()) {
			logger.debug("Skipping AOT generation for [%s]. Method is either returning an array or a geo-near, regex query"
					.formatted(method.getName()));
		}
		return skip;
	}

	private static MethodContributor<MongoQueryMethod> nearQueryMethodContributor(MongoQueryMethod queryMethod,
			NearQueryInteraction interaction) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(interaction).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			String variableName = context.localVariable("nearQuery");
			builder.add(geoNearBlockBuilder(context, queryMethod).usingQueryVariableName(variableName).build());

			if (!context.getBindableParameterNames().isEmpty()) {
				String filterQueryVariableName = context.localVariable("filterQuery");
				builder.add(queryBlockBuilder(context, queryMethod).usingQueryVariableName(filterQueryVariableName)
						.filter(interaction.getQuery()).build());
				builder.addStatement("$L.query($L)", variableName, filterQueryVariableName);
			}

			builder.add(geoNearExecutionBlockBuilder(context, queryMethod).referencing(variableName).build());

			return builder.build();
		});
	}

	static MethodContributor<MongoQueryMethod> aggregationMethodContributor(MongoQueryMethod queryMethod,
			AggregationInteraction aggregation) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(aggregation).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			String variableName = "aggregation";
			builder.add(aggregationBlockBuilder(context, queryMethod).stages(aggregation)
					.usingAggregationVariableName(variableName).build());
			builder.add(aggregationExecutionBlockBuilder(context, queryMethod).referencing(variableName).build());

			return builder.build();
		});
	}

	static MethodContributor<MongoQueryMethod> searchMethodContributor(MongoQueryMethod queryMethod,
			SearchInteraction interaction) {
		return MethodContributor.forQueryMethod(queryMethod).withMetadata(interaction).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			String variableName = "search";

			builder.add(
					new VectorSearchBocks.VectorSearchQueryCodeBlockBuilder(context, queryMethod, interaction.getSearchPath())
							.usingVariableName(variableName).withFilter(interaction.getFilter()).build());

			return builder.build();
		});
	}

	static MethodContributor<MongoQueryMethod> updateMethodContributor(MongoQueryMethod queryMethod,
			UpdateInteraction update) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(update).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			// update filter
			String filterVariableName = context.localVariable(update.name());
			builder.add(queryBlockBuilder(context, queryMethod).filter(update.getFilter())
					.usingQueryVariableName(filterVariableName).build());

			// update definition
			String updateVariableName;

			if (update.hasUpdateDefinitionParameter()) {
				updateVariableName = context.getParameterName(update.getRequiredUpdateDefinitionParameter());
			} else {
				updateVariableName = context.localVariable("updateDefinition");
				builder.add(updateBlockBuilder(context, queryMethod).update(update).usingUpdateVariableName(updateVariableName)
						.build());
			}

			builder.add(updateExecutionBlockBuilder(context, queryMethod).withFilter(filterVariableName)
					.referencingUpdate(updateVariableName).build());
			return builder.build();
		});
	}

	static MethodContributor<MongoQueryMethod> aggregationUpdateMethodContributor(MongoQueryMethod queryMethod,
			AggregationUpdateInteraction update) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(update).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			// update filter
			String filterVariableName = context.localVariable(update.name());
			QueryCodeBlockBuilder queryCodeBlockBuilder = queryBlockBuilder(context, queryMethod).filter(update.getFilter());
			builder.add(queryCodeBlockBuilder.usingQueryVariableName(filterVariableName).build());

			// update definition
			String updateVariableName = "updateDefinition";
			builder.add(aggregationBlockBuilder(context, queryMethod).stages(update)
					.usingAggregationVariableName(updateVariableName).pipelineOnly(true).build());

			builder.addStatement("$T $L = $T.from($L.getOperations())", AggregationUpdate.class,
					context.localVariable("aggregationUpdate"), AggregationUpdate.class, updateVariableName);

			builder.add(updateExecutionBlockBuilder(context, queryMethod).withFilter(filterVariableName)
					.referencingUpdate(context.localVariable("aggregationUpdate")).build());
			return builder.build();
		});
	}

	static MethodContributor<MongoQueryMethod> deleteMethodContributor(MongoQueryMethod queryMethod,
			QueryInteraction query) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(query).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();
			QueryCodeBlockBuilder queryCodeBlockBuilder = queryBlockBuilder(context, queryMethod).filter(query);

			String queryVariableName = context.localVariable(query.name());
			builder.add(queryCodeBlockBuilder.usingQueryVariableName(queryVariableName).build());
			builder.add(deleteExecutionBlockBuilder(context, queryMethod).referencing(queryVariableName).build());
			return builder.build();
		});
	}

	static MethodContributor<MongoQueryMethod> queryMethodContributor(MongoQueryMethod queryMethod,
			QueryInteraction query) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(query).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();
			QueryCodeBlockBuilder queryCodeBlockBuilder = queryBlockBuilder(context, queryMethod).filter(query);

			builder.add(queryCodeBlockBuilder.usingQueryVariableName(context.localVariable(query.name())).build());
			builder.add(queryExecutionBlockBuilder(context, queryMethod).forQuery(query).build());
			return builder.build();
		});
	}

}
