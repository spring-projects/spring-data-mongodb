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

import static org.springframework.data.mongodb.repository.aot.MongoCodeBlocks.*;
import static org.springframework.data.mongodb.repository.aot.QueryBlocks.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.mongodb.repository.config.MongoRepositoryConfigurationExtension;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.aot.generate.AotRepositoryClassBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.MethodContributor;
import org.springframework.data.repository.aot.generate.QueryMetadata;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.config.PropertiesBasedNamedQueriesFactoryBean;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
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
	private final SimpleTypeHolder simpleTypeHolder;
	private final MongoMappingContext mappingContext;
	private final NamedQueries namedQueries;

	public MongoRepositoryContributor(AotRepositoryContext repositoryContext) {

		super(repositoryContext);

		ClassLoader classLoader = repositoryContext.getBeanFactory() != null ? repositoryContext.getClassLoader() : null;
		if (classLoader == null) {
			classLoader = getClass().getClassLoader();
		}
		namedQueries = getNamedQueries(repositoryContext.getConfigurationSource(), classLoader);

		// avoid Java Time (JSR-310) Type introspection
		MongoCustomConversions mongoCustomConversions = MongoCustomConversions
				.create(MongoCustomConversions.MongoConverterConfigurationAdapter::useNativeDriverJavaTimeCodecs);

		this.simpleTypeHolder = mongoCustomConversions.getSimpleTypeHolder();

		this.mappingContext = new MongoMappingContext();
		this.mappingContext.setSimpleTypeHolder(this.simpleTypeHolder);
		this.mappingContext.setAutoIndexCreation(false);
		this.mappingContext.afterPropertiesSet();

		this.queryCreator = new AotQueryCreator(this.mappingContext);
	}

	@SuppressWarnings("NullAway")
	private NamedQueries getNamedQueries(@Nullable RepositoryConfigurationSource configSource, ClassLoader classLoader) {

		String location = configSource != null ? configSource.getNamedQueryLocation().orElse(null) : null;

		if (location == null) {
			location = new MongoRepositoryConfigurationExtension().getDefaultNamedQueryLocation();
		}

		if (StringUtils.hasText(location)) {

			try {

				PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(classLoader);

				PropertiesBasedNamedQueriesFactoryBean factoryBean = new PropertiesBasedNamedQueriesFactoryBean();
				factoryBean.setLocations(resolver.getResources(location));
				factoryBean.afterPropertiesSet();
				return factoryBean.getObject();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		return new PropertiesBasedNamedQueries(new Properties());
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

		MethodContributor.RepositoryMethodContribution contribution = null;
		QueryMetadata queryMetadata = null;

		if (queryMethod.hasAnnotatedAggregation()) {
			AggregationInteraction aggregation = new AggregationInteraction(queryMethod.getAnnotatedAggregation());
			queryMetadata = aggregation;
			contribution = aggregationMethodContributor(queryMethod, simpleTypeHolder, aggregation);
		} else {

			QueryInteraction query = createStringQuery(getRepositoryInformation(), queryMethod,
					AnnotatedElementUtils.findMergedAnnotation(method, Query.class), method);

			if (queryMethod.hasAnnotatedVectorSearch()) {

				VectorSearch vectorSearch = queryMethod.getRequiredVectorSearchAnnotation();
				SearchInteraction interaction = new SearchInteraction(getRepositoryInformation().getDomainType(), vectorSearch,
						query.getQuery(), queryMethod.getParameters(), mappingContext);

				queryMetadata = interaction;
				contribution = searchMethodContributor(queryMethod, interaction);
			} else if (queryMethod.isGeoNearQuery() || (queryMethod.getParameters().getMaxDistanceIndex() != -1
					&& queryMethod.getReturnType().isCollectionLike())) {
				NearQueryInteraction near = new NearQueryInteraction(query, queryMethod.getParameters());
				queryMetadata = near;
				contribution = nearQueryMethodContributor(queryMethod, near);
			} else if (query.isDelete()) {

				queryMetadata = query;
				contribution = deleteMethodContributor(queryMethod, query);
			} else if (queryMethod.isModifyingQuery()) {

				int updateIndex = queryMethod.getParameters().getUpdateIndex();
				if (updateIndex != -1) {

					UpdateInteraction update = new UpdateInteraction(query, null, updateIndex);
					queryMetadata = update;
					contribution = updateMethodContributor(queryMethod, update);
				} else {

					Update updateSource = queryMethod.getUpdateSource();
					if (StringUtils.hasText(updateSource.value())) {
						UpdateInteraction update = new UpdateInteraction(query, new StringUpdate(updateSource.value()), null);
						queryMetadata = update;
						contribution = updateMethodContributor(queryMethod, update);
					}

					if (!ObjectUtils.isEmpty(updateSource.pipeline())) {
						AggregationUpdateInteraction update = new AggregationUpdateInteraction(query, updateSource.pipeline());
						queryMetadata = update;
						contribution = aggregationUpdateMethodContributor(queryMethod, simpleTypeHolder, update);
					}
				}
			} else {
				queryMetadata = query;
				contribution = queryMethodContributor(queryMethod, query);
			}
		}

		if (queryMetadata == null) {
			return null;
		}

		if (backoff(queryMethod) || contribution == null) {
			return MethodContributor.forQueryMethod(queryMethod).metadataOnly(queryMetadata);
		}

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(queryMetadata).contribute(contribution);
	}

	@SuppressWarnings("NullAway")
	private QueryInteraction createStringQuery(RepositoryInformation repositoryInformation, MongoQueryMethod queryMethod,
			@Nullable Query queryAnnotation, Method source) {

		QueryInteraction query;
		if (queryMethod.hasAnnotatedQuery() && queryAnnotation != null) {
			query = new QueryInteraction(new AotStringQuery(queryMethod.getAnnotatedQuery()), queryAnnotation.count(),
					queryAnnotation.delete(), queryAnnotation.exists());
		} else if (namedQueries.hasQuery(queryMethod.getNamedQueryName())) {
			query = new QueryInteraction(new AotStringQuery(namedQueries.getQuery(queryMethod.getNamedQueryName())),
					queryAnnotation != null && queryAnnotation.count(), queryAnnotation != null && queryAnnotation.delete(),
					queryAnnotation != null && queryAnnotation.exists());
		} else {

			PartTree partTree = new PartTree(queryMethod.getName(), repositoryInformation.getDomainType());
			AotStringQuery aotStringQuery = queryCreator.createQuery(partTree, queryMethod, source);
			query = new QueryInteraction(aotStringQuery,
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

		// TODO: returning arrays.
		boolean skip = method.getReturnType().getType().isArray();

		if (skip && logger.isDebugEnabled()) {
			logger.debug("Skipping AOT generation for [%s]. Method is either returning an array or a geo-near, regex query"
					.formatted(method.getName()));
		}
		return skip;
	}

	private static MethodContributor.RepositoryMethodContribution nearQueryMethodContributor(MongoQueryMethod queryMethod,
			NearQueryInteraction interaction) {

		return context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			String variableName = context.localVariable("nearQuery");
			builder.add(geoNearBlockBuilder(context, queryMethod).usingQueryVariableName(variableName).build());

			if (!context.getBindableParameterNames().isEmpty()) {
				String filterQueryVariableName = context.localVariable("filterQuery");
				builder.add(queryBlockBuilder(context, queryMethod).usingQueryVariableName(filterQueryVariableName)
						.filter(interaction.getQuery()).build());
				builder.addStatement("$L.query($L)", variableName, filterQueryVariableName);
			}

			builder.add(geoNearExecutionBlockBuilder(context).referencing(variableName).build());

			return builder.build();
		};
	}

	static MethodContributor.RepositoryMethodContribution aggregationMethodContributor(MongoQueryMethod queryMethod,
			SimpleTypeHolder simpleTypeHolder,
			AggregationInteraction aggregation) {

		return context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			String variableName = context.localVariable("aggregation");

			builder.add(aggregationBlockBuilder(context, simpleTypeHolder, queryMethod).stages(aggregation)
					.usingAggregationVariableName(variableName).build());
			builder.add(
					aggregationExecutionBlockBuilder(context, simpleTypeHolder, queryMethod).referencing(variableName).build());

			return builder.build();
		};
	}

	static MethodContributor.RepositoryMethodContribution searchMethodContributor(MongoQueryMethod queryMethod,
			SearchInteraction interaction) {

		return context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			String variableName = context.localVariable("search");

			builder.add(
					new VectorSearchBlocks.VectorSearchQueryCodeBlockBuilder(context, queryMethod, interaction.getSearchPath())
							.usingVariableName(variableName).withFilter(interaction.getFilter()).build());

			return builder.build();
		};
	}

	static MethodContributor.RepositoryMethodContribution updateMethodContributor(MongoQueryMethod queryMethod,
			UpdateInteraction update) {

		return context -> {

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
				builder.add(updateBlockBuilder(context).update(update).usingUpdateVariableName(updateVariableName).build());
			}

			builder.add(updateExecutionBlockBuilder(context, queryMethod).withFilter(filterVariableName)
					.referencingUpdate(updateVariableName).build());
			return builder.build();
		};
	}

	static MethodContributor.RepositoryMethodContribution aggregationUpdateMethodContributor(MongoQueryMethod queryMethod,
			SimpleTypeHolder simpleTypeHolder,
			AggregationUpdateInteraction update) {

		return context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			// update filter
			String filterVariableName = context.localVariable(update.name());
			QueryCodeBlockBuilder queryCodeBlockBuilder = queryBlockBuilder(context, queryMethod).filter(update.getFilter());
			builder.add(queryCodeBlockBuilder.usingQueryVariableName(filterVariableName).build());

			// update definition
			String updateVariableName = context.localVariable("updateDefinition");
			builder.add(aggregationBlockBuilder(context, simpleTypeHolder, queryMethod).stages(update)
					.usingAggregationVariableName(updateVariableName).pipelineOnly(true).build());

			builder.addStatement("$T $L = $T.from($L.getOperations())", AggregationUpdate.class,
					context.localVariable("aggregationUpdate"), AggregationUpdate.class, updateVariableName);

			builder.add(updateExecutionBlockBuilder(context, queryMethod).withFilter(filterVariableName)
					.referencingUpdate(context.localVariable("aggregationUpdate")).build());
			return builder.build();
		};
	}

	static MethodContributor.RepositoryMethodContribution deleteMethodContributor(MongoQueryMethod queryMethod,
			QueryInteraction query) {

		return context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			QueryCodeBlockBuilder queryCodeBlockBuilder = queryBlockBuilder(context, queryMethod).filter(query);

			String queryVariableName = context.localVariable(query.name());
			builder.add(queryCodeBlockBuilder.usingQueryVariableName(queryVariableName).build());
			builder.add(deleteExecutionBlockBuilder(context, queryMethod).referencing(queryVariableName).build());
			return builder.build();
		};
	}

	static MethodContributor.RepositoryMethodContribution queryMethodContributor(MongoQueryMethod queryMethod,
			QueryInteraction query) {

		return context -> {

			CodeBlock.Builder builder = CodeBlock.builder();

			QueryCodeBlockBuilder queryCodeBlockBuilder = queryBlockBuilder(context, queryMethod).filter(query);

			builder.add(queryCodeBlockBuilder.usingQueryVariableName(context.localVariable(query.name())).build());
			builder.add(queryExecutionBlockBuilder(context, queryMethod).forQuery(query).build());
			return builder.build();
		};
	}

}
