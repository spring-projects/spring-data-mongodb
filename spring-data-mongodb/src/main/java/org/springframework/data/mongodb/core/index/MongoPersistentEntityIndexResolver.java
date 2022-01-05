/*
 * Copyright 2014-2022 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.CycleGuard.Path;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.TextIndexIncludeOptions.IncludeStrategy;
import org.springframework.data.mongodb.core.index.TextIndexDefinition.TextIndexDefinitionBuilder;
import org.springframework.data.mongodb.core.index.TextIndexDefinition.TextIndexedFieldSpec;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.mongodb.util.DotPath;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * {@link IndexResolver} implementation inspecting {@link MongoPersistentEntity} for {@link MongoPersistentEntity} to be
 * indexed. <br />
 * All {@link MongoPersistentProperty} of the {@link MongoPersistentEntity} are inspected for potential indexes by
 * scanning related annotations.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Martin Macko
 * @author Mark Paluch
 * @author Dave Perryman
 * @since 1.5
 */
public class MongoPersistentEntityIndexResolver implements IndexResolver {

	private static final Log LOGGER = LogFactory.getLog(MongoPersistentEntityIndexResolver.class);
	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private EvaluationContextProvider evaluationContextProvider = EvaluationContextProvider.DEFAULT;

	/**
	 * Create new {@link MongoPersistentEntityIndexResolver}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 */
	public MongoPersistentEntityIndexResolver(
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		Assert.notNull(mappingContext, "Mapping context must not be null in order to resolve index definitions");
		this.mappingContext = mappingContext;
	}

	@Override
	public Iterable<? extends IndexDefinitionHolder> resolveIndexFor(TypeInformation<?> typeInformation) {
		return resolveIndexForEntity(mappingContext.getRequiredPersistentEntity(typeInformation));
	}

	/**
	 * Resolve the {@link IndexDefinition}s for a given {@literal root} entity by traversing
	 * {@link MongoPersistentProperty} scanning for index annotations {@link Indexed}, {@link CompoundIndex} and
	 * {@link GeospatialIndex}. The given {@literal root} has therefore to be annotated with {@link Document}.
	 *
	 * @param root must not be null.
	 * @return List of {@link IndexDefinitionHolder}. Will never be {@code null}.
	 * @throws IllegalArgumentException in case of missing {@link Document} annotation marking root entities.
	 */
	public List<IndexDefinitionHolder> resolveIndexForEntity(MongoPersistentEntity<?> root) {

		Assert.notNull(root, "MongoPersistentEntity must not be null!");
		Document document = root.findAnnotation(Document.class);
		Assert.notNull(document, () -> String
				.format("Entity %s is not a collection root. Make sure to annotate it with @Document!", root.getName()));

		verifyWildcardIndexedProjection(root);

		List<IndexDefinitionHolder> indexInformation = new ArrayList<>();
		String collection = root.getCollection();
		indexInformation.addAll(potentiallyCreateCompoundIndexDefinitions("", collection, root));
		indexInformation.addAll(potentiallyCreateWildcardIndexDefinitions("", collection, root));
		indexInformation.addAll(potentiallyCreateTextIndexDefinition(root, collection));

		root.doWithProperties((PropertyHandler<MongoPersistentProperty>) property -> this
				.potentiallyAddIndexForProperty(root, property, indexInformation, new CycleGuard()));

		indexInformation.addAll(resolveIndexesForDbrefs("", collection, root));

		return indexInformation;
	}

	private void verifyWildcardIndexedProjection(MongoPersistentEntity<?> entity) {

		entity.doWithAll(it -> {

			if (it.isAnnotationPresent(WildcardIndexed.class)) {

				WildcardIndexed indexed = it.getRequiredAnnotation(WildcardIndexed.class);

				if (!ObjectUtils.isEmpty(indexed.wildcardProjection())) {

					throw new MappingException(String.format(
							"WildcardIndexed.wildcardProjection cannot be used on nested paths. Offending property: %s.%s",
							entity.getName(), it.getName()));
				}
			}
		});
	}

	private void potentiallyAddIndexForProperty(MongoPersistentEntity<?> root, MongoPersistentProperty persistentProperty,
			List<IndexDefinitionHolder> indexes, CycleGuard guard) {

		try {
			if (isMapWithoutWildcardIndex(persistentProperty)) {
				return;
			}

			if (persistentProperty.isEntity()) {
				indexes.addAll(resolveIndexForEntity(mappingContext.getPersistentEntity(persistentProperty),
						persistentProperty.isUnwrapped() ? "" : persistentProperty.getFieldName(), Path.of(persistentProperty),
						root.getCollection(), guard));
			}

			List<IndexDefinitionHolder> indexDefinitions = createIndexDefinitionHolderForProperty(
					persistentProperty.getFieldName(), root.getCollection(), persistentProperty);
			if (!indexDefinitions.isEmpty()) {
				indexes.addAll(indexDefinitions);
			}
		} catch (CyclicPropertyReferenceException e) {
			if (LOGGER.isInfoEnabled()) {
				LOGGER.info(e.getMessage());
			}
		}
	}

	/**
	 * Recursively resolve and inspect properties of given {@literal type} for indexes to be created.
	 *
	 * @param type
	 * @param dotPath The {@literal "dot} path.
	 * @param path {@link PersistentProperty} path for cycle detection.
	 * @param collection
	 * @param guard
	 * @return List of {@link IndexDefinitionHolder} representing indexes for given type and its referenced property
	 *         types. Will never be {@code null}.
	 */
	private List<IndexDefinitionHolder> resolveIndexForClass(TypeInformation<?> type, String dotPath, Path path,
			String collection, CycleGuard guard) {

		return resolveIndexForEntity(mappingContext.getRequiredPersistentEntity(type), dotPath, path, collection, guard);
	}

	private List<IndexDefinitionHolder> resolveIndexForEntity(MongoPersistentEntity<?> entity, String dotPath, Path path,
			String collection, CycleGuard guard) {

		List<IndexDefinitionHolder> indexInformation = new ArrayList<>();
		indexInformation.addAll(potentiallyCreateCompoundIndexDefinitions(dotPath, collection, entity));
		indexInformation.addAll(potentiallyCreateWildcardIndexDefinitions(dotPath, collection, entity));

		entity.doWithProperties((PropertyHandler<MongoPersistentProperty>) property -> this
				.guardAndPotentiallyAddIndexForProperty(property, dotPath, path, collection, indexInformation, guard));

		indexInformation.addAll(resolveIndexesForDbrefs(dotPath, collection, entity));

		return indexInformation;
	}

	private void guardAndPotentiallyAddIndexForProperty(MongoPersistentProperty persistentProperty, String dotPath,
			Path path, String collection, List<IndexDefinitionHolder> indexes, CycleGuard guard) {

		DotPath propertyDotPath = DotPath.from(dotPath);

		if (!persistentProperty.isUnwrapped()) {
			propertyDotPath = propertyDotPath.append(persistentProperty.getFieldName());
		}

		Path propertyPath = path.append(persistentProperty);
		guard.protect(persistentProperty, propertyPath);

		if (isMapWithoutWildcardIndex(persistentProperty)) {
			return;
		}

		if (persistentProperty.isEntity()) {
			try {
				indexes.addAll(resolveIndexForEntity(mappingContext.getPersistentEntity(persistentProperty),
						propertyDotPath.toString(), propertyPath, collection, guard));
			} catch (CyclicPropertyReferenceException e) {
				LOGGER.info(e.getMessage());
			}
		}

		List<IndexDefinitionHolder> indexDefinitions = createIndexDefinitionHolderForProperty(propertyDotPath.toString(),
				collection, persistentProperty);

		if (!indexDefinitions.isEmpty()) {
			indexes.addAll(indexDefinitions);
		}
	}

	private List<IndexDefinitionHolder> createIndexDefinitionHolderForProperty(String dotPath, String collection,
			MongoPersistentProperty persistentProperty) {

		List<IndexDefinitionHolder> indices = new ArrayList<>(2);

		if (persistentProperty.isUnwrapped() && (persistentProperty.isAnnotationPresent(Indexed.class)
				|| persistentProperty.isAnnotationPresent(HashIndexed.class)
				|| persistentProperty.isAnnotationPresent(GeoSpatialIndexed.class))) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Index annotation not allowed on unwrapped object for path '%s'.", dotPath));
		}

		if (persistentProperty.isAnnotationPresent(Indexed.class)) {
			indices.add(createIndexDefinition(dotPath, collection, persistentProperty));
		} else if (persistentProperty.isAnnotationPresent(GeoSpatialIndexed.class)) {
			indices.add(createGeoSpatialIndexDefinition(dotPath, collection, persistentProperty));
		}

		if (persistentProperty.isAnnotationPresent(HashIndexed.class)) {
			indices.add(createHashedIndexDefinition(dotPath, collection, persistentProperty));
		}
		if (persistentProperty.isAnnotationPresent(WildcardIndexed.class)) {
			indices.add(createWildcardIndexDefinition(dotPath, collection,
					persistentProperty.getRequiredAnnotation(WildcardIndexed.class),
					mappingContext.getPersistentEntity(persistentProperty)));
		}

		return indices;
	}

	private List<IndexDefinitionHolder> potentiallyCreateCompoundIndexDefinitions(String dotPath, String collection,
			MongoPersistentEntity<?> entity) {

		if (entity.findAnnotation(CompoundIndexes.class) == null && entity.findAnnotation(CompoundIndex.class) == null) {
			return Collections.emptyList();
		}

		return createCompoundIndexDefinitions(dotPath, collection, entity);
	}

	private List<IndexDefinitionHolder> potentiallyCreateWildcardIndexDefinitions(String dotPath, String collection,
			MongoPersistentEntity<?> entity) {

		if (!entity.isAnnotationPresent(WildcardIndexed.class)) {
			return Collections.emptyList();
		}

		return Collections.singletonList(new IndexDefinitionHolder(dotPath,
				createWildcardIndexDefinition(dotPath, collection, entity.getRequiredAnnotation(WildcardIndexed.class), entity),
				collection));
	}

	private Collection<? extends IndexDefinitionHolder> potentiallyCreateTextIndexDefinition(
			MongoPersistentEntity<?> root, String collection) {

		String name = root.getType().getSimpleName() + "_TextIndex";
		if (name.getBytes().length > 127) {
			String[] args = ClassUtils.getShortNameAsProperty(root.getType()).split("\\.");
			name = "";
			Iterator<String> it = Arrays.asList(args).iterator();
			while (it.hasNext()) {

				if (!it.hasNext()) {
					name += it.next() + "_TextIndex";
				} else {
					name += (it.next().charAt(0) + ".");
				}
			}

		}
		TextIndexDefinitionBuilder indexDefinitionBuilder = new TextIndexDefinitionBuilder().named(name);

		if (StringUtils.hasText(root.getLanguage())) {
			indexDefinitionBuilder.withDefaultLanguage(root.getLanguage());
		}

		try {
			appendTextIndexInformation(DotPath.empty(), Path.empty(), indexDefinitionBuilder, root,
					new TextIndexIncludeOptions(IncludeStrategy.DEFAULT), new CycleGuard());
		} catch (CyclicPropertyReferenceException e) {
			LOGGER.info(e.getMessage());
		}

		if (root.hasCollation()) {
			indexDefinitionBuilder.withSimpleCollation();
		}

		TextIndexDefinition indexDefinition = indexDefinitionBuilder.build();

		if (!indexDefinition.hasFieldSpec()) {
			return Collections.emptyList();
		}

		IndexDefinitionHolder holder = new IndexDefinitionHolder("", indexDefinition, collection);
		return Collections.singletonList(holder);

	}

	private void appendTextIndexInformation(DotPath dotPath, Path path, TextIndexDefinitionBuilder indexDefinitionBuilder,
			MongoPersistentEntity<?> entity, TextIndexIncludeOptions includeOptions, CycleGuard guard) {

		entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {

				guard.protect(persistentProperty, path);

				if (persistentProperty.isExplicitLanguageProperty() && dotPath.isEmpty()) {
					indexDefinitionBuilder.withLanguageOverride(persistentProperty.getFieldName());
				}

				if (persistentProperty.isMap()) {
					return;
				}

				TextIndexed indexed = persistentProperty.findAnnotation(TextIndexed.class);

				if (includeOptions.isForce() || indexed != null || persistentProperty.isEntity()) {

					DotPath propertyDotPath = dotPath.append(persistentProperty.getFieldName());

					Path propertyPath = path.append(persistentProperty);

					TextIndexedFieldSpec parentFieldSpec = includeOptions.getParentFieldSpec();
					Float weight = indexed != null ? indexed.weight()
							: (parentFieldSpec != null ? parentFieldSpec.getWeight() : 1.0F);

					if (persistentProperty.isEntity()) {

						TextIndexIncludeOptions optionsForNestedType = includeOptions;
						if (!IncludeStrategy.FORCE.equals(includeOptions.getStrategy()) && indexed != null) {
							optionsForNestedType = new TextIndexIncludeOptions(IncludeStrategy.FORCE,
									new TextIndexedFieldSpec(propertyDotPath.toString(), weight));
						}

						try {
							appendTextIndexInformation(propertyDotPath, propertyPath, indexDefinitionBuilder,
									mappingContext.getPersistentEntity(persistentProperty.getActualType()), optionsForNestedType, guard);
						} catch (CyclicPropertyReferenceException e) {
							LOGGER.info(e.getMessage());
						} catch (InvalidDataAccessApiUsageException e) {
							LOGGER.info(String.format("Potentially invalid index structure discovered. Breaking operation for %s.",
									entity.getName()), e);
						}
					} else if (includeOptions.isForce() || indexed != null) {
						indexDefinitionBuilder.onField(propertyDotPath.toString(), weight);
					}
				}

			}
		});

	}

	/**
	 * Create {@link IndexDefinition} wrapped in {@link IndexDefinitionHolder} for {@link CompoundIndexes} of a given
	 * type.
	 *
	 * @param dotPath The properties {@literal "dot"} path representation from its document root.
	 * @param fallbackCollection
	 * @param entity
	 * @return
	 */
	protected List<IndexDefinitionHolder> createCompoundIndexDefinitions(String dotPath, String fallbackCollection,
			MongoPersistentEntity<?> entity) {

		List<IndexDefinitionHolder> indexDefinitions = new ArrayList<>();
		CompoundIndexes indexes = entity.findAnnotation(CompoundIndexes.class);

		if (indexes != null) {
			indexDefinitions = Arrays.stream(indexes.value())
					.map(index -> createCompoundIndexDefinition(dotPath, fallbackCollection, index, entity))
					.collect(Collectors.toList());
		}

		CompoundIndex index = entity.findAnnotation(CompoundIndex.class);

		if (index != null) {
			indexDefinitions.add(createCompoundIndexDefinition(dotPath, fallbackCollection, index, entity));
		}

		return indexDefinitions;
	}

	protected IndexDefinitionHolder createCompoundIndexDefinition(String dotPath, String collection, CompoundIndex index,
			MongoPersistentEntity<?> entity) {

		CompoundIndexDefinition indexDefinition = new CompoundIndexDefinition(
				resolveCompoundIndexKeyFromStringDefinition(dotPath, index.def(), entity));

		if (!index.useGeneratedName()) {
			indexDefinition.named(pathAwareIndexName(index.name(), dotPath, entity, null));
		}

		if (index.unique()) {
			indexDefinition.unique();
		}

		if (index.sparse()) {
			indexDefinition.sparse();
		}

		if (index.background()) {
			indexDefinition.background();
		}

		if (StringUtils.hasText(index.partialFilter())) {
			indexDefinition.partial(evaluatePartialFilter(index.partialFilter(), entity));
		}

		return new IndexDefinitionHolder(dotPath, indexDefinition, collection);
	}

	protected IndexDefinitionHolder createWildcardIndexDefinition(String dotPath, String collection,
			WildcardIndexed index, @Nullable MongoPersistentEntity<?> entity) {

		WildcardIndex indexDefinition = new WildcardIndex(dotPath);

		if (StringUtils.hasText(index.wildcardProjection()) && ObjectUtils.isEmpty(dotPath)) {
			indexDefinition.wildcardProjection(evaluateWildcardProjection(index.wildcardProjection(), entity));
		}

		if (!index.useGeneratedName()) {
			indexDefinition.named(pathAwareIndexName(index.name(), dotPath, entity, null));
		}

		if (StringUtils.hasText(index.partialFilter())) {
			indexDefinition.partial(evaluatePartialFilter(index.partialFilter(), entity));
		}

		if (StringUtils.hasText(index.collation())) {
			indexDefinition.collation(evaluateCollation(index.collation(), entity));
		} else if (entity != null && entity.hasCollation()) {
			indexDefinition.collation(entity.getCollation());
		}

		return new IndexDefinitionHolder(dotPath, indexDefinition, collection);
	}

	private org.bson.Document resolveCompoundIndexKeyFromStringDefinition(String dotPath, String keyDefinitionString,
			PersistentEntity<?, ?> entity) {

		if (!StringUtils.hasText(dotPath) && !StringUtils.hasText(keyDefinitionString)) {
			throw new InvalidDataAccessApiUsageException("Cannot create index on root level for empty keys.");
		}

		if (!StringUtils.hasText(keyDefinitionString)) {
			return new org.bson.Document(dotPath, 1);
		}

		Object keyDefToUse = evaluate(keyDefinitionString, getEvaluationContextForProperty(entity));

		org.bson.Document dbo = (keyDefToUse instanceof org.bson.Document) ? (org.bson.Document) keyDefToUse
				: org.bson.Document.parse(ObjectUtils.nullSafeToString(keyDefToUse));

		if (!StringUtils.hasText(dotPath)) {
			return dbo;
		}

		org.bson.Document document = new org.bson.Document();

		for (String key : dbo.keySet()) {
			document.put(dotPath + "." + key, dbo.get(key));
		}
		return document;
	}

	/**
	 * Creates {@link IndexDefinition} wrapped in {@link IndexDefinitionHolder} out of {@link Indexed} for a given
	 * {@link MongoPersistentProperty}.
	 *
	 * @param dotPath The properties {@literal "dot"} path representation from its document root.
	 * @param collection
	 * @param persistentProperty
	 * @return
	 */
	@Nullable
	protected IndexDefinitionHolder createIndexDefinition(String dotPath, String collection,
			MongoPersistentProperty persistentProperty) {

		Indexed index = persistentProperty.findAnnotation(Indexed.class);

		if (index == null) {
			return null;
		}

		Index indexDefinition = new Index().on(dotPath,
				IndexDirection.ASCENDING.equals(index.direction()) ? Sort.Direction.ASC : Sort.Direction.DESC);

		if (!index.useGeneratedName()) {
			indexDefinition
					.named(pathAwareIndexName(index.name(), dotPath, persistentProperty.getOwner(), persistentProperty));
		}

		if (index.unique()) {
			indexDefinition.unique();
		}

		if (index.sparse()) {
			indexDefinition.sparse();
		}

		if (index.background()) {
			indexDefinition.background();
		}

		if (index.expireAfterSeconds() >= 0) {
			indexDefinition.expire(index.expireAfterSeconds(), TimeUnit.SECONDS);
		}

		if (StringUtils.hasText(index.expireAfter())) {

			if (index.expireAfterSeconds() >= 0) {
				throw new IllegalStateException(String.format(
						"@Indexed already defines an expiration timeout of %s seconds via Indexed#expireAfterSeconds. Please make to use either expireAfterSeconds or expireAfter.",
						index.expireAfterSeconds()));
			}

			Duration timeout = computeIndexTimeout(index.expireAfter(),
					getEvaluationContextForProperty(persistentProperty.getOwner()));
			if (!timeout.isZero() && !timeout.isNegative()) {
				indexDefinition.expire(timeout);
			}
		}

		if (StringUtils.hasText(index.partialFilter())) {
			indexDefinition.partial(evaluatePartialFilter(index.partialFilter(), persistentProperty.getOwner()));
		}

		return new IndexDefinitionHolder(dotPath, indexDefinition, collection);
	}

	private PartialIndexFilter evaluatePartialFilter(String filterExpression, PersistentEntity<?, ?> entity) {

		Object result = evaluate(filterExpression, getEvaluationContextForProperty(entity));

		if (result instanceof org.bson.Document) {
			return PartialIndexFilter.of((org.bson.Document) result);
		}

		return PartialIndexFilter.of(BsonUtils.parse(filterExpression, null));
	}

	private org.bson.Document evaluateWildcardProjection(String projectionExpression, PersistentEntity<?, ?> entity) {

		Object result = evaluate(projectionExpression, getEvaluationContextForProperty(entity));

		if (result instanceof org.bson.Document) {
			return (org.bson.Document) result;
		}

		return BsonUtils.parse(projectionExpression, null);
	}

	private Collation evaluateCollation(String collationExpression, PersistentEntity<?, ?> entity) {

		Object result = evaluate(collationExpression, getEvaluationContextForProperty(entity));
		if (result instanceof org.bson.Document) {
			return Collation.from((org.bson.Document) result);
		}
		if (result instanceof Collation) {
			return (Collation) result;
		}
		if (result instanceof String) {
			return Collation.parse(result.toString());
		}
		throw new IllegalStateException("Cannot parse collation " + result);

	}

	/**
	 * Creates {@link HashedIndex} wrapped in {@link IndexDefinitionHolder} out of {@link HashIndexed} for a given
	 * {@link MongoPersistentProperty}.
	 *
	 * @param dotPath The properties {@literal "dot"} path representation from its document root.
	 * @param collection
	 * @param persistentProperty
	 * @return
	 * @since 2.2
	 */
	@Nullable
	protected IndexDefinitionHolder createHashedIndexDefinition(String dotPath, String collection,
			MongoPersistentProperty persistentProperty) {

		HashIndexed index = persistentProperty.findAnnotation(HashIndexed.class);

		if (index == null) {
			return null;
		}

		return new IndexDefinitionHolder(dotPath, HashedIndex.hashed(dotPath), collection);
	}

	/**
	 * Get the default {@link EvaluationContext}.
	 *
	 * @return never {@literal null}.
	 * @since 2.2
	 */
	protected EvaluationContext getEvaluationContext() {
		return evaluationContextProvider.getEvaluationContext(null);
	}

	/**
	 * Get the {@link EvaluationContext} for a given {@link PersistentEntity entity} the default one.
	 *
	 * @param persistentEntity can be {@literal null}
	 * @return
	 */
	private EvaluationContext getEvaluationContextForProperty(@Nullable PersistentEntity<?, ?> persistentEntity) {

		if (persistentEntity == null || !(persistentEntity instanceof BasicMongoPersistentEntity)) {
			return getEvaluationContext();
		}

		EvaluationContext contextFromEntity = ((BasicMongoPersistentEntity<?>) persistentEntity).getEvaluationContext(null);

		if (contextFromEntity != null && !EvaluationContextProvider.DEFAULT.equals(contextFromEntity)) {
			return contextFromEntity;
		}

		return getEvaluationContext();
	}

	/**
	 * Set the {@link EvaluationContextProvider} used for obtaining the {@link EvaluationContext} used to compute
	 * {@link org.springframework.expression.spel.standard.SpelExpression expressions}.
	 *
	 * @param evaluationContextProvider must not be {@literal null}.
	 * @since 2.2
	 */
	public void setEvaluationContextProvider(EvaluationContextProvider evaluationContextProvider) {
		this.evaluationContextProvider = evaluationContextProvider;
	}

	/**
	 * Creates {@link IndexDefinition} wrapped in {@link IndexDefinitionHolder} out of {@link GeoSpatialIndexed} for
	 * {@link MongoPersistentProperty}.
	 *
	 * @param dotPath The properties {@literal "dot"} path representation from its document root.
	 * @param collection
	 * @param persistentProperty
	 * @return
	 */
	@Nullable
	protected IndexDefinitionHolder createGeoSpatialIndexDefinition(String dotPath, String collection,
			MongoPersistentProperty persistentProperty) {

		GeoSpatialIndexed index = persistentProperty.findAnnotation(GeoSpatialIndexed.class);

		if (index == null) {
			return null;
		}

		GeospatialIndex indexDefinition = new GeospatialIndex(dotPath);
		indexDefinition.withBits(index.bits());
		indexDefinition.withMin(index.min()).withMax(index.max());

		if (!index.useGeneratedName()) {
			indexDefinition
					.named(pathAwareIndexName(index.name(), dotPath, persistentProperty.getOwner(), persistentProperty));
		}

		indexDefinition.typed(index.type()).withBucketSize(index.bucketSize()).withAdditionalField(index.additionalField());

		return new IndexDefinitionHolder(dotPath, indexDefinition, collection);
	}

	private String pathAwareIndexName(String indexName, String dotPath, @Nullable PersistentEntity<?, ?> entity,
			@Nullable MongoPersistentProperty property) {

		String nameToUse = "";
		if (StringUtils.hasText(indexName)) {

			Object result = evaluate(indexName, getEvaluationContextForProperty(entity));

			if (result != null) {
				nameToUse = ObjectUtils.nullSafeToString(result);
			}
		}

		if (!StringUtils.hasText(dotPath) || (property != null && dotPath.equals(property.getFieldName()))) {
			return StringUtils.hasText(nameToUse) ? nameToUse : dotPath;
		}

		if (StringUtils.hasText(dotPath)) {

			nameToUse = StringUtils.hasText(nameToUse)
					? (property != null ? dotPath.replace("." + property.getFieldName(), "") : dotPath) + "." + nameToUse
					: dotPath;
		}
		return nameToUse;

	}

	private List<IndexDefinitionHolder> resolveIndexesForDbrefs(final String path, final String collection,
			MongoPersistentEntity<?> entity) {

		final List<IndexDefinitionHolder> indexes = new ArrayList<>(0);
		entity.doWithAssociations((AssociationHandler<MongoPersistentProperty>) association -> this
				.resolveAndAddIndexesForAssociation(association, indexes, path, collection));
		return indexes;
	}

	private void resolveAndAddIndexesForAssociation(Association<MongoPersistentProperty> association,
			List<IndexDefinitionHolder> indexes, String path, String collection) {

		MongoPersistentProperty property = association.getInverse();

		DotPath propertyDotPath = DotPath.from(path).append(property.getFieldName());

		if (property.isAnnotationPresent(GeoSpatialIndexed.class) || property.isAnnotationPresent(TextIndexed.class)) {
			throw new MappingException(
					String.format("Cannot create geospatial-/text- index on DBRef in collection '%s' for path '%s'.", collection,
							propertyDotPath));
		}

		List<IndexDefinitionHolder> indexDefinitions = createIndexDefinitionHolderForProperty(propertyDotPath.toString(),
				collection, property);

		if (!indexDefinitions.isEmpty()) {
			indexes.addAll(indexDefinitions);
		}
	}

	/**
	 * Compute the index timeout value by evaluating a potential
	 * {@link org.springframework.expression.spel.standard.SpelExpression} and parsing the final value.
	 *
	 * @param timeoutValue must not be {@literal null}.
	 * @param evaluationContext must not be {@literal null}.
	 * @return never {@literal null}
	 * @since 2.2
	 * @throws IllegalArgumentException for invalid duration values.
	 */
	private static Duration computeIndexTimeout(String timeoutValue, EvaluationContext evaluationContext) {

		Object evaluatedTimeout = evaluate(timeoutValue, evaluationContext);

		if (evaluatedTimeout == null) {
			return Duration.ZERO;
		}

		if (evaluatedTimeout instanceof Duration) {
			return (Duration) evaluatedTimeout;
		}

		String val = evaluatedTimeout.toString();

		if (val == null) {
			return Duration.ZERO;
		}

		return DurationStyle.detectAndParse(val);
	}

	@Nullable
	private static Object evaluate(String value, EvaluationContext evaluationContext) {

		Expression expression = PARSER.parseExpression(value, ParserContext.TEMPLATE_EXPRESSION);
		if (expression instanceof LiteralExpression) {
			return value;
		}

		return expression.getValue(evaluationContext, Object.class);
	}

	private static boolean isMapWithoutWildcardIndex(MongoPersistentProperty property) {
		return property.isMap() && !property.isAnnotationPresent(WildcardIndexed.class);
	}

	/**
	 * {@link CycleGuard} holds information about properties and the paths for accessing those. This information is used
	 * to detect potential cycles within the references.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 */
	static class CycleGuard {

		private final Set<String> seenProperties = new HashSet<>();

		/**
		 * Detect a cycle in a property path if the property was seen at least once.
		 *
		 * @param property The property to inspect
		 * @param path The type path under which the property can be reached.
		 * @throws CyclicPropertyReferenceException in case a potential cycle is detected.
		 * @see Path#isCycle()
		 */
		void protect(MongoPersistentProperty property, Path path) throws CyclicPropertyReferenceException {

			String propertyTypeKey = createMapKey(property);
			if (!seenProperties.add(propertyTypeKey)) {

				if (path.isCycle()) {
					throw new CyclicPropertyReferenceException(property.getFieldName(), property.getOwner().getType(),
							path.toCyclePath());
				}
			}
		}

		private String createMapKey(MongoPersistentProperty property) {
			return ClassUtils.getShortName(property.getOwner().getType()) + ":" + property.getFieldName();
		}

		/**
		 * Path defines the full property path from the document root. <br />
		 * A {@link Path} with {@literal spring.data.mongodb} would be created for the property {@code Three.mongodb}.
		 *
		 * <pre>
		 * <code>
		 * &#64;Document
		 * class One {
		 *   Two spring;
		 * }
		 *
		 * class Two {
		 *   Three data;
		 * }
		 *
		 * class Three {
		 *   String mongodb;
		 * }
		 * </code>
		 * </pre>
		 *
		 * @author Christoph Strobl
		 * @author Mark Paluch
		 */
		static class Path {

			private static final Path EMPTY = new Path(Collections.emptyList(), false);

			private final List<PersistentProperty<?>> elements;
			private final boolean cycle;

			private Path(List<PersistentProperty<?>> elements, boolean cycle) {
				this.elements = elements;
				this.cycle = cycle;
			}

			/**
			 * @return an empty {@link Path}.
			 * @since 1.10.8
			 */
			static Path empty() {
				return EMPTY;
			}

			/**
			 * Creates a new {@link Path} from the initial {@link PersistentProperty}.
			 *
			 * @param initial must not be {@literal null}.
			 * @return the new {@link Path}.
			 * @since 1.10.8
			 */
			static Path of(PersistentProperty<?> initial) {
				return new Path(Collections.singletonList(initial), false);
			}

			/**
			 * Creates a new {@link Path} by appending a {@link PersistentProperty breadcrumb} to the path.
			 *
			 * @param breadcrumb must not be {@literal null}.
			 * @return the new {@link Path}.
			 * @since 1.10.8
			 */
			Path append(PersistentProperty<?> breadcrumb) {

				List<PersistentProperty<?>> elements = new ArrayList<>(this.elements.size() + 1);
				elements.addAll(this.elements);
				elements.add(breadcrumb);

				return new Path(elements, this.elements.contains(breadcrumb));
			}

			/**
			 * @return {@literal true} if a cycle was detected.
			 * @since 1.10.8
			 */
			public boolean isCycle() {
				return cycle;
			}

			@Override
			public String toString() {
				return this.elements.isEmpty() ? "(empty)" : toPath(this.elements.iterator());
			}

			/**
			 * Returns the cycle path truncated to the first discovered cycle. The result for the path
			 * {@literal foo.bar.baz.bar} is {@literal bar -> baz -> bar}.
			 *
			 * @return the cycle path truncated to the first discovered cycle.
			 * @since 1.10.8
			 */
			String toCyclePath() {

				if (!cycle) {
					return "";
				}

				for (int i = 0; i < this.elements.size(); i++) {

					int index = indexOf(this.elements, this.elements.get(i), i + 1);

					if (index != -1) {
						return toPath(this.elements.subList(i, index + 1).iterator());
					}
				}

				return toString();
			}

			private static <T> int indexOf(List<T> haystack, T needle, int offset) {

				for (int i = offset; i < haystack.size(); i++) {
					if (haystack.get(i).equals(needle)) {
						return i;
					}
				}

				return -1;
			}

			private static String toPath(Iterator<PersistentProperty<?>> iterator) {

				StringBuilder builder = new StringBuilder();
				while (iterator.hasNext()) {

					builder.append(iterator.next().getName());
					if (iterator.hasNext()) {
						builder.append(" -> ");
					}
				}

				return builder.toString();
			}

			@Override
			public boolean equals(Object o) {
				if (this == o)
					return true;
				if (o == null || getClass() != o.getClass())
					return false;

				Path that = (Path) o;

				if (this.cycle != that.cycle) {
					return false;
				}
				return ObjectUtils.nullSafeEquals(this.elements, that.elements);
			}

			@Override
			public int hashCode() {
				int result = ObjectUtils.nullSafeHashCode(elements);
				result = 31 * result + (cycle ? 1 : 0);
				return result;
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	public static class CyclicPropertyReferenceException extends RuntimeException {

		private static final long serialVersionUID = -3762979307658772277L;

		private final String propertyName;
		private final @Nullable Class<?> type;
		private final String dotPath;

		public CyclicPropertyReferenceException(String propertyName, @Nullable Class<?> type, String dotPath) {

			this.propertyName = propertyName;
			this.type = type;
			this.dotPath = dotPath;
		}

		@Override
		public String getMessage() {
			return String.format("Found cycle for field '%s' in type '%s' for path '%s'", propertyName,
					type != null ? type.getSimpleName() : "unknown", dotPath);
		}
	}

	/**
	 * Implementation of {@link IndexDefinition} holding additional (property)path information used for creating the
	 * index. The path itself is the properties {@literal "dot"} path representation from its root document.
	 *
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	public static class IndexDefinitionHolder implements IndexDefinition {

		private final String path;
		private final IndexDefinition indexDefinition;
		private final String collection;

		/**
		 * Create
		 *
		 * @param path
		 */
		public IndexDefinitionHolder(String path, IndexDefinition definition, String collection) {

			this.path = path;
			this.indexDefinition = definition;
			this.collection = collection;
		}

		public String getCollection() {
			return collection;
		}

		/**
		 * Get the {@literal "dot"} path used to create the index.
		 *
		 * @return
		 */
		public String getPath() {
			return path;
		}

		/**
		 * Get the {@literal raw} {@link IndexDefinition}.
		 *
		 * @return
		 */
		public IndexDefinition getIndexDefinition() {
			return indexDefinition;
		}

		@Override
		public org.bson.Document getIndexKeys() {
			return indexDefinition.getIndexKeys();
		}

		@Override
		public org.bson.Document getIndexOptions() {
			return indexDefinition.getIndexOptions();
		}

		@Override
		public String toString() {
			return "IndexDefinitionHolder{" + "indexKeys=" + getIndexKeys() + '}';
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.6
	 */
	static class TextIndexIncludeOptions {

		enum IncludeStrategy {
			FORCE, DEFAULT;
		}

		private final IncludeStrategy strategy;

		private final @Nullable TextIndexedFieldSpec parentFieldSpec;

		public TextIndexIncludeOptions(IncludeStrategy strategy, @Nullable TextIndexedFieldSpec parentFieldSpec) {
			this.strategy = strategy;
			this.parentFieldSpec = parentFieldSpec;
		}

		public TextIndexIncludeOptions(IncludeStrategy strategy) {
			this(strategy, null);
		}

		public IncludeStrategy getStrategy() {
			return strategy;
		}

		@Nullable
		public TextIndexedFieldSpec getParentFieldSpec() {
			return parentFieldSpec;
		}

		public boolean isForce() {
			return IncludeStrategy.FORCE.equals(strategy);
		}

	}
}
