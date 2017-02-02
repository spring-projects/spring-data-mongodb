/*
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.TextIndexIncludeOptions.IncludeStrategy;
import org.springframework.data.mongodb.core.index.TextIndexDefinition.TextIndexDefinitionBuilder;
import org.springframework.data.mongodb.core.index.TextIndexDefinition.TextIndexedFieldSpec;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * {@link IndexResolver} implementation inspecting {@link MongoPersistentEntity} for {@link MongoPersistentEntity} to be
 * indexed. <br />
 * All {@link MongoPersistentProperty} of the {@link MongoPersistentEntity} are inspected for potential indexes by
 * scanning related annotations.
 * 
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @since 1.5
 */
public class MongoPersistentEntityIndexResolver implements IndexResolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoPersistentEntityIndexResolver.class);

	private final MongoMappingContext mappingContext;

	/**
	 * Create new {@link MongoPersistentEntityIndexResolver}.
	 * 
	 * @param mappingContext must not be {@literal null}.
	 */
	public MongoPersistentEntityIndexResolver(MongoMappingContext mappingContext) {

		Assert.notNull(mappingContext, "Mapping context must not be null in order to resolve index definitions");
		this.mappingContext = mappingContext;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.index.IndexResolver#resolveIndexForClass(org.springframework.data.util.TypeInformation)
	 */
	@Override
	public Iterable<? extends IndexDefinitionHolder> resolveIndexFor(TypeInformation<?> typeInformation) {
		return resolveIndexForEntity(mappingContext.getRequiredPersistentEntity(typeInformation));
	}

	/**
	 * Resolve the {@link IndexDefinition}s for given {@literal root} entity by traversing {@link MongoPersistentProperty}
	 * scanning for index annotations {@link Indexed}, {@link CompoundIndex} and {@link GeospatialIndex}. The given
	 * {@literal root} has therefore to be annotated with {@link Document}.
	 * 
	 * @param root must not be null.
	 * @return List of {@link IndexDefinitionHolder}. Will never be {@code null}.
	 * @throws IllegalArgumentException in case of missing {@link Document} annotation marking root entities.
	 */
	public List<IndexDefinitionHolder> resolveIndexForEntity(final MongoPersistentEntity<?> root) {

		Assert.notNull(root, "Index cannot be resolved for given 'null' entity.");
		Document document = root.findAnnotation(Document.class).orElseThrow(() -> new IllegalArgumentException("Given entity is not collection root."));
		Assert.notNull(document, "Given entity is not collection root.");

		final List<IndexDefinitionHolder> indexInformation = new ArrayList<MongoPersistentEntityIndexResolver.IndexDefinitionHolder>();
		indexInformation.addAll(potentiallyCreateCompoundIndexDefinitions("", root.getCollection(), root));
		indexInformation.addAll(potentiallyCreateTextIndexDefinition(root));

		final CycleGuard guard = new CycleGuard();

		root.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {

				try {
					if (persistentProperty.isEntity()) {
						indexInformation.addAll(resolveIndexForClass(persistentProperty.getTypeInformation().getActualType(),
								persistentProperty.getFieldName(), root.getCollection(), guard));
					}

					IndexDefinitionHolder indexDefinitionHolder = createIndexDefinitionHolderForProperty(
							persistentProperty.getFieldName(), root.getCollection(), persistentProperty);
					if (indexDefinitionHolder != null) {
						indexInformation.add(indexDefinitionHolder);
					}
				} catch (CyclicPropertyReferenceException e) {
					LOGGER.info(e.getMessage());
				}
			}
		});

		indexInformation.addAll(resolveIndexesForDbrefs("", root.getCollection(), root));

		return indexInformation;
	}

	/**
	 * Recursively resolve and inspect properties of given {@literal type} for indexes to be created.
	 * 
	 * @param type
	 * @param path The {@literal "dot} path.
	 * @param collection
	 * @return List of {@link IndexDefinitionHolder} representing indexes for given type and its referenced property
	 *         types. Will never be {@code null}.
	 */
	private List<IndexDefinitionHolder> resolveIndexForClass(final TypeInformation<?> type, final String path,
			final String collection, final CycleGuard guard) {

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);

		final List<IndexDefinitionHolder> indexInformation = new ArrayList<MongoPersistentEntityIndexResolver.IndexDefinitionHolder>();
		indexInformation.addAll(potentiallyCreateCompoundIndexDefinitions(path, collection, entity));

		entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {

				String propertyDotPath = (StringUtils.hasText(path) ? path + "." : "") + persistentProperty.getFieldName();
				guard.protect(persistentProperty, path);

				if (persistentProperty.isEntity()) {
					try {
						indexInformation.addAll(resolveIndexForClass(persistentProperty.getTypeInformation().getActualType(),
								propertyDotPath, collection, guard));
					} catch (CyclicPropertyReferenceException e) {
						LOGGER.info(e.getMessage());
					}
				}

				IndexDefinitionHolder indexDefinitionHolder = createIndexDefinitionHolderForProperty(propertyDotPath,
						collection, persistentProperty);
				if (indexDefinitionHolder != null) {
					indexInformation.add(indexDefinitionHolder);
				}
			}
		});

		indexInformation.addAll(resolveIndexesForDbrefs(path, collection, entity));

		return indexInformation;
	}

	private IndexDefinitionHolder createIndexDefinitionHolderForProperty(String dotPath, String collection,
			MongoPersistentProperty persistentProperty) {

		if (persistentProperty.isAnnotationPresent(Indexed.class)) {
			return createIndexDefinition(dotPath, collection, persistentProperty);
		} else if (persistentProperty.isAnnotationPresent(GeoSpatialIndexed.class)) {
			return createGeoSpatialIndexDefinition(dotPath, collection, persistentProperty);
		}

		return null;
	}

	private List<IndexDefinitionHolder> potentiallyCreateCompoundIndexDefinitions(String dotPath, String collection,
			MongoPersistentEntity<?> entity) {

		if (entity.findAnnotation(CompoundIndexes.class) == null && entity.findAnnotation(CompoundIndex.class) == null) {
			return Collections.emptyList();
		}

		return createCompoundIndexDefinitions(dotPath, collection, entity);
	}

	private Collection<? extends IndexDefinitionHolder> potentiallyCreateTextIndexDefinition(
			MongoPersistentEntity<?> root) {

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
			appendTextIndexInformation("", indexDefinitionBuilder, root, new TextIndexIncludeOptions(IncludeStrategy.DEFAULT),
					new CycleGuard());
		} catch (CyclicPropertyReferenceException e) {
			LOGGER.info(e.getMessage());
		}

		TextIndexDefinition indexDefinition = indexDefinitionBuilder.build();

		if (!indexDefinition.hasFieldSpec()) {
			return Collections.emptyList();
		}

		IndexDefinitionHolder holder = new IndexDefinitionHolder("", indexDefinition, root.getCollection());
		return Collections.singletonList(holder);

	}

	private void appendTextIndexInformation(final String dotPath, final TextIndexDefinitionBuilder indexDefinitionBuilder,
			final MongoPersistentEntity<?> entity, final TextIndexIncludeOptions includeOptions, final CycleGuard guard) {

		entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {

				guard.protect(persistentProperty, dotPath);

				if (persistentProperty.isExplicitLanguageProperty() && !StringUtils.hasText(dotPath)) {
					indexDefinitionBuilder.withLanguageOverride(persistentProperty.getFieldName());
				}

				Optional<TextIndexed> indexed = persistentProperty.findAnnotation(TextIndexed.class);

				if (includeOptions.isForce() || indexed.isPresent()|| persistentProperty.isEntity()) {

					String propertyDotPath = (StringUtils.hasText(dotPath) ? dotPath + "." : "")
							+ persistentProperty.getFieldName();

					Float weight = indexed.isPresent() ? indexed.get().weight()
							: (includeOptions.getParentFieldSpec() != null ? includeOptions.getParentFieldSpec().getWeight() : 1.0F);

					if (persistentProperty.isEntity()) {

						TextIndexIncludeOptions optionsForNestedType = includeOptions;
						if (!IncludeStrategy.FORCE.equals(includeOptions.getStrategy()) && indexed.isPresent()) {
							optionsForNestedType = new TextIndexIncludeOptions(IncludeStrategy.FORCE,
									new TextIndexedFieldSpec(propertyDotPath, weight));
						}

						try {
							appendTextIndexInformation(propertyDotPath, indexDefinitionBuilder,
									mappingContext.getRequiredPersistentEntity(persistentProperty.getActualType()), optionsForNestedType, guard);
						} catch (CyclicPropertyReferenceException e) {
							LOGGER.info(e.getMessage(), e);
						} catch (InvalidDataAccessApiUsageException e) {
							LOGGER.info(String.format("Potentially invalid index structure discovered. Breaking operation for %s.",
									entity.getName()), e);
						}
					} else if (includeOptions.isForce() || indexed.isPresent()) {
						indexDefinitionBuilder.onField(propertyDotPath, weight);
					}
				}

			}
		});

	}

	/**
	 * Create {@link IndexDefinition} wrapped in {@link IndexDefinitionHolder} for {@link CompoundIndexes} of given type.
	 * 
	 * @param dotPath The properties {@literal "dot"} path representation from its document root.
	 * @param fallbackCollection
	 * @param type
	 * @return
	 */
	protected List<IndexDefinitionHolder> createCompoundIndexDefinitions(String dotPath, String fallbackCollection,
			MongoPersistentEntity<?> entity) {

		List<IndexDefinitionHolder> indexDefinitions = new ArrayList<MongoPersistentEntityIndexResolver.IndexDefinitionHolder>();
		Optional<CompoundIndexes> indexes = entity.findAnnotation(CompoundIndexes.class);

		if (indexes.isPresent()) {
			for (CompoundIndex index : indexes.get().value()) {
				indexDefinitions.add(createCompoundIndexDefinition(dotPath, fallbackCollection, index, entity));
			}
		}

		Optional<CompoundIndex> index = entity.findAnnotation(CompoundIndex.class);

		if (index.isPresent()) {
			indexDefinitions.add(createCompoundIndexDefinition(dotPath, fallbackCollection, index.get(), entity));
		}

		return indexDefinitions;
	}

	@SuppressWarnings("deprecation")
	protected IndexDefinitionHolder createCompoundIndexDefinition(String dotPath, String collection,
			CompoundIndex index, MongoPersistentEntity<?> entity) {

		CompoundIndexDefinition indexDefinition = new CompoundIndexDefinition(
				resolveCompoundIndexKeyFromStringDefinition(dotPath, index.def()));

		if (!index.useGeneratedName()) {
			indexDefinition.named(pathAwareIndexName(index.name(), dotPath, null));
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

		return new IndexDefinitionHolder(dotPath, indexDefinition, collection);
	}

	private org.bson.Document resolveCompoundIndexKeyFromStringDefinition(String dotPath, String keyDefinitionString) {

		if (!StringUtils.hasText(dotPath) && !StringUtils.hasText(keyDefinitionString)) {
			throw new InvalidDataAccessApiUsageException("Cannot create index on root level for empty keys.");
		}

		if (!StringUtils.hasText(keyDefinitionString)) {
			return new org.bson.Document(dotPath, 1);
		}

		org.bson.Document dbo = org.bson.Document.parse(keyDefinitionString);
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
	 * Creates {@link IndexDefinition} wrapped in {@link IndexDefinitionHolder} out of {@link Indexed} for given
	 * {@link MongoPersistentProperty}.
	 * 
	 * @param dotPath The properties {@literal "dot"} path representation from its document root.
	 * @param collection
	 * @param persitentProperty
	 * @return
	 */
	protected IndexDefinitionHolder createIndexDefinition(String dotPath, String collection,
			MongoPersistentProperty persitentProperty) {

		Optional<Indexed> index = persitentProperty.findAnnotation(Indexed.class);

		if(!index.isPresent()){
			return null;
		}

		Index indexDefinition = new Index().on(dotPath,
				IndexDirection.ASCENDING.equals(index.get().direction()) ? Sort.Direction.ASC : Sort.Direction.DESC);

		if (!index.get().useGeneratedName()) {
			indexDefinition.named(pathAwareIndexName(index.get().name(), dotPath, persitentProperty));
		}

		if (index.get().unique()) {
			indexDefinition.unique();
		}

		if (index.get().sparse()) {
			indexDefinition.sparse();
		}

		if (index.get().background()) {
			indexDefinition.background();
		}

		if (index.get().expireAfterSeconds() >= 0) {
			indexDefinition.expire(index.get().expireAfterSeconds(), TimeUnit.SECONDS);
		}

		return new IndexDefinitionHolder(dotPath, indexDefinition, collection);
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
	protected IndexDefinitionHolder createGeoSpatialIndexDefinition(String dotPath, String collection,
			MongoPersistentProperty persistentProperty) {

		Optional<GeoSpatialIndexed> index = persistentProperty.findAnnotation(GeoSpatialIndexed.class);

		if(!index.isPresent()) {
			return null;
		}

		GeospatialIndex indexDefinition = new GeospatialIndex(dotPath);
		indexDefinition.withBits(index.get().bits());
		indexDefinition.withMin(index.get().min()).withMax(index.get().max());

		if (!index.get().useGeneratedName()) {
			indexDefinition.named(pathAwareIndexName(index.get().name(), dotPath, persistentProperty));
		}

		indexDefinition.typed(index.get().type()).withBucketSize(index.get().bucketSize()).withAdditionalField(index.get().additionalField());

		return new IndexDefinitionHolder(dotPath, indexDefinition, collection);
	}

	private String pathAwareIndexName(String indexName, String dotPath, MongoPersistentProperty property) {

		String nameToUse = StringUtils.hasText(indexName) ? indexName : "";

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

		final List<IndexDefinitionHolder> indexes = new ArrayList<IndexDefinitionHolder>(0);
		entity.doWithAssociations(new AssociationHandler<MongoPersistentProperty>() {

			@Override
			public void doWithAssociation(Association<MongoPersistentProperty> association) {

				MongoPersistentProperty property = association.getInverse();

				String propertyDotPath = (StringUtils.hasText(path) ? path + "." : "") + property.getFieldName();

				if (property.isAnnotationPresent(GeoSpatialIndexed.class) || property.isAnnotationPresent(TextIndexed.class)) {
					throw new MappingException(
							String.format("Cannot create geospatial-/text- index on DBRef in collection '%s' for path '%s'.",
									collection, propertyDotPath));
				}

				IndexDefinitionHolder indexDefinitionHolder = createIndexDefinitionHolderForProperty(propertyDotPath,
						collection, property);

				if (indexDefinitionHolder != null) {
					indexes.add(indexDefinitionHolder);
				}
			}
		});

		return indexes;
	}

	/**
	 * {@link CycleGuard} holds information about properties and the paths for accessing those. This information is used
	 * to detect potential cycles within the references.
	 * 
	 * @author Christoph Strobl
	 */
	static class CycleGuard {

		private final Map<String, List<Path>> propertyTypeMap;

		CycleGuard() {
			this.propertyTypeMap = new LinkedHashMap<String, List<Path>>();
		}

		/**
		 * @param property The property to inspect
		 * @param path The path under which the property can be reached.
		 * @throws CyclicPropertyReferenceException in case a potential cycle is detected.
		 * @see Path#cycles(MongoPersistentProperty, String)
		 */
		void protect(MongoPersistentProperty property, String path) throws CyclicPropertyReferenceException {

			String propertyTypeKey = createMapKey(property);
			if (propertyTypeMap.containsKey(propertyTypeKey)) {

				List<Path> paths = propertyTypeMap.get(propertyTypeKey);

				for (Path existingPath : paths) {

					if (existingPath.cycles(property, path) && property.isEntity()) {
						paths.add(new Path(property, path));

						throw new CyclicPropertyReferenceException(property.getFieldName(), property.getOwner().getType(),
								existingPath.getPath());
					}
				}

				paths.add(new Path(property, path));
			} else {

				ArrayList<Path> paths = new ArrayList<Path>();
				paths.add(new Path(property, path));
				propertyTypeMap.put(propertyTypeKey, paths);
			}
		}

		private String createMapKey(MongoPersistentProperty property) {
			return property.getOwner().getType().getSimpleName() + ":" + property.getFieldName();
		}

		/**
		 * Path defines the property and its full path from the document root. <br />
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
		 */
		static class Path {

			private final MongoPersistentProperty property;
			private final String path;

			Path(MongoPersistentProperty property, String path) {

				this.property = property;
				this.path = path;
			}

			public String getPath() {
				return path;
			}

			/**
			 * Checks whether the given property is owned by the same entity and if it has been already visited by a subset of
			 * the current path. Given {@literal foo.bar.bar} cycles if {@literal foo.bar} has already been visited and
			 * {@code class Bar} contains a property of type {@code Bar}. The previously mentioned path would not cycle if
			 * {@code class Bar} contained a property of type {@code SomeEntity} named {@literal bar}.
			 * 
			 * @param property
			 * @param path
			 * @return
			 */
			boolean cycles(MongoPersistentProperty property, String path) {

				if (!property.getOwner().equals(this.property.getOwner())) {
					return false;
				}

				return path.equals(this.path) || path.contains(this.path + ".") || path.contains("." + this.path);
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
		private final Class<?> type;
		private final String dotPath;

		public CyclicPropertyReferenceException(String propertyName, Class<?> type, String dotPath) {

			this.propertyName = propertyName;
			this.type = type;
			this.dotPath = dotPath;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Throwable#getMessage()
		 */
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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.index.IndexDefinition#getIndexKeys()
		 */
		@Override
		public org.bson.Document getIndexKeys() {
			return indexDefinition.getIndexKeys();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.index.IndexDefinition#getIndexOptions()
		 */
		@Override
		public org.bson.Document getIndexOptions() {
			return indexDefinition.getIndexOptions();
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

		private final TextIndexedFieldSpec parentFieldSpec;

		public TextIndexIncludeOptions(IncludeStrategy strategy, TextIndexedFieldSpec parentFieldSpec) {
			this.strategy = strategy;
			this.parentFieldSpec = parentFieldSpec;
		}

		public TextIndexIncludeOptions(IncludeStrategy strategy) {
			this(strategy, null);
		}

		public IncludeStrategy getStrategy() {
			return strategy;
		}

		public TextIndexedFieldSpec getParentFieldSpec() {
			return parentFieldSpec;
		}

		public boolean isForce() {
			return IncludeStrategy.FORCE.equals(strategy);
		}

	}
}
