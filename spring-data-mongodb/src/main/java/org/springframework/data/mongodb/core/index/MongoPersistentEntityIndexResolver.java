/*
 * Copyright 2014 the original author or authors.
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mongodb.core.index.Index.Duplicates;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * {@link IndexResolver} implementation inspecting {@link MongoPersistentEntity} for {@link MongoPersistentEntity} to be
 * indexed. <br />
 * All {@link MongoPersistentProperty} of the {@link MongoPersistentEntity} are inspected for potential indexes by
 * scanning related annotations.
 * 
 * @author Christoph Strobl
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.index.IndexResolver#resolveIndexForClass(java.lang.Class)
	 */
	@Override
	public List<IndexDefinitionHolder> resolveIndexForClass(Class<?> type) {
		return resolveIndexForEntity(mappingContext.getPersistentEntity(type));
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
		Document document = root.findAnnotation(Document.class);
		Assert.notNull(document, "Given entity is not collection root.");

		final List<IndexDefinitionHolder> indexInformation = new ArrayList<MongoPersistentEntityIndexResolver.IndexDefinitionHolder>();
		indexInformation.addAll(potentiallyCreateCompoundIndexDefinitions("", root.getCollection(), root.getType()));

		final CycleGuard guard = new CycleGuard();

		root.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {

				if (persistentProperty.isEntity()) {
					indexInformation.addAll(resolveIndexForClass(persistentProperty.getActualType(),
							persistentProperty.getFieldName(), root.getCollection(), guard));
				}

				IndexDefinitionHolder indexDefinitionHolder = createIndexDefinitionHolderForProperty(
						persistentProperty.getFieldName(), root.getCollection(), persistentProperty);
				if (indexDefinitionHolder != null) {
					indexInformation.add(indexDefinitionHolder);
				}
			}
		});

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
	private List<IndexDefinitionHolder> resolveIndexForClass(final Class<?> type, final String path,
			final String collection, final CycleGuard guard) {

		final List<IndexDefinitionHolder> indexInformation = new ArrayList<MongoPersistentEntityIndexResolver.IndexDefinitionHolder>();
		indexInformation.addAll(potentiallyCreateCompoundIndexDefinitions(path, collection, type));

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);
		entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {

				String propertyDotPath = (StringUtils.hasText(path) ? path + "." : "") + persistentProperty.getFieldName();
				guard.protect(persistentProperty, path);

				if (persistentProperty.isEntity()) {
					try {
						indexInformation.addAll(resolveIndexForClass(persistentProperty.getActualType(), propertyDotPath,
								collection, guard));
					} catch (CyclicPropertyReferenceException e) {
						LOGGER.warn(e.getMessage());
					}
				}

				IndexDefinitionHolder indexDefinitionHolder = createIndexDefinitionHolderForProperty(propertyDotPath,
						collection, persistentProperty);
				if (indexDefinitionHolder != null) {
					indexInformation.add(indexDefinitionHolder);
				}
			}
		});

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
			Class<?> type) {

		if (AnnotationUtils.findAnnotation(type, CompoundIndexes.class) == null
				&& AnnotationUtils.findAnnotation(type, CompoundIndex.class) == null) {
			return Collections.emptyList();
		}

		return createCompoundIndexDefinitions(dotPath, collection, type);
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
			Class<?> type) {

		List<IndexDefinitionHolder> indexDefinitions = new ArrayList<MongoPersistentEntityIndexResolver.IndexDefinitionHolder>();
		CompoundIndexes indexes = AnnotationUtils.findAnnotation(type, CompoundIndexes.class);

		if (indexes != null) {
			for (CompoundIndex index : indexes.value()) {
				indexDefinitions.add(createCompoundIndexDefinition(dotPath, fallbackCollection, index));
			}
		}

		CompoundIndex index = AnnotationUtils.findAnnotation(type, CompoundIndex.class);

		if (index != null) {
			indexDefinitions.add(createCompoundIndexDefinition(dotPath, fallbackCollection, index));
		}

		return indexDefinitions;
	}

	protected IndexDefinitionHolder createCompoundIndexDefinition(String dotPath, String fallbackCollection,
			CompoundIndex index) {

		CompoundIndexDefinition indexDefinition = new CompoundIndexDefinition(resolveCompoundIndexKeyFromStringDefinition(
				dotPath, index.def()));

		if (!index.useGeneratedName()) {
			indexDefinition.named(index.name());
		}

		if (index.unique()) {
			indexDefinition.unique(index.dropDups() ? Duplicates.DROP : Duplicates.RETAIN);
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

		String collection = StringUtils.hasText(index.collection()) ? index.collection() : fallbackCollection;
		return new IndexDefinitionHolder(dotPath, indexDefinition, collection);
	}

	private DBObject resolveCompoundIndexKeyFromStringDefinition(String dotPath, String keyDefinitionString) {

		if (!StringUtils.hasText(dotPath) && !StringUtils.hasText(keyDefinitionString)) {
			throw new InvalidDataAccessApiUsageException("Cannot create index on root level for empty keys.");
		}

		if (!StringUtils.hasText(keyDefinitionString)) {
			return new BasicDBObject(dotPath, 1);
		}

		DBObject dbo = (DBObject) JSON.parse(keyDefinitionString);
		if (!StringUtils.hasText(dotPath)) {
			return dbo;
		}

		BasicDBObjectBuilder dboBuilder = new BasicDBObjectBuilder();

		for (String key : dbo.keySet()) {
			dboBuilder.add(dotPath + "." + key, dbo.get(key));
		}
		return dboBuilder.get();
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
	protected IndexDefinitionHolder createIndexDefinition(String dotPath, String fallbackCollection,
			MongoPersistentProperty persitentProperty) {

		Indexed index = persitentProperty.findAnnotation(Indexed.class);
		String collection = StringUtils.hasText(index.collection()) ? index.collection() : fallbackCollection;

		Index indexDefinition = new Index().on(dotPath,
				IndexDirection.ASCENDING.equals(index.direction()) ? Sort.Direction.ASC : Sort.Direction.DESC);

		if (!index.useGeneratedName()) {
			indexDefinition.named(StringUtils.hasText(index.name()) ? index.name() : dotPath);
		}

		if (index.unique()) {
			indexDefinition.unique(index.dropDups() ? Duplicates.DROP : Duplicates.RETAIN);
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
	protected IndexDefinitionHolder createGeoSpatialIndexDefinition(String dotPath, String fallbackCollection,
			MongoPersistentProperty persistentProperty) {

		GeoSpatialIndexed index = persistentProperty.findAnnotation(GeoSpatialIndexed.class);
		String collection = StringUtils.hasText(index.collection()) ? index.collection() : fallbackCollection;

		GeospatialIndex indexDefinition = new GeospatialIndex(dotPath);
		indexDefinition.withBits(index.bits());
		indexDefinition.withMin(index.min()).withMax(index.max());

		if (!index.useGeneratedName()) {
			indexDefinition.named(StringUtils.hasText(index.name()) ? index.name() : persistentProperty.getName());
		}

		indexDefinition.typed(index.type()).withBucketSize(index.bucketSize()).withAdditionalField(index.additionalField());

		return new IndexDefinitionHolder(dotPath, indexDefinition, collection);
	}

	/**
	 * {@link CycleGuard} holds information about properties and the paths for accessing those. This information is used
	 * to detect potential cycles within the references.
	 * 
	 * @author Christoph Strobl
	 */
	private static class CycleGuard {

		private final Map<String, List<Path>> propertyTypeMap;

		CycleGuard() {
			this.propertyTypeMap = new LinkedHashMap<String, List<Path>>();
		}

		/**
		 * @param property The property to inspect
		 * @param path The path under which the property can be reached.
		 * @throws CyclicPropertyReferenceException in case a potential cycle is detected.
		 */
		void protect(MongoPersistentProperty property, String path) throws CyclicPropertyReferenceException {

			String propertyTypeKey = createMapKey(property);
			if (propertyTypeMap.containsKey(propertyTypeKey)) {

				List<Path> paths = propertyTypeMap.get(propertyTypeKey);

				for (Path existingPath : paths) {

					if (existingPath.cycles(property)) {
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

		private static class Path {

			private final MongoPersistentProperty property;
			private final String path;

			Path(MongoPersistentProperty property, String path) {

				this.property = property;
				this.path = path;
			}

			public String getPath() {
				return path;
			}

			boolean cycles(MongoPersistentProperty property) {

				Pattern pattern = Pattern.compile("\\p{Punct}?" + Pattern.quote(property.getFieldName()) + "(\\p{Punct}|\\w)?");
				Matcher matcher = pattern.matcher(path);

				int count = 0;
				while (matcher.find()) {
					count++;
				}

				return count >= 1 && property.getOwner().getType().equals(this.property.getOwner().getType());
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
			return String.format("Found cycle for field '%s' in type '%s' for path '%s'", propertyName, type.getSimpleName(),
					dotPath);
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
		public DBObject getIndexKeys() {
			return indexDefinition.getIndexKeys();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.index.IndexDefinition#getIndexOptions()
		 */
		@Override
		public DBObject getIndexOptions() {
			return indexDefinition.getIndexOptions();
		}
	}
}
