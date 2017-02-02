/*
 * Copyright 2011-2017 the original author or authors.
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.IndexOperations;
import org.springframework.data.mongodb.core.IndexOperationsProvider;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.IndexDefinitionHolder;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.util.MongoDbErrorCodes;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.mongodb.MongoException;

/**
 * Component that inspects {@link MongoPersistentEntity} instances contained in the given {@link MongoMappingContext}
 * for indexing metadata and ensures the indexes to be available.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Johno Crawford
 * @author Laurent Canet
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MongoPersistentEntityIndexCreator implements ApplicationListener<MappingContextEvent<?, ?>> {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoPersistentEntityIndexCreator.class);

	private final Map<Class<?>, Boolean> classesSeen = new ConcurrentHashMap<Class<?>, Boolean>();
	private final IndexOperationsProvider indexOperationsProvider;
	private final MongoMappingContext mappingContext;
	private final IndexResolver indexResolver;

	/**
	 * Creates a new {@link MongoPersistentEntityIndexCreator} for the given {@link MongoMappingContext} and
	 * {@link MongoDbFactory}.
	 *  @param mappingContext must not be {@literal null}.
	 * @param indexOperationsProvider must not be {@literal null}.
	 */
	public MongoPersistentEntityIndexCreator(MongoMappingContext mappingContext, IndexOperationsProvider indexOperationsProvider) {
		this(mappingContext, indexOperationsProvider, new MongoPersistentEntityIndexResolver(mappingContext));
	}

	/**
	 * Creates a new {@link MongoPersistentEntityIndexCreator} for the given {@link MongoMappingContext} and
	 * {@link MongoDbFactory}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param indexResolver must not be {@literal null}.
	 */
	public MongoPersistentEntityIndexCreator(MongoMappingContext mappingContext, IndexOperationsProvider indexOperationsProvider,
											 IndexResolver indexResolver) {

		Assert.notNull(mappingContext, "MongoMappingContext must not be null!");
		Assert.notNull(indexOperationsProvider, "IndexOperationsProvider must not be null!");
		Assert.notNull(indexResolver, "IndexResolver must not be null!");

		this.indexOperationsProvider = indexOperationsProvider;
		this.mappingContext = mappingContext;
		this.indexResolver = indexResolver;

		for (MongoPersistentEntity<?> entity : mappingContext.getPersistentEntities()) {
			checkForIndexes(entity);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	public void onApplicationEvent(MappingContextEvent<?, ?> event) {

		if (!event.wasEmittedBy(mappingContext)) {
			return;
		}

		PersistentEntity<?, ?> entity = event.getPersistentEntity();

		// Double check type as Spring infrastructure does not consider nested generics
		if (entity instanceof MongoPersistentEntity) {
			checkForIndexes((MongoPersistentEntity<?>) entity);
		}
	}

	private void checkForIndexes(final MongoPersistentEntity<?> entity) {

		Class<?> type = entity.getType();

		if (!classesSeen.containsKey(type)) {

			this.classesSeen.put(type, Boolean.TRUE);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Analyzing class " + type + " for index information.");
			}

			checkForAndCreateIndexes(entity);
		}
	}

	private void checkForAndCreateIndexes(MongoPersistentEntity<?> entity) {

		if (entity.findAnnotation(Document.class).isPresent()) {
			for (IndexDefinitionHolder indexToCreate : indexResolver.resolveIndexFor(entity.getTypeInformation())) {
				createIndex(indexToCreate);
			}
		}
	}

	void createIndex(IndexDefinitionHolder indexDefinition) {

		try {

			IndexOperations indexOperations = indexOperationsProvider.indexOps(indexDefinition.getCollection());
			indexOperations.ensureIndex(indexDefinition);

		} catch (UncategorizedMongoDbException ex) {

			if (ex.getCause() instanceof MongoException &&
					MongoDbErrorCodes.isDataIntegrityViolationCode(((MongoException) ex.getCause()).getCode())) {

				IndexInfo existingIndex = fetchIndexInformation(indexDefinition);
				String message = "Cannot create index for '%s' in collection '%s' with keys '%s' and options '%s'.";

				if (existingIndex != null) {
					message += " Index already defined as '%s'.";
				}

				throw new DataIntegrityViolationException(
						String.format(message, indexDefinition.getPath(), indexDefinition.getCollection(),
								indexDefinition.getIndexKeys(), indexDefinition.getIndexOptions(), existingIndex),
						ex.getCause());
			}

			throw ex;
		}
	}

	/**
	 * Returns whether the current index creator was registered for the given {@link MappingContext}.
	 *
	 * @param context
	 * @return
	 */
	public boolean isIndexCreatorFor(MappingContext<?, ?> context) {
		return this.mappingContext.equals(context);
	}

	private IndexInfo fetchIndexInformation(IndexDefinitionHolder indexDefinition) {

		if (indexDefinition == null) {
			return null;
		}

		try {

			IndexOperations indexOperations = indexOperationsProvider.indexOps(indexDefinition.getCollection());
			Object indexNameToLookUp = indexDefinition.getIndexOptions().get("name");

			List<IndexInfo> existingIndexes = indexOperations.getIndexInfo();

			return existingIndexes.stream().//
					filter(indexInfo -> ObjectUtils.nullSafeEquals(indexNameToLookUp, indexInfo.getName())).//
					findFirst().//
					orElse(null);

		} catch (Exception e) {
			LOGGER.debug(
					String.format("Failed to load index information for collection '%s'.", indexDefinition.getCollection()), e);
		}

		return null;
	}
}
