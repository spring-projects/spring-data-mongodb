/*
 * Copyright 2011-2016 the original author or authors.
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.IndexDefinitionHolder;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.util.MongoDbErrorCodes;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;

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
 */
public class MongoPersistentEntityIndexCreator implements ApplicationListener<MappingContextEvent<?, ?>> {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoPersistentEntityIndexCreator.class);

	private final Map<Class<?>, Boolean> classesSeen = new ConcurrentHashMap<Class<?>, Boolean>();
	private final MongoDbFactory mongoDbFactory;
	private final MongoMappingContext mappingContext;
	private final IndexResolver indexResolver;

	/**
	 * Creates a new {@link MongoPersistentEntityIndexCreator} for the given {@link MongoMappingContext} and
	 * {@link MongoDbFactory}.
	 * 
	 * @param mappingContext must not be {@literal null}.
	 * @param mongoDbFactory must not be {@literal null}.
	 */
	public MongoPersistentEntityIndexCreator(MongoMappingContext mappingContext, MongoDbFactory mongoDbFactory) {
		this(mappingContext, mongoDbFactory, new MongoPersistentEntityIndexResolver(mappingContext));
	}

	/**
	 * Creates a new {@link MongoPersistentEntityIndexCreator} for the given {@link MongoMappingContext} and
	 * {@link MongoDbFactory}.
	 * 
	 * @param mappingContext must not be {@literal null}.
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param indexResolver must not be {@literal null}.
	 */
	public MongoPersistentEntityIndexCreator(MongoMappingContext mappingContext, MongoDbFactory mongoDbFactory,
			IndexResolver indexResolver) {

		Assert.notNull(mongoDbFactory);
		Assert.notNull(mappingContext);
		Assert.notNull(indexResolver);

		this.mongoDbFactory = mongoDbFactory;
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

		if (entity.findAnnotation(Document.class) != null) {
			for (IndexDefinitionHolder indexToCreate : indexResolver.resolveIndexFor(entity.getTypeInformation())) {
				createIndex(indexToCreate);
			}
		}
	}

	void createIndex(IndexDefinitionHolder indexDefinition) {

		try {

			IndexOptions ops = new IndexOptions();

			if (indexDefinition.getIndexOptions() != null) {

				org.bson.Document indexOptions = indexDefinition.getIndexOptions();

				if (indexOptions.containsKey("name")) {
					ops = ops.name(indexOptions.get("name").toString());
				}
				if (indexOptions.containsKey("unique")) {
					ops = ops.unique((Boolean) indexOptions.get("unique"));
				}
				if (indexOptions.containsKey("sparse")) {
					ops = ops.sparse((Boolean) indexOptions.get("sparse"));
				}
				if (indexOptions.containsKey("background")) {
					ops = ops.background((Boolean) indexOptions.get("background"));
				}
				if (indexOptions.containsKey("expireAfterSeconds")) {
					ops = ops.expireAfter((Long) indexOptions.get("expireAfterSeconds"), TimeUnit.SECONDS);
				}
				if (indexOptions.containsKey("min")) {
					ops = ops.min(((Number) indexOptions.get("min")).doubleValue());
				}
				if (indexOptions.containsKey("max")) {
					ops = ops.max(((Number) indexOptions.get("max")).doubleValue());
				}
				if (indexOptions.containsKey("bits")) {
					ops = ops.bits((Integer) indexOptions.get("bits"));
				}
				if (indexOptions.containsKey("bucketSize")) {
					ops = ops.bucketSize(((Number) indexOptions.get("bucketSize")).doubleValue());
				}
				if (indexOptions.containsKey("default_language")) {
					ops = ops.defaultLanguage(indexOptions.get("default_language").toString());
				}
				if (indexOptions.containsKey("language_override")) {
					ops = ops.languageOverride(indexOptions.get("language_override").toString());
				}
				if (indexOptions.containsKey("weights")) {
					ops = ops.weights((org.bson.Document) indexOptions.get("weights"));
				}

				for (String key : indexOptions.keySet()) {
					if (ObjectUtils.nullSafeEquals("2dsphere", indexOptions.get(key))) {
						ops = ops.sphereVersion(2);
					}
				}
			}

			mongoDbFactory.getDb().getCollection(indexDefinition.getCollection(), Document.class)
					.createIndex(indexDefinition.getIndexKeys(), ops);

		} catch (MongoException ex) {

			if (MongoDbErrorCodes.isDataIntegrityViolationCode(ex.getCode())) {

				org.bson.Document existingIndex = fetchIndexInformation(indexDefinition);
				String message = "Cannot create index for '%s' in collection '%s' with keys '%s' and options '%s'.";

				if (existingIndex != null) {
					message += " Index already defined as '%s'.";
				}

				throw new DataIntegrityViolationException(
						String.format(message, indexDefinition.getPath(), indexDefinition.getCollection(),
								indexDefinition.getIndexKeys(), indexDefinition.getIndexOptions(), existingIndex),
						ex);
			}

			RuntimeException exceptionToThrow = mongoDbFactory.getExceptionTranslator().translateExceptionIfPossible(ex);

			throw exceptionToThrow != null ? exceptionToThrow : ex;
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

	private org.bson.Document fetchIndexInformation(IndexDefinitionHolder indexDefinition) {

		if (indexDefinition == null) {
			return null;
		}

		try {

			Object indexNameToLookUp = indexDefinition.getIndexOptions().get("name");

			MongoCursor<org.bson.Document> cursor = mongoDbFactory.getDb().getCollection(indexDefinition.getCollection())
					.listIndexes(org.bson.Document.class).iterator();

			while (cursor.hasNext()) {

				org.bson.Document index = cursor.next();
				if (ObjectUtils.nullSafeEquals(indexNameToLookUp, index.get("name"))) {
					return index;
				}
			}

		} catch (Exception e) {
			LOGGER.debug(
					String.format("Failed to load index information for collection '%s'.", indexDefinition.getCollection()), e);
		}

		return null;
	}
}
