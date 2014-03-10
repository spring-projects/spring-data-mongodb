/*
 * Copyright 2011-2014 the original author or authors.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Component that inspects {@link MongoPersistentEntity} instances contained in the given {@link MongoMappingContext}
 * for indexing metadata and ensures the indexes to be available.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Johno Crawford
 * @author Laurent Canet
 * @author Thomas Darimont
 */
public class MongoPersistentEntityIndexCreator implements
		ApplicationListener<MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty>> {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoPersistentEntityIndexCreator.class);

	private final Map<Class<?>, Boolean> classesSeen = new ConcurrentHashMap<Class<?>, Boolean>();
	private final MongoDbFactory mongoDbFactory;
	private final MongoMappingContext mappingContext;

	/**
	 * Creats a new {@link MongoPersistentEntityIndexCreator} for the given {@link MongoMappingContext} and
	 * {@link MongoDbFactory}.
	 * 
	 * @param mappingContext must not be {@literal null}
	 * @param mongoDbFactory must not be {@literal null}
	 */
	public MongoPersistentEntityIndexCreator(MongoMappingContext mappingContext, MongoDbFactory mongoDbFactory) {

		Assert.notNull(mongoDbFactory);
		Assert.notNull(mappingContext);

		this.mongoDbFactory = mongoDbFactory;
		this.mappingContext = mappingContext;

		for (MongoPersistentEntity<?> entity : mappingContext.getPersistentEntities()) {
			checkForIndexes(entity);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	public void onApplicationEvent(MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty> event) {

		if (!event.wasEmittedBy(mappingContext)) {
			return;
		}

		PersistentEntity<?, ?> entity = event.getPersistentEntity();

		// Double check type as Spring infrastructure does not consider nested generics
		if (entity instanceof MongoPersistentEntity) {
			checkForIndexes(event.getPersistentEntity());
		}
	}

	protected void checkForIndexes(final MongoPersistentEntity<?> entity) {
		final Class<?> type = entity.getType();
		if (!classesSeen.containsKey(type)) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Analyzing class " + type + " for index information.");
			}

			// Make sure indexes get created
			if (type.isAnnotationPresent(CompoundIndexes.class)) {
				CompoundIndexes indexes = type.getAnnotation(CompoundIndexes.class);
				for (CompoundIndex index : indexes.value()) {

					String indexColl = StringUtils.hasText(index.collection()) ? index.collection() : entity.getCollection();
					DBObject definition = (DBObject) JSON.parse(index.def());

					ensureIndex(indexColl, index.useGeneratedName() ? null : index.name(), definition, index.unique(),
							index.dropDups(), index.sparse(), index.background(), index.expireAfterSeconds());

					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Created compound index " + index);
					}
				}
			}

			entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {
				public void doWithPersistentProperty(MongoPersistentProperty property) {

					if (property.isAnnotationPresent(Indexed.class)) {

						Indexed index = property.findAnnotation(Indexed.class);
						String name = index.name();

						if (index.useGeneratedName()) {
							name = null;
						} else if (!StringUtils.hasText(name)) {
							name = property.getFieldName();
						} else {
							if (!name.equals(property.getName()) && index.unique() && !index.sparse()) {
								// Names don't match, and sparse is not true. This situation will generate an error on the server.
								if (LOGGER.isWarnEnabled()) {
									LOGGER.warn("The index name " + name + " doesn't match this property name: " + property.getName()
											+ ". Setting sparse=true on this index will prevent errors when inserting documents.");
								}
							}
						}

						String collection = StringUtils.hasText(index.collection()) ? index.collection() : entity.getCollection();
						int direction = index.direction() == IndexDirection.ASCENDING ? 1 : -1;
						DBObject definition = new BasicDBObject(property.getFieldName(), direction);

						ensureIndex(collection, name, definition, index.unique(), index.dropDups(), index.sparse(),
								index.background(), index.expireAfterSeconds());

						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("Created property index " + index);
						}

					} else if (property.isAnnotationPresent(GeoSpatialIndexed.class)) {

						GeoSpatialIndexed index = property.findAnnotation(GeoSpatialIndexed.class);

						GeospatialIndex indexObject = new GeospatialIndex(property.getFieldName());
						indexObject.withMin(index.min()).withMax(index.max());
						indexObject.named(index.useGeneratedName() ? null : StringUtils.hasText(index.name()) ? index.name()
								: property.getName());
						indexObject.typed(index.type()).withBucketSize(index.bucketSize())
								.withAdditionalField(index.additionalField());

						String collection = StringUtils.hasText(index.collection()) ? index.collection() : entity.getCollection();
						mongoDbFactory.getDb().getCollection(collection)
								.ensureIndex(indexObject.getIndexKeys(), indexObject.getIndexOptions());

						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug(String.format("Created %s for entity %s in collection %s! ", indexObject, entity.getType(),
									collection));
						}
					}
				}
			});

			classesSeen.put(type, true);
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

	/**
	 * Triggers the actual index creation.
	 * 
	 * @param collection the collection to create the index in
	 * @param name the name of the index about to be created
	 * @param indexDefinition the index definition
	 * @param unique whether it shall be a unique index
	 * @param dropDups whether to drop duplicates
	 * @param sparse sparse or not
	 * @param background whether the index will be created in the background
	 * @param expireAfterSeconds the time to live for documents in the collection
	 */
	protected void ensureIndex(String collection, String name, DBObject indexDefinition, boolean unique,
			boolean dropDups, boolean sparse, boolean background, int expireAfterSeconds) {

		DBObject opts = new BasicDBObject();
		if (name != null) {
			// name is optional, if not specified MongoDB generates a name.
			opts.put("name", name);
		}
		opts.put("dropDups", dropDups);
		opts.put("sparse", sparse);
		opts.put("unique", unique);
		opts.put("background", background);

		if (expireAfterSeconds != -1) {
			opts.put("expireAfterSeconds", expireAfterSeconds);
		}

		mongoDbFactory.getDb().getCollection(collection).ensureIndex(indexDefinition, opts);
	}
}
