/*
 * Copyright 2011-2012 the original author or authors.
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

import java.lang.reflect.Field;
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
 */
public class MongoPersistentEntityIndexCreator implements
		ApplicationListener<MappingContextEvent<MongoPersistentEntity<?>, MongoPersistentProperty>> {

	private static final Logger log = LoggerFactory.getLogger(MongoPersistentEntityIndexCreator.class);

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
			if (log.isDebugEnabled()) {
				log.debug("Analyzing class " + type + " for index information.");
			}

			// Make sure indexes get created
			if (type.isAnnotationPresent(CompoundIndexes.class)) {
				CompoundIndexes indexes = type.getAnnotation(CompoundIndexes.class);
				for (CompoundIndex index : indexes.value()) {

					String indexColl = StringUtils.hasText(index.collection()) ? index.collection() : entity.getCollection();
					DBObject definition = (DBObject) JSON.parse(index.def());

					ensureIndex(indexColl, index.name(), definition, index.unique(), index.dropDups(), index.sparse(), index.background());

					if (log.isDebugEnabled()) {
						log.debug("Created compound index " + index);
					}
				}
			}

			entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {
				public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {

					Field field = persistentProperty.getField();

					if (field.isAnnotationPresent(Indexed.class)) {

						Indexed index = field.getAnnotation(Indexed.class);
						String name = index.name();

						if (!StringUtils.hasText(name)) {
							name = persistentProperty.getFieldName();
						} else {
							if (!name.equals(field.getName()) && index.unique() && !index.sparse()) {
								// Names don't match, and sparse is not true. This situation will generate an error on the server.
								if (log.isWarnEnabled()) {
									log.warn("The index name " + name + " doesn't match this property name: " + field.getName()
											+ ". Setting sparse=true on this index will prevent errors when inserting documents.");
								}
							}
						}

						String collection = StringUtils.hasText(index.collection()) ? index.collection() : entity.getCollection();
						int direction = index.direction() == IndexDirection.ASCENDING ? 1 : -1;
						DBObject definition = new BasicDBObject(persistentProperty.getFieldName(), direction);

						ensureIndex(collection, name, definition, index.unique(), index.dropDups(), index.sparse(), index.background());

						if (log.isDebugEnabled()) {
							log.debug("Created property index " + index);
						}

					} else if (field.isAnnotationPresent(GeoSpatialIndexed.class)) {

						GeoSpatialIndexed index = field.getAnnotation(GeoSpatialIndexed.class);

						GeospatialIndex indexObject = new GeospatialIndex(persistentProperty.getFieldName());
						indexObject.withMin(index.min()).withMax(index.max());
						indexObject.named(StringUtils.hasText(index.name()) ? index.name() : field.getName());

						String collection = StringUtils.hasText(index.collection()) ? index.collection() : entity.getCollection();
						mongoDbFactory.getDb().getCollection(collection)
								.ensureIndex(indexObject.getIndexKeys(), indexObject.getIndexOptions());

						if (log.isDebugEnabled()) {
							log.debug(String.format("Created %s for entity %s in collection %s! ", indexObject, entity.getType(),
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
	 */
	protected void ensureIndex(String collection, String name, DBObject indexDefinition, boolean unique,
			boolean dropDups, boolean sparse, boolean background) {

		DBObject opts = new BasicDBObject();
		opts.put("name", name);
		opts.put("dropDups", dropDups);
		opts.put("sparse", sparse);
		opts.put("unique", unique);
        opts.put("background", background);

		mongoDbFactory.getDb().getCollection(collection).ensureIndex(indexDefinition, opts);
	}
}
