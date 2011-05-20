/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.mongodb.mapping;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.data.document.mongodb.MongoDbFactory;
import org.springframework.data.document.mongodb.index.CompoundIndex;
import org.springframework.data.document.mongodb.index.CompoundIndexes;
import org.springframework.data.document.mongodb.index.GeoSpatialIndexed;
import org.springframework.data.document.mongodb.index.IndexDirection;
import org.springframework.data.document.mongodb.index.Indexed;
import org.springframework.data.document.mongodb.query.GeospatialIndex;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.event.MappingContextEvent;
import org.springframework.data.mapping.model.PersistentProperty;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Component that inspects {@link BasicMongoPersistentEntity} instances contained in the given
 * {@link MongoMappingContext} for indexing metadata and ensures the indexes to be available.
 *
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke
 */
public class MongoPersistentEntityIndexCreator implements ApplicationListener<MappingContextEvent> {

	private static final Log log = LogFactory.getLog(MongoPersistentEntityIndexCreator.class);

	private Set<Class<?>> classesSeen = Collections.newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>());

	private final MongoDbFactory mongoDbFactory;

	public MongoPersistentEntityIndexCreator(MongoMappingContext mappingContext, MongoDbFactory mongoDbFactory) {

		Assert.notNull(mongoDbFactory);
		Assert.notNull(mappingContext);
		this.mongoDbFactory = mongoDbFactory;

		for (MongoPersistentEntity<?> entity : mappingContext.getPersistentEntities()) {
			checkForIndexes(entity);
		}
	}

	/* (non-Javadoc)
		* @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
		*/
	public void onApplicationEvent(MappingContextEvent event) {
		checkForIndexes((MongoPersistentEntity<?>) event.getPersistentEntity());
	}

	protected void checkForIndexes(final MongoPersistentEntity<?> entity) {
		final Class<?> type = entity.getType();
		if (!classesSeen.contains(type)) {
			if (log.isDebugEnabled()) {
				log.debug("Analyzing class " + type + " for index information.");
			}

			// Make sure indexes get created
			if (type.isAnnotationPresent(CompoundIndexes.class)) {
				CompoundIndexes indexes = type.getAnnotation(CompoundIndexes.class);
				for (CompoundIndex index : indexes.value()) {
					String indexColl = index.collection();
					if ("".equals(indexColl)) {
						indexColl = entity.getCollection();
					}
					ensureIndex(indexColl, index.name(), index.def(), index.direction(), index.unique(), index.dropDups(),
							index.sparse());
					if (log.isDebugEnabled()) {
						log.debug("Created compound index " + index);
					}
				}
			}

			entity.doWithProperties(new PropertyHandler() {
				public void doWithPersistentProperty(PersistentProperty persistentProperty) {
					Field field = persistentProperty.getField();
					if (field.isAnnotationPresent(Indexed.class)) {
						Indexed index = field.getAnnotation(Indexed.class);
						String name = index.name();
						if ("".equals(name)) {
							name = field.getName();
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
						ensureIndex(collection, name, null, index.direction(), index.unique(), index.dropDups(), index.sparse());
						if (log.isDebugEnabled()) {
							log.debug("Created property index " + index);
						}
					} else if (field.isAnnotationPresent(GeoSpatialIndexed.class)) {

						GeoSpatialIndexed index = field.getAnnotation(GeoSpatialIndexed.class);

						GeospatialIndex indexObject = new GeospatialIndex(StringUtils.hasText(index.name()) ? index.name() : field
								.getName());
						indexObject.withMin(index.min()).withMax(index.max());

						String collection = StringUtils.hasText(index.collection()) ? index.collection() : entity.getCollection();
						mongoDbFactory.getDb().getCollection(collection).ensureIndex(indexObject.getIndexKeys(), indexObject.getIndexOptions());

						if (log.isDebugEnabled()) {
							log.debug(String.format("Created %s for entity %s in collection %s! ", indexObject, entity.getType(),
									collection));
						}
					}
				}
			});

			classesSeen.add(type);
		}

	}

	protected void ensureIndex(String collection,
														 final String name,
														 final String def,
														 final IndexDirection direction,
														 final boolean unique,
														 final boolean dropDups,
														 final boolean sparse) {
		DBObject defObj;
		if (null != def) {
			defObj = (DBObject) JSON.parse(def);
		} else {
			defObj = new BasicDBObject();
			defObj.put(name, (direction == IndexDirection.ASCENDING ? 1 : -1));
		}
		DBObject opts = new BasicDBObject();
		// opts.put("name", name + "_idx");
		opts.put("dropDups", dropDups);
		opts.put("sparse", sparse);
		opts.put("unique", unique);
		mongoDbFactory.getDb().getCollection(collection).ensureIndex(defObj, opts);
	}

}
