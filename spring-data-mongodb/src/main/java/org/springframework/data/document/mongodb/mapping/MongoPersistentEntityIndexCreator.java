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
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.dao.DataAccessException;
import org.springframework.data.document.mongodb.CollectionCallback;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.index.CompoundIndex;
import org.springframework.data.document.mongodb.index.CompoundIndexes;
import org.springframework.data.document.mongodb.index.IndexDirection;
import org.springframework.data.document.mongodb.index.Indexed;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.event.MappingContextEvent;
import org.springframework.data.mapping.model.PersistentProperty;
import org.springframework.util.Assert;

/**
 * Component that inspects {@link MongoPersistentEntity} instances contained in the given {@link MongoMappingContext}
 * for indexing metadata and ensures the indexes to be available.
 *
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke
 */
public class MongoPersistentEntityIndexCreator implements ApplicationListener<MappingContextEvent> {

	private static final Log log = LogFactory.getLog(MongoPersistentEntityIndexCreator.class);

	private Set<Class<?>> classesSeen = Collections.newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>());

	private final MongoTemplate mongoTemplate;

	public MongoPersistentEntityIndexCreator(MongoMappingContext mappingContext, MongoTemplate mongoTemplate) {

		Assert.notNull(mongoTemplate);
		Assert.notNull(mappingContext);
		this.mongoTemplate = mongoTemplate;

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
			// Check for special collection setting
			if (type.isAnnotationPresent(Document.class)) {
				Document doc = type.getAnnotation(Document.class);
				String collection = doc.collection();
				if ("".equals(collection)) {
					collection = type.getSimpleName().toLowerCase();
				}
				entity.setCollection(collection);
			}

			// Make sure indexes get created
			if (type.isAnnotationPresent(CompoundIndexes.class)) {
				CompoundIndexes indexes = type.getAnnotation(CompoundIndexes.class);
				for (CompoundIndex index : indexes.value()) {
					String indexColl = index.collection();
					if ("".equals(indexColl)) {
						indexColl = entity.getCollection();
					}
					ensureIndex(indexColl, index.name(), index.def(), index.direction(), index.unique(), index.dropDups(), index.sparse());
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
						String collection = index.collection();
						if ("".equals(collection)) {
							collection = entity.getCollection();
						}
						ensureIndex(collection, name, null, index.direction(), index.unique(), index.dropDups(), index.sparse());
						if (log.isDebugEnabled()) {
							log.debug("Created property index " + index);
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
		mongoTemplate.execute(collection, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				DBObject defObj;
				if (null != def) {
					defObj = (DBObject) JSON.parse(def);
				} else {
					defObj = new BasicDBObject();
					defObj.put(name, (direction == IndexDirection.ASCENDING ? 1 : -1));
				}
				DBObject opts = new BasicDBObject();
				//opts.put("name", name + "_idx");
				opts.put("dropDups", dropDups);
				opts.put("sparse", sparse);
				opts.put("unique", unique);
				collection.ensureIndex(defObj, opts);
				return null;
			}
		});
	}

}
