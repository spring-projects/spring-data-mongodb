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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class MongoPersistentEntityIndexCreator implements ApplicationListener<MappingContextEvent>{

  private static final Logger log = LoggerFactory.getLogger(MongoPersistentEntityIndexCreator.class);

  private Map<String, CompoundIndex> compoundIndexes = new HashMap<String, CompoundIndex>();
  private Map<String, Indexed> fieldIndexes = new HashMap<String, Indexed>();
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

  protected void checkForIndexes(MongoPersistentEntity<?> entity) {
    Class<?> type = entity.getType();
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
            indexColl = type.getSimpleName().toLowerCase();
          }
          if (!compoundIndexes.containsKey(indexColl)) {
            ensureIndex(indexColl, index.name(), index.def(), index.direction(), index.unique(), index.dropDups(), index.sparse());
            if (log.isDebugEnabled()) {
              log.debug("Created compound index " + index);
            }
            compoundIndexes.put(indexColl, index);
          }
        }
      }

      entity.doWithProperties(new PropertyHandler() {
        public void doWithPersistentProperty(PersistentProperty persistentProperty) {
          Field field = persistentProperty.getField();
          if (field.isAnnotationPresent(Indexed.class)) {
            Indexed index = field.getAnnotation(Indexed.class);
            String collection = index.collection();
            if ("".equals(collection)) {
              collection = field.getName();
            }
            if (!fieldIndexes.containsKey(collection)) {
              ensureIndex(collection, index.name(), null, index.direction(), index.unique(), index.dropDups(), index.sparse());
              if (log.isDebugEnabled()) {
                log.debug("Created property index " + index);
              }
              fieldIndexes.put(collection, index);
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
        if (!"".equals(name)) {
          opts.put("name", name);
        }
        opts.put("dropDups", dropDups);
        opts.put("sparse", sparse);
        opts.put("unique", unique);
        collection.ensureIndex(defObj, opts);
        return null;
      }
    });
  }

}
