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

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;
import org.springframework.dao.DataAccessException;
import org.springframework.data.document.mongodb.CollectionCallback;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.index.CompoundIndex;
import org.springframework.data.document.mongodb.index.CompoundIndexes;
import org.springframework.data.document.mongodb.index.IndexDirection;
import org.springframework.data.document.mongodb.index.Indexed;
import org.springframework.data.mapping.BasicMappingConfigurationBuilder;
import org.springframework.data.mapping.MappingBeanHelper;
import org.springframework.data.mapping.model.*;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MongoMappingConfigurationBuilder extends BasicMappingConfigurationBuilder {

  protected Map<String, CompoundIndex> compoundIndexes = new HashMap<String, CompoundIndex>();
  protected Map<String, Indexed> fieldIndexes = new HashMap<String, Indexed>();
  protected MongoTemplate mongoTemplate;

  public MongoMappingConfigurationBuilder(MongoTemplate mongoTemplate) {
    this.mongoTemplate = mongoTemplate;
    // Augment simpleTypes with MongoDB-specific classes
    Set<String> simpleTypes = MappingBeanHelper.getSimpleTypes();
    simpleTypes.add(DBRef.class.getName());
    simpleTypes.add(ObjectId.class.getName());
    simpleTypes.add(CodeWScope.class.getName());
  }

  @Override
  public PersistentProperty<?> createPersistentProperty(Field field, PropertyDescriptor descriptor) throws MappingConfigurationException {
    PersistentProperty<?> property = super.createPersistentProperty(field, descriptor);
    if (field.isAnnotationPresent(Indexed.class)) {
      Indexed index = field.getAnnotation(Indexed.class);
      String collection = index.collection();
      if ("".equals(collection)) {
        collection = field.getName();
      }
      if (!fieldIndexes.containsKey(collection)) {
        ensureIndex(collection, index.name(), null, index.direction(), index.unique(), index.dropDups(), index.sparse());
        fieldIndexes.put(collection, index);
      }
    }
    return property;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public <T> PersistentEntity<T> createPersistentEntity(Class<T> type, MappingContext mappingContext) throws MappingConfigurationException {
    MongoPersistentEntity<T> entity = new MongoPersistentEntity<T>(mappingContext, type);

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
          compoundIndexes.put(indexColl, index);
        }
      }
    }

    return entity;
  }

  @Override
  public boolean isAssociation(Field field, PropertyDescriptor descriptor) throws MappingConfigurationException {
    if (field.isAnnotationPresent(DBRef.class)) {
      return true;
    }
    return super.isAssociation(field, descriptor);
  }

  @Override
  public Association createAssociation(PersistentProperty<?> property) {
    return super.createAssociation(property);
  }

  @Override
  protected boolean isIdField(Field field) {
    if (super.isIdField(field)) {
      return true;
    }
    if (field.getType() == ObjectId.class || field.getType() == BigInteger.class) {
      if ("id".equals(field.getName()) || "_id".equals(field.getName())) {
        return true;
      }
    }
    return false;
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
