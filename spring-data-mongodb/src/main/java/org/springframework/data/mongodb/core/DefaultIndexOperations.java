/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.Order;
import org.springframework.util.Assert;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

/**
 * Default implementation of {@link IndexOperations}
 * @author Mark Pollack
 *
 */
public class DefaultIndexOperations implements IndexOperations {

	private MongoTemplate mongoTemplate;
	private String collectionName;

	public DefaultIndexOperations(MongoTemplate mongoTemplate, String collectionName) {
		Assert.notNull(collectionName, "collectionName can not be null");
		this.mongoTemplate = mongoTemplate;
		this.collectionName = collectionName;
	}

	public void ensureIndex(final IndexDefinition indexDefinition) {
		mongoTemplate.execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				DBObject indexOptions = indexDefinition.getIndexOptions();
				if (indexOptions != null) {
					collection.ensureIndex(indexDefinition.getIndexKeys(), indexOptions);
				} else {
					collection.ensureIndex(indexDefinition.getIndexKeys());
				}
				return null;
			}
		});
	}

	public void dropIndex(final String name) {
		mongoTemplate.execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.dropIndex(name);
				return null;
			}
		});
		
	}

	public void dropAllIndexes() {
		dropIndex("*");		
	}
	
  public void resetIndexCache()  {
		mongoTemplate.execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.resetIndexCache();
				return null;
			}
		});
  }

	public List<IndexInfo> getIndexInfo() {
		return mongoTemplate.execute(collectionName, new CollectionCallback<List<IndexInfo> >() {
			public List<IndexInfo> doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				List<DBObject> dbObjectList = collection.getIndexInfo();
				return getIndexData(dbObjectList);
				//return indexInfoList;
			}

			private List<IndexInfo> getIndexData(List<DBObject> dbObjectList) {
				List<IndexInfo> indexInfoList = new ArrayList<IndexInfo>(); 
				for (DBObject ix : dbObjectList) {
					Map<String, Order> keyOrderMap = new LinkedHashMap<String, Order>();
					DBObject keyDbObject = (DBObject) ix.get("key");
					Iterator entries = keyDbObject.toMap().entrySet().iterator();					
					while (entries.hasNext()) {
					  Entry thisEntry = (Entry) entries.next();
					  String key = thisEntry.getKey().toString();
					  int value = (Integer) thisEntry.getValue();
					  if (value == 1) {
					  	keyOrderMap.put(key, Order.ASCENDING);
					  } else {
					  	keyOrderMap.put(key, Order.DESCENDING);
					  }
					}
					

					String name = ix.get("name").toString();
					boolean unique = 	ix.containsField("unique") ? (Boolean)ix.get("unique") : false;
					boolean dropDuplicates = ix.containsField("dropDups") ? (Boolean)ix.get("dropDups") : false;
					boolean sparse = ix.containsField("sparse") ? (Boolean) ix.get("sparse") : false;
					
					indexInfoList.add(new IndexInfo(keyOrderMap, name, unique, dropDuplicates, sparse));
				}
				return indexInfoList;
			}
		});
	}

  
  
}
