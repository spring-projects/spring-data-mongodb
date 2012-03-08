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
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.Order;
import org.springframework.util.Assert;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

/**
 * Default implementation of {@link IndexOperations}.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class DefaultIndexOperations implements IndexOperations {

	private final MongoOperations mongoOperations;
	private final String collectionName;

	/**
	 * Creates a new {@link DefaultIndexOperations}.
	 * 
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 */
	public DefaultIndexOperations(MongoOperations mongoOperations, String collectionName) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null!");
		Assert.notNull(collectionName, "Collection name can not be null!");

		this.mongoOperations = mongoOperations;
		this.collectionName = collectionName;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#ensureIndex(org.springframework.data.mongodb.core.index.IndexDefinition)
	 */
	public void ensureIndex(final IndexDefinition indexDefinition) {
		mongoOperations.execute(collectionName, new CollectionCallback<Object>() {
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#dropIndex(java.lang.String)
	 */
	public void dropIndex(final String name) {
		mongoOperations.execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.dropIndex(name);
				return null;
			}
		});

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#dropAllIndexes()
	 */
	public void dropAllIndexes() {
		dropIndex("*");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#resetIndexCache()
	 */
	public void resetIndexCache() {
		mongoOperations.execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.resetIndexCache();
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#getIndexInfo()
	 */
	public List<IndexInfo> getIndexInfo() {

		return mongoOperations.execute(collectionName, new CollectionCallback<List<IndexInfo>>() {
			public List<IndexInfo> doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				List<DBObject> dbObjectList = collection.getIndexInfo();
				return getIndexData(dbObjectList);
			}

			private List<IndexInfo> getIndexData(List<DBObject> dbObjectList) {

				List<IndexInfo> indexInfoList = new ArrayList<IndexInfo>();

				for (DBObject ix : dbObjectList) {

					DBObject keyDbObject = (DBObject) ix.get("key");
					int numberOfElements = keyDbObject.keySet().size();

					List<IndexField> indexFields = new ArrayList<IndexField>(numberOfElements);

					for (String key : keyDbObject.keySet()) {

						Object value = keyDbObject.get(key);

						if (Integer.valueOf(1).equals(value)) {
							indexFields.add(IndexField.create(key, Order.ASCENDING));
						} else if (Integer.valueOf(-1).equals(value)) {
							indexFields.add(IndexField.create(key, Order.DESCENDING));
						} else if ("2d".equals(value)) {
							indexFields.add(IndexField.geo(key));
						}
					}

					String name = ix.get("name").toString();

					boolean unique = ix.containsField("unique") ? (Boolean) ix.get("unique") : false;
					boolean dropDuplicates = ix.containsField("dropDups") ? (Boolean) ix.get("dropDups") : false;
					boolean sparse = ix.containsField("sparse") ? (Boolean) ix.get("sparse") : false;

					indexInfoList.add(new IndexInfo(indexFields, name, unique, dropDuplicates, sparse));
				}

				return indexInfoList;
			}
		});
	}
}
