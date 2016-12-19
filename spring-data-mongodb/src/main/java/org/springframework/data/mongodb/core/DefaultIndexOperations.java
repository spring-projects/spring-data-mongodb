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
package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.util.Assert;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

/**
 * Default implementation of {@link IndexOperations}.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Komi Innocent
 * @author Christoph Strobl
 */
public class DefaultIndexOperations implements IndexOperations {

	private static final String PARTIAL_FILTER_EXPRESSION_KEY = "partialFilterExpression";

	private final MongoOperations mongoOperations;
	private final String collectionName;
	private final QueryMapper mapper;
	private final Class<?> type;

	/**
	 * Creates a new {@link DefaultIndexOperations}.
	 * 
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 */
	public DefaultIndexOperations(MongoOperations mongoOperations, String collectionName) {
		this(mongoOperations, collectionName, null);
	}

	/**
	 * Creates a new {@link DefaultIndexOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param type Type used for mapping potential partial index filter expression. Can be {@literal null}.
	 * @since 1.10
	 */
	public DefaultIndexOperations(MongoOperations mongoOperations, String collectionName, Class<?> type) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null!");
		Assert.notNull(collectionName, "Collection name can not be null!");

		this.mongoOperations = mongoOperations;
		this.collectionName = collectionName;
		this.mapper = new QueryMapper(mongoOperations.getConverter());
		this.type = type;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#ensureIndex(org.springframework.data.mongodb.core.index.IndexDefinition)
	 */
	public void ensureIndex(final IndexDefinition indexDefinition) {

		mongoOperations.execute(collectionName, new CollectionCallback<Object>() {
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				DBObject indexOptions = indexDefinition.getIndexOptions();

				if (indexOptions != null && indexOptions.containsField(PARTIAL_FILTER_EXPRESSION_KEY)) {

					Assert.isInstanceOf(DBObject.class, indexOptions.get(PARTIAL_FILTER_EXPRESSION_KEY));

					indexOptions.put(PARTIAL_FILTER_EXPRESSION_KEY,
							mapper.getMappedObject((DBObject) indexOptions.get(PARTIAL_FILTER_EXPRESSION_KEY),
									lookupPersistentEntity(type, collectionName)));
				}

				if (indexOptions != null) {
					collection.createIndex(indexDefinition.getIndexKeys(), indexOptions);
				} else {
					collection.createIndex(indexDefinition.getIndexKeys());
				}
				return null;
			}

			private MongoPersistentEntity<?> lookupPersistentEntity(Class<?> entityType, String collection) {

				if (entityType != null) {
					return mongoOperations.getConverter().getMappingContext().getPersistentEntity(entityType);
				}

				Collection<? extends MongoPersistentEntity<?>> entities = mongoOperations.getConverter().getMappingContext()
						.getPersistentEntities();

				for (MongoPersistentEntity<?> entity : entities) {
					if (entity.getCollection().equals(collection)) {
						return entity;
					}
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
	@Deprecated
	public void resetIndexCache() {
		mongoOperations.execute(collectionName, new CollectionCallback<Void>() {
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {

				ReflectiveDBCollectionInvoker.resetIndexCache(collection);
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
					indexInfoList.add(IndexInfo.indexInfoOf(ix));
				}

				return indexInfoList;
			}
		});
	}
}
