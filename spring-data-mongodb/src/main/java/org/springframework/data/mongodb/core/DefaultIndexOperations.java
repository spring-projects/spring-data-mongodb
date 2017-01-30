/*
 * Copyright 2011-2017 the original author or authors.
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

import static org.springframework.data.mongodb.core.MongoTemplate.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.util.Assert;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;

/**
 * Default implementation of {@link IndexOperations}.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Komi Innocent
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class DefaultIndexOperations implements IndexOperations {

	private static final String PARTIAL_FILTER_EXPRESSION_KEY = "partialFilterExpression";

	private final MongoDbFactory mongoDbFactory;
	private final String collectionName;
	private final QueryMapper mapper;
	private final Class<?> type;

	/**
	 * Creates a new {@link DefaultIndexOperations}.
	 * 
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param queryMapper must not be {@literal null}.
	 */
	public DefaultIndexOperations(MongoDbFactory mongoDbFactory, String collectionName, QueryMapper queryMapper) {

		this(mongoDbFactory, collectionName, queryMapper, null);
	}

	/**
	 * Creates a new {@link DefaultIndexOperations}.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param queryMapper must not be {@literal null}.
	 * @param type Type used for mapping potential partial index filter expression. Can be {@literal null}.
	 * @since 1.10
	 */
	public DefaultIndexOperations(MongoDbFactory mongoDbFactory, String collectionName, QueryMapper queryMapper,
			Class<?> type) {

		Assert.notNull(mongoDbFactory, "MongoDbFactory must not be null!");
		Assert.notNull(collectionName, "Collection name can not be null!");
		Assert.notNull(queryMapper, "QueryMapper must not be null!");

		this.mongoDbFactory = mongoDbFactory;
		this.collectionName = collectionName;
		this.mapper = queryMapper;
		this.type = type;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#ensureIndex(org.springframework.data.mongodb.core.index.IndexDefinition)
	 */
	public String ensureIndex(final IndexDefinition indexDefinition) {

		return execute(collection -> {

			Document indexOptions = indexDefinition.getIndexOptions();

			if (indexOptions != null) {

				IndexOptions ops = IndexConverters.indexDefinitionToIndexOptionsConverter().convert(indexDefinition);

				if (indexOptions.containsKey(PARTIAL_FILTER_EXPRESSION_KEY)) {

					Assert.isInstanceOf(Document.class, indexOptions.get(PARTIAL_FILTER_EXPRESSION_KEY));

					ops.partialFilterExpression( mapper.getMappedObject(
							(Document) indexOptions.get(PARTIAL_FILTER_EXPRESSION_KEY), lookupPersistentEntity(type, collectionName)));
				}

				return collection.createIndex(indexDefinition.getIndexKeys(), ops);
			}
			return collection.createIndex(indexDefinition.getIndexKeys());
		}

		);
	}

	private MongoPersistentEntity<?> lookupPersistentEntity(Class<?> entityType, String collection) {

		if (entityType != null) {
			return mapper.getMappingContext().getRequiredPersistentEntity(entityType);
		}

		Collection<? extends MongoPersistentEntity<?>> entities = mapper.getMappingContext().getPersistentEntities();

		for (MongoPersistentEntity<?> entity : entities) {
			if (entity.getCollection().equals(collection)) {
				return entity;
			}
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.IndexOperations#dropIndex(java.lang.String)
	 */
	public void dropIndex(final String name) {

		execute(collection -> {
			collection.dropIndex(name);
			return null;
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
	 * @see org.springframework.data.mongodb.core.IndexOperations#getIndexInfo()
	 */
	public List<IndexInfo> getIndexInfo() {

		return execute(new CollectionCallback<List<IndexInfo>>() {

			public List<IndexInfo> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				MongoCursor<Document> cursor = collection.listIndexes(Document.class).iterator();
				return getIndexData(cursor);
			}

			private List<IndexInfo> getIndexData(MongoCursor<Document> cursor) {

				List<IndexInfo> indexInfoList = new ArrayList<IndexInfo>();

				while (cursor.hasNext()) {

					Document ix = cursor.next();
					IndexInfo indexInfo = IndexConverters.documentToIndexInfoConverter().convert(ix);
					indexInfoList.add(indexInfo);
				}

				return indexInfoList;
			}
		});
	}

	public <T> T execute(CollectionCallback<T> callback) {

		Assert.notNull(callback, "CollectionCallback must not be null!");

		try {
			MongoCollection<Document> collection = mongoDbFactory.getDb().getCollection(collectionName);
			return callback.doInCollection(collection);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e, mongoDbFactory.getExceptionTranslator());
		}
	}
}
