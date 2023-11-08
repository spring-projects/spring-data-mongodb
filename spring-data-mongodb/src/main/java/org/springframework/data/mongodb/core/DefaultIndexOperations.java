/*
 * Copyright 2011-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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

import org.bson.Document;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

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

	private final String collectionName;
	private final QueryMapper mapper;
	private final @Nullable Class<?> type;

	private MongoOperations mongoOperations;

	/**
	 * Creates a new {@link DefaultIndexOperations}.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param queryMapper must not be {@literal null}.
	 * @deprecated since 2.1. Please use
	 *             {@link DefaultIndexOperations#DefaultIndexOperations(MongoOperations, String, Class)}.
	 */
	@Deprecated
	public DefaultIndexOperations(MongoDatabaseFactory mongoDbFactory, String collectionName, QueryMapper queryMapper) {
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
	 * @deprecated since 2.1. Please use
	 *             {@link DefaultIndexOperations#DefaultIndexOperations(MongoOperations, String, Class)}.
	 */
	@Deprecated
	public DefaultIndexOperations(MongoDatabaseFactory mongoDbFactory, String collectionName, QueryMapper queryMapper,
			@Nullable Class<?> type) {

		Assert.notNull(mongoDbFactory, "MongoDbFactory must not be null");
		Assert.notNull(collectionName, "Collection name can not be null");
		Assert.notNull(queryMapper, "QueryMapper must not be null");

		this.collectionName = collectionName;
		this.mapper = queryMapper;
		this.type = type;
		this.mongoOperations = new MongoTemplate(mongoDbFactory);
	}

	/**
	 * Creates a new {@link DefaultIndexOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null} or empty.
	 * @param type can be {@literal null}.
	 * @since 2.1
	 */
	public DefaultIndexOperations(MongoOperations mongoOperations, String collectionName, @Nullable Class<?> type) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null");
		Assert.hasText(collectionName, "Collection name must not be null or empty");

		this.mongoOperations = mongoOperations;
		this.mapper = new QueryMapper(mongoOperations.getConverter());
		this.collectionName = collectionName;
		this.type = type;
	}

	public String ensureIndex(final IndexDefinition indexDefinition) {

		return execute(collection -> {

			MongoPersistentEntity<?> entity = lookupPersistentEntity(type, collectionName);

			IndexOptions indexOptions = IndexConverters.indexDefinitionToIndexOptionsConverter().convert(indexDefinition);

			indexOptions = addPartialFilterIfPresent(indexOptions, indexDefinition.getIndexOptions(), entity);
			indexOptions = addDefaultCollationIfRequired(indexOptions, entity);

			Document mappedKeys = mapper.getMappedSort(indexDefinition.getIndexKeys(), entity);
			return collection.createIndex(mappedKeys, indexOptions);
		});
	}

	@Nullable
	private MongoPersistentEntity<?> lookupPersistentEntity(@Nullable Class<?> entityType, String collection) {

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

	public void dropIndex(final String name) {

		execute(collection -> {
			collection.dropIndex(name);
			return null;
		});

	}

	@Override
	public void alterIndex(String name, org.springframework.data.mongodb.core.index.IndexOptions options) {

		Document indexOptions = new Document("name", name);
		indexOptions.putAll(options.toDocument());

		Document result = mongoOperations
				.execute(db -> db.runCommand(new Document("collMod", collectionName).append("index", indexOptions)));

		if(NumberUtils.convertNumberToTargetClass(result.get("ok", (Number) 0), Integer.class) != 1) {
			throw new UncategorizedMongoDbException("Index '%s' could not be modified. Response was %s".formatted(name, result.toJson()), null);
		}
	}

	public void dropAllIndexes() {
		dropIndex("*");
	}

	public List<IndexInfo> getIndexInfo() {

		return execute(new CollectionCallback<List<IndexInfo>>() {

			public List<IndexInfo> doInCollection(MongoCollection<Document> collection)
					throws MongoException, DataAccessException {

				MongoCursor<Document> cursor = collection.listIndexes(Document.class).iterator();
				return getIndexData(cursor);
			}

			private List<IndexInfo> getIndexData(MongoCursor<Document> cursor) {

				int available = cursor.available();
				List<IndexInfo> indexInfoList = available > 0 ? new ArrayList<>(available) : new ArrayList<>();

				while (cursor.hasNext()) {

					Document ix = cursor.next();
					IndexInfo indexInfo = IndexConverters.documentToIndexInfoConverter().convert(ix);
					indexInfoList.add(indexInfo);
				}

				return indexInfoList;
			}
		});
	}

	@Nullable
	public <T> T execute(CollectionCallback<T> callback) {

		Assert.notNull(callback, "CollectionCallback must not be null");

		if (type != null) {
			return mongoOperations.execute(type, callback);
		}

		return mongoOperations.execute(collectionName, callback);
	}

	private IndexOptions addPartialFilterIfPresent(IndexOptions ops, Document sourceOptions,
			@Nullable MongoPersistentEntity<?> entity) {

		if (!sourceOptions.containsKey(PARTIAL_FILTER_EXPRESSION_KEY)) {
			return ops;
		}

		Assert.isInstanceOf(Document.class, sourceOptions.get(PARTIAL_FILTER_EXPRESSION_KEY));
		return ops.partialFilterExpression(
				mapper.getMappedSort((Document) sourceOptions.get(PARTIAL_FILTER_EXPRESSION_KEY), entity));
	}

	private static IndexOptions addDefaultCollationIfRequired(IndexOptions ops, MongoPersistentEntity<?> entity) {

		if (ops.getCollation() != null || entity == null || !entity.hasCollation()) {
			return ops;
		}

		return ops.collation(entity.getCollation().toMongoCollation());
	}
}
