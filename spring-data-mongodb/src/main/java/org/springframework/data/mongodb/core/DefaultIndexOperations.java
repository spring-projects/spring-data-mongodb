/*
 * Copyright 2011-present the original author or authors.
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
import java.util.List;

import org.bson.Document;
import org.jspecify.annotations.Nullable;

import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.mapping.CollectionName;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link IndexOperations}.
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Komi Innocent
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class DefaultIndexOperations extends IndexOperationsSupport implements IndexOperations {

	private final MongoOperations mongoOperations;

	/**
	 * Creates a new {@code DefaultIndexOperations}.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param queryMapper must not be {@literal null}.
	 * @deprecated since 2.1. Please use
	 *             {@code DefaultIndexOperations#DefaultIndexOperations(MongoOperations, String, Class)}.
	 */
	@Deprecated
	public DefaultIndexOperations(MongoDatabaseFactory mongoDbFactory, String collectionName, QueryMapper queryMapper) {
		this(mongoDbFactory, collectionName, queryMapper, null);
	}

	/**
	 * Creates a new {@code DefaultIndexOperations}.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param queryMapper must not be {@literal null}.
	 * @param type Type used for mapping potential partial index filter expression. Can be {@literal null}.
	 * @since 1.10
	 * @deprecated since 2.1. Please use
	 *             {@code DefaultIndexOperations#DefaultIndexOperations(MongoOperations, String, Class)}.
	 */
	@Deprecated
	public DefaultIndexOperations(MongoDatabaseFactory mongoDbFactory, String collectionName, QueryMapper queryMapper,
			@Nullable Class<?> type) {
		this(new MongoTemplate(mongoDbFactory), collectionName, queryMapper, type);
	}

	/**
	 * Creates a new {@code DefaultIndexOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null} or empty.
	 * @param type can be {@literal null}.
	 * @since 2.1
	 */
	public DefaultIndexOperations(MongoOperations mongoOperations, String collectionName, @Nullable Class<?> type) {
		this(mongoOperations, CollectionName.just(collectionName), new QueryMapper(mongoOperations.getConverter()), type);
	}

	/**
	 * Creates a new {@code DefaultIndexOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param queryMapper must not be {@literal null}.
	 * @param type used for mapping potential partial index filter expression.
	 * @since 5.1
	 */
	public DefaultIndexOperations(MongoOperations mongoOperations, String collectionName, QueryMapper queryMapper,
			@Nullable Class<?> type) {
		this(mongoOperations, CollectionName.just(collectionName), queryMapper, type);
	}

	/**
	 * Creates a new {@code DefaultIndexOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null} or empty.
	 * @param type can be {@literal null}.
	 * @since 5.1
	 */
	protected DefaultIndexOperations(MongoOperations mongoOperations, CollectionName collectionName,
			QueryMapper queryMapper, @Nullable Class<?> type) {

		super(collectionName, queryMapper, type);

		Assert.notNull(mongoOperations, "MongoOperations must not be null");
		this.mongoOperations = mongoOperations;
	}

	@Override
	public String createIndex(IndexDefinition indexDefinition) {

		return execute(collection -> {

			CreateIndexCommand command = toCreateIndexCommand(indexDefinition);
			return collection.createIndexes(command.toIndexModels(), command.createIndexOptions()).get(0);
		});
	}

	@Override
	public void dropIndex(String name) {

		execute(collection -> {
			collection.dropIndex(name);
			return null;
		});
	}

	@Override
	@SuppressWarnings("NullAway")
	public void alterIndex(String name, org.springframework.data.mongodb.core.index.IndexOptions options) {

		Document result = mongoOperations
				.execute(db -> db.runCommand(alterIndexCommand(name, options), Document.class));

		validateAlterIndexResponse(name, result);
	}

	@Override
	public void dropAllIndexes() {
		dropIndex("*");
	}

	@Override
	public List<IndexInfo> getIndexInfo() {
		return execute(
				collection -> collection.listIndexes(Document.class).map(IndexInfo::indexInfoOf).into(new ArrayList<>()));
	}

	@SuppressWarnings("NullAway")
	public <T> T execute(CollectionCallback<T> callback) {

		Assert.notNull(callback, "CollectionCallback must not be null");
		return mongoOperations.execute(getCollectionName(), callback);
	}
}
