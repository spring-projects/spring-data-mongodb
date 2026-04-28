/*
 * Copyright 2016-present the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.bson.Document;
import org.jspecify.annotations.Nullable;

import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.ReactiveIndexOperations;
import org.springframework.data.mongodb.core.mapping.CollectionName;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveIndexOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public class DefaultReactiveIndexOperations extends IndexOperationsSupport implements ReactiveIndexOperations {

	private final ReactiveMongoOperations mongoOperations;

	/**
	 * Creates a new {@code DefaultReactiveIndexOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param queryMapper must not be {@literal null}.
	 */
	public DefaultReactiveIndexOperations(ReactiveMongoOperations mongoOperations, String collectionName,
			QueryMapper queryMapper) {
		this(mongoOperations, collectionName, queryMapper, null);
	}

	/**
	 * Creates a new {@code DefaultReactiveIndexOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param queryMapper must not be {@literal null}.
	 * @param type used for mapping potential partial index filter expression.
	 */
	public DefaultReactiveIndexOperations(ReactiveMongoOperations mongoOperations, String collectionName,
			QueryMapper queryMapper, @Nullable Class<?> type) {
		this(mongoOperations, CollectionName.just(collectionName), queryMapper, type);
	}

	/**
	 * Creates a new {@code DefaultReactiveIndexOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param queryMapper must not be {@literal null}.
	 * @param type used for mapping potential partial index filter expression.
	 */
	protected DefaultReactiveIndexOperations(ReactiveMongoOperations mongoOperations, CollectionName collectionName,
			QueryMapper queryMapper, @Nullable Class<?> type) {

		super(collectionName, queryMapper, type);

		Assert.notNull(mongoOperations, "ReactiveMongoOperations must not be null");
		this.mongoOperations = mongoOperations;
	}

	@Override
	@SuppressWarnings("NullAway")
	public Mono<String> createIndex(IndexDefinition indexDefinition) {

		return mongoOperations.execute(getCollectionName(), collection -> {

			CreateIndexCommand command = toCreateIndexCommand(indexDefinition);
			return collection.createIndexes(command.toIndexModels(), command.createIndexOptions());
		}).next();
	}

	@Override
	public Mono<Void> alterIndex(String name, org.springframework.data.mongodb.core.index.IndexOptions options) {

		return mongoOperations.execute(db -> {
			return Flux.from(db.runCommand(alterIndexCommand(name, options)))
					.doOnNext(result -> {
						validateAlterIndexResponse(name, result);
					});
		}).then();
	}

	@Override
	public Mono<Void> dropIndex(String name) {
		return mongoOperations.execute(getCollectionName(), collection -> collection.dropIndex(name)).then();
	}

	@Override
	public Mono<Void> dropAllIndexes() {
		return dropIndex("*");
	}

	@Override
	public Flux<IndexInfo> getIndexInfo() {
		return mongoOperations.execute(getCollectionName(), collection -> collection.listIndexes(Document.class)) //
				.map(IndexInfo::indexInfoOf);
	}

}
