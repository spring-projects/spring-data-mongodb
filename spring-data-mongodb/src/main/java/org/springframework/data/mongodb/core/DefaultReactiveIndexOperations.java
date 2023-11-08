/*
 * Copyright 2016-2023 the original author or authors.
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

import java.util.Collection;
import java.util.Optional;

import org.bson.Document;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.ReactiveIndexOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

import com.mongodb.client.model.IndexOptions;

/**
 * Default implementation of {@link ReactiveIndexOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public class DefaultReactiveIndexOperations implements ReactiveIndexOperations {

	private static final String PARTIAL_FILTER_EXPRESSION_KEY = "partialFilterExpression";

	private final ReactiveMongoOperations mongoOperations;
	private final String collectionName;
	private final QueryMapper queryMapper;
	private final Optional<Class<?>> type;

	/**
	 * Creates a new {@link DefaultReactiveIndexOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param queryMapper must not be {@literal null}.
	 */
	public DefaultReactiveIndexOperations(ReactiveMongoOperations mongoOperations, String collectionName,
			QueryMapper queryMapper) {
		this(mongoOperations, collectionName, queryMapper, Optional.empty());
	}

	/**
	 * Creates a new {@link DefaultReactiveIndexOperations}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param queryMapper must not be {@literal null}.
	 * @param type used for mapping potential partial index filter expression, must not be {@literal null}.
	 */
	public DefaultReactiveIndexOperations(ReactiveMongoOperations mongoOperations, String collectionName,
			QueryMapper queryMapper, Class<?> type) {
		this(mongoOperations, collectionName, queryMapper, Optional.of(type));
	}

	private DefaultReactiveIndexOperations(ReactiveMongoOperations mongoOperations, String collectionName,
			QueryMapper queryMapper, Optional<Class<?>> type) {

		Assert.notNull(mongoOperations, "ReactiveMongoOperations must not be null");
		Assert.notNull(collectionName, "Collection must not be null");
		Assert.notNull(queryMapper, "QueryMapper must not be null");

		this.mongoOperations = mongoOperations;
		this.collectionName = collectionName;
		this.queryMapper = queryMapper;
		this.type = type;
	}

	public Mono<String> ensureIndex(final IndexDefinition indexDefinition) {

		return mongoOperations.execute(collectionName, collection -> {

			MongoPersistentEntity<?> entity = type
					.map(val -> (MongoPersistentEntity) queryMapper.getMappingContext().getRequiredPersistentEntity(val))
					.orElseGet(() -> lookupPersistentEntity(collectionName));

			IndexOptions indexOptions = IndexConverters.indexDefinitionToIndexOptionsConverter().convert(indexDefinition);

			indexOptions = addPartialFilterIfPresent(indexOptions, indexDefinition.getIndexOptions(), entity);
			indexOptions = addDefaultCollationIfRequired(indexOptions, entity);

			return collection.createIndex(indexDefinition.getIndexKeys(), indexOptions);

		}).next();
	}

	@Override
	public Mono<Void> alterIndex(String name, org.springframework.data.mongodb.core.index.IndexOptions options) {

		return mongoOperations.execute(db -> {
			Document indexOptions = new Document("name", name);
			indexOptions.putAll(options.toDocument());

			return Flux.from(db.runCommand(new Document("collMod", collectionName).append("index", indexOptions)))
					.doOnNext(result -> {
						if(NumberUtils.convertNumberToTargetClass(result.get("ok", (Number) 0), Integer.class) != 1) {
							throw new UncategorizedMongoDbException("Index '%s' could not be modified. Response was %s".formatted(name, result.toJson()), null);
						}
					});
		}).then();
	}

	@Nullable
	private MongoPersistentEntity<?> lookupPersistentEntity(String collection) {

		Collection<? extends MongoPersistentEntity<?>> entities = queryMapper.getMappingContext().getPersistentEntities();

		return entities.stream() //
				.filter(entity -> entity.getCollection().equals(collection)) //
				.findFirst() //
				.orElse(null);
	}

	public Mono<Void> dropIndex(final String name) {
		return mongoOperations.execute(collectionName, collection -> collection.dropIndex(name)).then();
	}

	public Mono<Void> dropAllIndexes() {
		return dropIndex("*");
	}

	public Flux<IndexInfo> getIndexInfo() {

		return mongoOperations.execute(collectionName, collection -> collection.listIndexes(Document.class)) //
				.map(IndexConverters.documentToIndexInfoConverter()::convert);
	}

	private IndexOptions addPartialFilterIfPresent(IndexOptions ops, Document sourceOptions,
			@Nullable MongoPersistentEntity<?> entity) {

		if (!sourceOptions.containsKey(PARTIAL_FILTER_EXPRESSION_KEY)) {
			return ops;
		}

		Assert.isInstanceOf(Document.class, sourceOptions.get(PARTIAL_FILTER_EXPRESSION_KEY));
		return ops.partialFilterExpression(
				queryMapper.getMappedObject((Document) sourceOptions.get(PARTIAL_FILTER_EXPRESSION_KEY), entity));
	}

	private static IndexOptions addDefaultCollationIfRequired(IndexOptions ops, MongoPersistentEntity<?> entity) {

		if (ops.getCollation() != null || entity == null || !entity.hasCollation()) {
			return ops;
		}

		return ops.collation(entity.getCollation().toMongoCollation());
	}
}
