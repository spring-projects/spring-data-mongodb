/*
 * Copyright 2026-present the original author or authors.
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

import java.util.List;

import org.bson.Document;
import org.jspecify.annotations.Nullable;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.mapping.CollectionName;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;

/**
 * Base class for index operations.
 * <p>
 * This class enables the implementations to use consistent commands and mapping to be implemented easily for the target
 * execution model.
 *
 * @author Mark Paluch
 * @since 5.1
 */
public abstract class IndexOperationsSupport {

	private static final String PARTIAL_FILTER_EXPRESSION_KEY = "partialFilterExpression";

	private final CollectionName collectionName;
	private final QueryMapper queryMapper;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final @Nullable Class<?> entityType;

	/**
	 * Creates a new {@code IndexOperationsSupport} for the given {@link CollectionName} and {@link QueryMapper}.
	 *
	 * @param collectionName the collection name to use.
	 * @param queryMapper the query mapper to use for mapping index keys.
	 * @param entityType optional entity entityType to use for mapping.
	 */
	public IndexOperationsSupport(CollectionName collectionName, QueryMapper queryMapper, @Nullable Class<?> entityType) {

		Assert.notNull(collectionName, "Collection must not be null");
		Assert.notNull(queryMapper, "QueryMapper must not be null");

		this.collectionName = collectionName;
		this.queryMapper = queryMapper;
		this.mappingContext = queryMapper.getMappingContext();
		this.entityType = entityType;
	}

	/**
	 * Map {@link IndexDefinition} to {@link CreateIndexCommand}.
	 *
	 * @param indexDefinition the index definition to map.
	 * @return the mapped {@link CreateIndexCommand}.
	 */
	protected CreateIndexCommand toCreateIndexCommand(IndexDefinition indexDefinition) {

		IndexOptions indexOptions = mapIndexDefinition(indexDefinition);
		Document keys = mapKeys(indexDefinition);

		return new DefaultCreateIndexCommand(keys, indexOptions, new CreateIndexOptions());
	}

	/**
	 * Map {@link IndexDefinition} to {@link IndexOptions}.
	 *
	 * @param indexDefinition the index definition to map.
	 * @return the mapped {@link IndexOptions}.
	 */
	protected IndexOptions mapIndexDefinition(IndexDefinition indexDefinition) {

		MongoPersistentEntity<?> entity = getConfiguredEntity();

		IndexOptions indexOptions = IndexConverters.toIndexOptions(indexDefinition);

		indexOptions = addPartialFilterIfPresent(indexOptions, indexDefinition.getIndexOptions(), entity);
		indexOptions = addDefaultCollationIfRequired(indexOptions, entity);

		return indexOptions;
	}

	/**
	 * Map index keys to {@link Document}.
	 *
	 * @param indexDefinition the source index definition.
	 * @return the mapped {@link Document}.
	 */
	protected Document mapKeys(IndexDefinition indexDefinition) {
		return queryMapper.getMappedSort(indexDefinition.getIndexKeys(), getConfiguredEntity());
	}

	/**
	 * Return the collection name to use.
	 */
	protected String getCollectionName() {
		return collectionName.getCollectionName(mappingContext::getRequiredPersistentEntity);
	}

	/**
	 * Create the command to alter an index.
	 *
	 * @param name index name.
	 * @param options index options to use.
	 * @return
	 */
	Document alterIndexCommand(String name, org.springframework.data.mongodb.core.index.IndexOptions options) {
		Document indexOptions = new Document("name", name);
		indexOptions.putAll(options.toDocument());
		return new Document("collMod", getCollectionName()).append("index", indexOptions);
	}

	/**
	 * Validate the alter index response and throw {@link UncategorizedMongoDbException} if the index could not be
	 * altered.
	 *
	 * @param name index name.
	 * @param result result document from an earlier
	 *          {@link #alterIndexCommand(String, org.springframework.data.mongodb.core.index.IndexOptions)}.
	 */
	static void validateAlterIndexResponse(String name, Document result) {
		Integer ok = NumberUtils.convertNumberToTargetClass(result.get("ok", (Number) 0), Integer.class);
		if (ok != 1) {
			throw new UncategorizedMongoDbException(
					"Index '%s' could not be modified. Response was %s".formatted(name, result.toJson()), null);
		}
	}

	private @Nullable MongoPersistentEntity<?> getConfiguredEntity() {
		return entityType != null ? mappingContext.getRequiredPersistentEntity(entityType) : null;
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

	@SuppressWarnings("NullAway")
	private static IndexOptions addDefaultCollationIfRequired(IndexOptions ops,
			@Nullable MongoPersistentEntity<?> entity) {

		if (ops.getCollation() != null || entity == null || !entity.hasCollation()) {
			return ops;
		}

		return ops.collation(entity.getCollation().toMongoCollation());
	}

	/**
	 * Interface for a create index command.
	 */
	protected interface CreateIndexCommand {

		/**
		 * Return the index keys.
		 */
		Document keys();

		/**
		 * Return the index options.
		 */
		IndexOptions indexOptions();

		/**
		 * Return the index creation options.
		 */
		CreateIndexOptions createIndexOptions();

		/**
		 * Create a new {@code CreateIndexCommand} with the given {@link CreateIndexOptions}.
		 *
		 * @param createIndexOptions the new {@link CreateIndexOptions} to use.
		 * @return a new {@code CreateIndexCommand} with the given {@link CreateIndexOptions} applied.
		 */
		default CreateIndexCommand withCreateIndexOptions(CreateIndexOptions createIndexOptions) {
			return new DefaultCreateIndexCommand(keys(), indexOptions(), createIndexOptions);
		}

		default IndexModel toIndexModel() {
			return new IndexModel(keys(), indexOptions());
		}

		default List<IndexModel> toIndexModels() {
			return List.of(toIndexModel());
		}
	}

	/**
	 * Value object for a create index command.
	 *
	 * @param keys index keys.
	 * @param indexOptions the index options to use.
	 * @param createIndexOptions index creation options.
	 */
	private record DefaultCreateIndexCommand(Document keys, IndexOptions indexOptions,
			CreateIndexOptions createIndexOptions) implements CreateIndexCommand {

	}

}
