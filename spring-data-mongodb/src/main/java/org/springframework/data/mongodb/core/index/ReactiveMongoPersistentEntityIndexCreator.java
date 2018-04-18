/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.IndexDefinitionHolder;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.util.MongoDbErrorCodes;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.mongodb.MongoException;

/**
 * Component that inspects {@link MongoPersistentEntity} instances contained in the given {@link MongoMappingContext}
 * for indexing metadata and ensures the indexes to be available using reactive infrastructure.
 *
 * @author Mark Paluch
 * @since 2.1
 */
public class ReactiveMongoPersistentEntityIndexCreator {

	private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveMongoPersistentEntityIndexCreator.class);

	private final Map<Class<?>, Boolean> classesSeen = new ConcurrentHashMap<Class<?>, Boolean>();
	private final MongoMappingContext mappingContext;
	private final ReactiveIndexOperationsProvider operationsProvider;
	private final IndexResolver indexResolver;

	/**
	 * Creates a new {@link ReactiveMongoPersistentEntityIndexCreator} for the given {@link MongoMappingContext},
	 * {@link ReactiveIndexOperationsProvider}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param operationsProvider must not be {@literal null}.
	 */
	public ReactiveMongoPersistentEntityIndexCreator(MongoMappingContext mappingContext,
			ReactiveIndexOperationsProvider operationsProvider) {
		this(mappingContext, operationsProvider, new MongoPersistentEntityIndexResolver(mappingContext));
	}

	/**
	 * Creates a new {@link ReactiveMongoPersistentEntityIndexCreator} for the given {@link MongoMappingContext},
	 * {@link ReactiveIndexOperationsProvider}, and {@link IndexResolver}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param operationsProvider must not be {@literal null}.
	 * @param indexResolver must not be {@literal null}.
	 */
	public ReactiveMongoPersistentEntityIndexCreator(MongoMappingContext mappingContext,
			ReactiveIndexOperationsProvider operationsProvider, IndexResolver indexResolver) {

		Assert.notNull(mappingContext, "MongoMappingContext must not be null!");
		Assert.notNull(operationsProvider, "ReactiveIndexOperations must not be null!");
		Assert.notNull(indexResolver, "IndexResolver must not be null!");

		this.mappingContext = mappingContext;
		this.operationsProvider = operationsProvider;
		this.indexResolver = indexResolver;
	}

	/**
	 * Returns whether the current index creator was registered for the given {@link MappingContext}.
	 *
	 * @param context
	 * @return
	 */
	public boolean isIndexCreatorFor(MappingContext<?, ?> context) {
		return this.mappingContext.equals(context);
	}

	/**
	 * Inspect entities for index creation.
	 *
	 * @return a {@link Mono} that completes without value after indexes were created.
	 */
	public Mono<Void> checkForIndexes(MongoPersistentEntity<?> entity) {

		Class<?> type = entity.getType();

		if (!classesSeen.containsKey(type)) {

			if (this.classesSeen.put(type, Boolean.TRUE) == null) {

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Analyzing class " + type + " for index information.");
				}

				return checkForAndCreateIndexes(entity);
			}
		}

		return Mono.empty();
	}

	private Mono<Void> checkForAndCreateIndexes(MongoPersistentEntity<?> entity) {

		List<Mono<?>> publishers = new ArrayList<>();

		if (entity.isAnnotationPresent(Document.class)) {
			for (IndexDefinitionHolder indexToCreate : indexResolver.resolveIndexFor(entity.getTypeInformation())) {
				publishers.add(createIndex(indexToCreate));
			}
		}

		return publishers.isEmpty() ? Mono.empty() : Flux.merge(publishers).then();
	}

	Mono<String> createIndex(IndexDefinitionHolder indexDefinition) {

		return operationsProvider.indexOps(indexDefinition.getCollection()).ensureIndex(indexDefinition) //
				.onErrorResume(ReactiveMongoPersistentEntityIndexCreator::isDataIntegrityViolation,
						e -> translateException(e, indexDefinition));

	}

	private Mono<? extends String> translateException(Throwable e, IndexDefinitionHolder indexDefinition) {

		Mono<IndexInfo> existingIndex = fetchIndexInformation(indexDefinition);

		Mono<String> defaultError = Mono.error(new DataIntegrityViolationException(
				String.format("Cannot create index for '%s' in collection '%s' with keys '%s' and options '%s'.",
						indexDefinition.getPath(), indexDefinition.getCollection(), indexDefinition.getIndexKeys(),
						indexDefinition.getIndexOptions()),
				e.getCause()));

		return existingIndex.flatMap(it -> {
			return Mono.<String> error(new DataIntegrityViolationException(
					String.format("Index already defined as '%s'.", indexDefinition.getPath()), e.getCause()));
		}).switchIfEmpty(defaultError);
	}

	private Mono<IndexInfo> fetchIndexInformation(IndexDefinitionHolder indexDefinition) {

		Object indexNameToLookUp = indexDefinition.getIndexOptions().get("name");

		Flux<IndexInfo> existingIndexes = operationsProvider.indexOps(indexDefinition.getCollection()).getIndexInfo();

		return existingIndexes //
				.filter(indexInfo -> ObjectUtils.nullSafeEquals(indexNameToLookUp, indexInfo.getName())) //
				.next() //
				.doOnError(e -> {
					LOGGER.debug(
							String.format("Failed to load index information for collection '%s'.", indexDefinition.getCollection()),
							e);
				});
	}

	private static boolean isDataIntegrityViolation(Throwable t) {

		if (t instanceof UncategorizedMongoDbException) {

			return t.getCause() instanceof MongoException
					&& MongoDbErrorCodes.isDataIntegrityViolationCode(((MongoException) t.getCause()).getCode());
		}

		return false;
	}
}
