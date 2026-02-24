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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.mongodb.core.QueryOperations.DeleteContext;
import org.springframework.data.mongodb.core.QueryOperations.UpdateContext;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate.SourceAwareDocument;
import org.springframework.data.mongodb.core.bulk.Bulk;
import org.springframework.data.mongodb.core.bulk.BulkOperation;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Insert;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Remove;
import org.springframework.data.mongodb.core.bulk.BulkOperation.RemoveFirst;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Replace;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Update;
import org.springframework.data.mongodb.core.bulk.BulkOperation.UpdateFirst;
import org.springframework.data.mongodb.core.bulk.BulkOperationContext.TypedNamespace;
import org.springframework.data.mongodb.core.bulk.BulkOperationResult;
import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.util.StringUtils;

import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;

/**
 * Internal API wrapping a {@link ReactiveMongoTemplate} to encapsulate {@link Bulk} handling using a reactive flow.
 *
 * @author Christoph Strobl
 * @since 5.1
 */
class ReactiveBulkWriter {

	ReactiveMongoTemplate template;

	ReactiveBulkWriter(ReactiveMongoTemplate template) {
		this.template = template;
	}

	public Mono<BulkOperationResult<?>> write(String defaultDatabase, Bulk bulk, BulkWriteOptions options) {

		Set<TypedNamespace> namespaces = bulk.operations().stream().map(it -> it.context().namespace())
				.collect(Collectors.toSet());
		if (namespaces.size() == 1) {
			return writeToSingleCollection(defaultDatabase, bulk, options, namespaces.iterator().next())
					.map(r -> (BulkOperationResult<?>) r);
		}
		return writeToMultipleCollections(defaultDatabase, bulk, options).map(r -> (BulkOperationResult<?>) r);
	}

	private Mono<BulkOperationResult<BulkWriteResult>> writeToSingleCollection(String defaultDatabase, Bulk bulk,
			BulkWriteOptions options, TypedNamespace namespace) {

		MongoNamespace mongoNamespace = new MongoNamespace(defaultDatabase,
				StringUtils.hasText(namespace.collection()) ? namespace.collection()
						: template.getCollectionName(namespace.type()));

		SingleCollectionCollector collector = new SingleCollectionCollector(mongoNamespace);
		return buildWriteModelsReactive(bulk, collector).then(Mono.defer(() -> {

			String collectionName = collector.getNamespace().getCollectionName();
			List<SourceAwareDocument<Object>> afterSaveCallables = collector.getAfterSaveCallables();

			return template
					.createMono(collectionName,
							col -> col.bulkWrite(collector.getWriteModels(),
									new com.mongodb.client.model.BulkWriteOptions()
											.ordered(options.getOrder().equals(BulkWriteOptions.Order.ORDERED))))
					.map(
							BulkOperationResult::from)
					.doOnSuccess(
							v -> afterSaveCallables
									.forEach(callable -> template.maybeEmitEvent(new AfterSaveEvent<>(callable.source(),
											callable.document(), callable.collectionName()))))
					.flatMap(result -> Flux.concat(afterSaveCallables.stream().map(callable -> template
							.maybeCallAfterSave(callable.source(), callable.document(), callable.collectionName())).toList())
							.then(Mono.just(result)));
		}));
	}

	private Mono<BulkOperationResult<ClientBulkWriteResult>> writeToMultipleCollections(String defaultDatabase, Bulk bulk,
			BulkWriteOptions options) {

		MultiCollectionCollector collector = new MultiCollectionCollector(defaultDatabase);
		return buildWriteModelsReactive(bulk, collector).then(Mono.defer(() -> {

			List<ClientNamespacedWriteModel> writeModels = collector.getWriteModels();
			List<SourceAwareDocument<Object>> afterSaveCallables = collector.getAfterSaveCallables();

			return template
					.doWithClient(client -> client.bulkWrite(writeModels,
							ClientBulkWriteOptions
									.clientBulkWriteOptions().ordered(options.getOrder().equals(BulkWriteOptions.Order.ORDERED))))
					.map(
							BulkOperationResult::from)
					.doOnSuccess(
							v -> afterSaveCallables
									.forEach(callable -> template.maybeEmitEvent(new AfterSaveEvent<>(callable.source(),
											callable.document(), callable.collectionName()))))
					.flatMap(result -> Flux.concat(afterSaveCallables.stream().map(callable -> template
							.maybeCallAfterSave(callable.source(), callable.document(), callable.collectionName())).toList())
							.then(Mono.just(result)));
		}));
	}

	private Mono<Void> buildWriteModelsReactive(Bulk bulk, WriteModelCollector<?> collector) {
		return Flux.fromIterable(bulk.operations()).concatMap(bulkOp -> addOperationReactive(bulkOp, collector)).then();
	}

	private Mono<Void> addOperationReactive(BulkOperation bulkOp, WriteModelCollector<?> collector) {

		MongoNamespace namespace = collector.resolveNamespace(bulkOp, ns -> template.getCollectionName(ns.type()));

		if (bulkOp instanceof Insert insert) {

			return template
					.prepareObjectForSaveReactive(namespace.getCollectionName(), insert.value(), template.getConverter())
					.doOnNext(sad -> collector.addInsert(namespace, sad.document(), toObject(sad))).then();
		}

		if (bulkOp instanceof Update update) {

			Class<?> domainType = update.context().namespace().type();
			boolean multi = !(bulkOp instanceof UpdateFirst);

			UpdateContext updateContext = template.getQueryOperations().updateContext(update.update(), update.query(),
					update.upsert());
			MongoPersistentEntity<?> entity = template.getPersistentEntity(domainType);

			Document mappedQuery = updateContext.getMappedQuery(entity);
			Object mappedUpdate = updateContext.isAggregationUpdate() ? updateContext.getUpdatePipeline(domainType)
					: updateContext.getMappedUpdate(entity);
			UpdateOptions updateOptions = updateContext.getUpdateOptions(domainType, update.query());

			collector.addUpdate(namespace, multi, mappedQuery, mappedUpdate, updateOptions);
			return Mono.empty();
		}

		if (bulkOp instanceof Remove remove) {

			Class<?> domainType = remove.context().namespace().type();
			DeleteContext deleteContext = template.getQueryOperations().deleteQueryContext(remove.query());

			Document mappedQuery = deleteContext.getMappedQuery(template.getPersistentEntity(domainType));
			DeleteOptions deleteOptions = deleteContext.getDeleteOptions(domainType);

			collector.addRemove(namespace, remove instanceof RemoveFirst, mappedQuery, deleteOptions);
			return Mono.empty();
		}

		if (bulkOp instanceof Replace replace) {

			return template
					.prepareObjectForSaveReactive(namespace.getCollectionName(), replace.replacement(), template.getConverter())
					.doOnNext(sad -> {

						UpdateContext updateContext = template.getQueryOperations().replaceSingleContext(replace.query(),
								MappedDocument.of(sad.document()), replace.upsert());

						Document mappedQuery = updateContext
								.getMappedQuery(template.getPersistentEntity(replace.context().namespace().type()));
						UpdateOptions updateOptions = updateContext.getUpdateOptions(replace.context().namespace().type(),
								replace.query());

						collector.addReplace(namespace, mappedQuery, sad.document(), updateOptions, toObject(sad));
					}).then();
		}

		return Mono.error(new IllegalStateException("Unknown bulk operation type: " + bulkOp.getClass()));
	}

	@SuppressWarnings("unchecked")
	private static SourceAwareDocument<Object> toObject(SourceAwareDocument<?> sad) {
		return (SourceAwareDocument<Object>) sad;
	}

	private interface WriteModelCollector<T> {

		MongoNamespace resolveNamespace(String collectionName);

		default MongoNamespace resolveNamespace(BulkOperation operation, Function<TypedNamespace, String> fallback) {

			TypedNamespace typedNamespace = operation.context().namespace();
			if (StringUtils.hasText(typedNamespace.collection())) {
				return resolveNamespace(typedNamespace.collection());
			}
			return resolveNamespace(fallback.apply(typedNamespace));
		}

		void addInsert(MongoNamespace namespace, Document document, SourceAwareDocument<Object> sourceDoc);

		void addUpdate(MongoNamespace namespace, boolean multi, Document query, Object update, UpdateOptions options);

		void addRemove(MongoNamespace namespace, boolean removeFirst, Document query, DeleteOptions options);

		void addReplace(MongoNamespace namespace, Document query, Document replacement, UpdateOptions options,
				SourceAwareDocument<Object> sourceDoc);

		List<SourceAwareDocument<Object>> getAfterSaveCallables();
	}

	private static class SingleCollectionCollector implements WriteModelCollector<WriteModel<Document>> {

		private final List<WriteModel<Document>> writeModels = new ArrayList<>();
		private final List<SourceAwareDocument<Object>> afterSaveCallables = new ArrayList<>();
		private final MongoNamespace namespace;

		SingleCollectionCollector(MongoNamespace namespace) {
			this.namespace = namespace;
		}

		@Override
		public MongoNamespace resolveNamespace(String collectionName) {
			return namespace;
		}

		@Override
		public void addInsert(MongoNamespace namespace, Document document, SourceAwareDocument<Object> sourceDoc) {
			writeModels.add(new InsertOneModel<>(document));
			afterSaveCallables.add(sourceDoc);
		}

		@Override
		public void addUpdate(MongoNamespace namespace, boolean multi, Document query, Object update,
				UpdateOptions options) {
			if (multi) {
				writeModels.add(BulkWriteSupport.updateMany(query, update, options));
			} else {
				writeModels.add(BulkWriteSupport.updateOne(query, update, options));
			}
		}

		@Override
		public void addRemove(MongoNamespace namespace, boolean removeFirst, Document query, DeleteOptions options) {
			if (removeFirst) {
				writeModels.add(BulkWriteSupport.removeOne(query, options));
			} else {
				writeModels.add(BulkWriteSupport.removeMany(query, options));
			}
		}

		@Override
		public void addReplace(MongoNamespace namespace, Document query, Document replacement, UpdateOptions options,
				SourceAwareDocument<Object> sourceDoc) {
			writeModels.add(BulkWriteSupport.replaceOne(query, replacement, options));
			afterSaveCallables.add(sourceDoc);
		}

		@Override
		public List<SourceAwareDocument<Object>> getAfterSaveCallables() {
			return afterSaveCallables;
		}

		MongoNamespace getNamespace() {
			return namespace;
		}

		List<WriteModel<Document>> getWriteModels() {
			return writeModels;
		}
	}

	private static class MultiCollectionCollector implements WriteModelCollector<ClientNamespacedWriteModel> {

		private final List<ClientNamespacedWriteModel> writeModels = new ArrayList<>();
		private final List<SourceAwareDocument<Object>> afterSaveCallables = new ArrayList<>();
		private final String defaultDatabaseName;

		MultiCollectionCollector(String defaultDatabaseName) {
			this.defaultDatabaseName = defaultDatabaseName;
		}

		@Override
		public MongoNamespace resolveNamespace(String collectionName) {
			return new MongoNamespace(defaultDatabaseName, collectionName);
		}

		@Override
		public void addInsert(MongoNamespace namespace, Document document, SourceAwareDocument<Object> sourceDoc) {
			writeModels.add(ClientNamespacedWriteModel.insertOne(namespace, document));
			afterSaveCallables.add(sourceDoc);
		}

		@Override
		public void addUpdate(MongoNamespace namespace, boolean multi, Document query, Object update,
				UpdateOptions options) {
			if (multi) {
				writeModels.add(BulkWriteSupport.updateMany(namespace, query, update, options));
			} else {
				writeModels.add(BulkWriteSupport.updateOne(namespace, query, update, options));
			}
		}

		@Override
		public void addRemove(MongoNamespace namespace, boolean removeFirst, Document query, DeleteOptions options) {
			if (removeFirst) {
				writeModels.add(BulkWriteSupport.removeOne(namespace, query, options));
			} else {
				writeModels.add(BulkWriteSupport.removeMany(namespace, query, options));
			}
		}

		@Override
		public void addReplace(MongoNamespace namespace, Document query, Document replacement, UpdateOptions options,
				SourceAwareDocument<Object> sourceDoc) {
			writeModels.add(BulkWriteSupport.replaceOne(namespace, query, replacement, options));
			afterSaveCallables.add(sourceDoc);
		}

		@Override
		public List<SourceAwareDocument<Object>> getAfterSaveCallables() {
			return afterSaveCallables;
		}

		List<ClientNamespacedWriteModel> getWriteModels() {
			return writeModels;
		}
	}
}
