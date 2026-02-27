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

import java.util.List;
import java.util.Set;

import org.bson.Document;

import org.springframework.data.mongodb.core.QueryOperations.DeleteContext;
import org.springframework.data.mongodb.core.QueryOperations.UpdateContext;
import org.springframework.data.mongodb.core.bulk.Bulk;
import org.springframework.data.mongodb.core.bulk.BulkOperation;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Insert;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Remove;
import org.springframework.data.mongodb.core.bulk.BulkOperation.RemoveFirst;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Replace;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Update;
import org.springframework.data.mongodb.core.bulk.BulkOperation.UpdateFirst;
import org.springframework.data.mongodb.core.bulk.BulkOperationContext.TypedNamespace;
import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import org.springframework.data.mongodb.core.bulk.BulkWriteResult;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;

/**
 * Internal API wrapping a {@link ReactiveMongoTemplate} to encapsulate {@link Bulk} handling using a reactive flow.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 5.1
 */
class ReactiveBulkWriter extends BulkWriterSupport {

	private final ReactiveMongoTemplate template;

	ReactiveBulkWriter(ReactiveMongoTemplate template) {

		super(template.getEntityOperations(), template.getQueryOperations(), template.getConverter().getMappingContext());
		this.template = template;
	}

	public Mono<BulkWriteResult> write(String defaultDatabase, Bulk bulk, BulkWriteOptions options) {

		Set<TypedNamespace> namespaces = getTypedNamespaces(bulk);
		if (namespaces.size() == 1) {
			return writeToSingleCollection(defaultDatabase, bulk, options, namespaces.iterator().next());
		}
		return writeToMultipleCollections(defaultDatabase, bulk, options);
	}

	private Mono<BulkWriteResult> writeToSingleCollection(String defaultDatabase, Bulk bulk,
			BulkWriteOptions options, TypedNamespace namespace) {

		MongoNamespace mongoNamespace = new MongoNamespace(defaultDatabase, resolveCollectionName(namespace));

		SingleCollectionCollector collector = new SingleCollectionCollector(mongoNamespace);
		return buildWriteModelsReactive(bulk, collector).then(Mono.defer(() -> {

			String collectionName = collector.getNamespace().getCollectionName();
			List<SourceAwareDocument<Object>> afterSaveCallables = collector.getAfterSaveCallables();

			return template
					.createMono(collectionName,
							col -> col.bulkWrite(collector.getWriteModels(),
									new com.mongodb.client.model.BulkWriteOptions()
											.ordered(options.getOrder().equals(BulkWriteOptions.Order.ORDERED))))
					.map(BulkWriteResult::from)
					.doOnSuccess(
							v -> afterSaveCallables
									.forEach(callable -> template.maybeEmitEvent(new AfterSaveEvent<>(callable.source(),
											callable.document(), callable.collectionName()))))
					.flatMap(result -> Flux.concat(afterSaveCallables.stream().map(callable -> template
							.maybeCallAfterSave(callable.source(), callable.document(), callable.collectionName())).toList())
							.then(Mono.just(result)));
		}));
	}

	private Mono<BulkWriteResult> writeToMultipleCollections(String defaultDatabase, Bulk bulk,
			BulkWriteOptions options) {

		MultiCollectionCollector collector = new MultiCollectionCollector(defaultDatabase);

		return buildWriteModelsReactive(bulk, collector).then(Mono.defer(() -> {

			List<ClientNamespacedWriteModel> writeModels = collector.getWriteModels();
			List<SourceAwareDocument<Object>> afterSaveCallables = collector.getAfterSaveCallables();

			return template
					.doWithCluster(client -> client.bulkWrite(writeModels,
							ClientBulkWriteOptions
									.clientBulkWriteOptions().ordered(options.getOrder().equals(BulkWriteOptions.Order.ORDERED))))
					.map(BulkWriteResult::from)
					.doOnSuccess(
							v -> afterSaveCallables
									.forEach(callable -> template.maybeEmitEvent(new AfterSaveEvent<>(callable.source(),
											callable.document(), callable.collectionName()))))
					.flatMap(result -> Flux.concat(afterSaveCallables.stream().map(callable -> template
							.maybeCallAfterSave(callable.source(), callable.document(), callable.collectionName())).toList())
							.then(Mono.just(result)));
		}));
	}

	private Mono<Void> buildWriteModelsReactive(Bulk bulk, WriteModelCollector collector) {
		return Flux.fromIterable(bulk.operations()).concatMap(bulkOp -> addOperationReactive(bulkOp, collector)).then();
	}

	private Mono<Void> addOperationReactive(BulkOperation bulkOp, WriteModelCollector collector) {

		MongoNamespace namespace = collector.resolveNamespace(resolveCollectionName(bulkOp));
		MongoPersistentEntity<?> entity = getPersistentEntity(bulkOp.context());

		if (bulkOp instanceof Insert insert) {

			return template
					.prepareObjectForSaveReactive(namespace.getCollectionName(), insert.value())
					.doOnNext(sad -> collector.addInsert(namespace, sad.document(), toObject(sad))).then();
		}

		if (bulkOp instanceof Update update) {

			boolean multi = !(bulkOp instanceof UpdateFirst);
			UpdateContext updateContext = queryOperations.updateContext(update.update(), update.query(),
					update.upsert());

			Document mappedQuery = updateContext.getMappedQuery(entity);
			Object mappedUpdate = updateContext.isAggregationUpdate() ? updateContext.getUpdatePipeline(entity)
					: updateContext.getMappedUpdate(entity);
			UpdateOptions updateOptions = updateContext.getUpdateOptions(entity, update.query());

			collector.addUpdate(namespace, multi, mappedQuery, mappedUpdate, updateOptions);
			return Mono.empty();
		}

		if (bulkOp instanceof Remove remove) {

			DeleteContext deleteContext = queryOperations.deleteQueryContext(remove.query());

			Document mappedQuery = deleteContext.getMappedQuery(entity);
			DeleteOptions deleteOptions = deleteContext.getDeleteOptions(entity);

			collector.addRemove(namespace, remove instanceof RemoveFirst, mappedQuery, deleteOptions);
			return Mono.empty();
		}

		if (bulkOp instanceof Replace replace) {

			return template
					.prepareObjectForSaveReactive(namespace.getCollectionName(), replace.replacement())
					.doOnNext(sad -> {

						UpdateContext updateContext = queryOperations.replaceSingleContext(replace.query(),
								MappedDocument.of(sad.document()), replace.upsert());

						Document mappedQuery = updateContext
								.getMappedQuery(entity);
						UpdateOptions updateOptions = updateContext.getUpdateOptions(entity,
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

}
