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

import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

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
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;

/**
 * Internal API wrapping a {@link ReactiveMongoTemplate} to encapsulate {@link Bulk} handling using a reactive flow.
 *
 * @author Christoph Strobl
 * @since 2026/02
 */
class ReactiveBulkWriter {

	ReactiveMongoTemplate template;

	ReactiveBulkWriter(ReactiveMongoTemplate template) {
		this.template = template;
	}

	public Mono<ClientBulkWriteResult> write(String defaultDatabase, Bulk bulk, BulkWriteOptions options) {

		return Flux.fromIterable(bulk.operations()).concatMap(bulkOp -> toWriteModelAndAfterSave(defaultDatabase, bulkOp))
				.collectList().flatMap(results -> {

					List<ClientNamespacedWriteModel> writeModels = new ArrayList<>();
					List<SourceAwareDocument<?>> afterSaveCallables = new ArrayList<>();

					for (WriteModelAndAfterSave result : results) {
						writeModels.add(result.model());
						if (result.afterSave() != null) {
							afterSaveCallables.add(result.afterSave());
						}
					}

					return template
							.doWithClient(client -> client.bulkWrite(writeModels,
									ClientBulkWriteOptions.clientBulkWriteOptions().ordered(options.getOrder().equals(BulkWriteOptions.Order.ORDERED))))
							.doOnSuccess(v -> afterSaveCallables.forEach(callable -> {
								template.maybeEmitEvent(
										new AfterSaveEvent<>(callable.source(), callable.document(), callable.collectionName()));

							}))
							.flatMap(v -> Flux.concat(afterSaveCallables.stream().map(callable -> template
									.maybeCallAfterSave(callable.source(), callable.document(), callable.collectionName())).toList())
									.then(Mono.just(v)));
				});
	}

	private Mono<WriteModelAndAfterSave> toWriteModelAndAfterSave(String defaultDatabase, BulkOperation bulkOp) {

		String collectionName = bulkOp.context().namespace().collection() != null
				? bulkOp.context().namespace().collection()
				: template.getCollectionName(bulkOp.context().namespace().type());

		MongoNamespace mongoNamespace = new MongoNamespace(defaultDatabase, collectionName);

		if (bulkOp instanceof Insert insert) {

			return template.prepareObjectForSaveReactive(collectionName, insert.value(), template.getConverter())
					.map(sourceAwareDocument -> {
						ClientNamespacedWriteModel model = ClientNamespacedWriteModel.insertOne(mongoNamespace,
								sourceAwareDocument.document());
						return new WriteModelAndAfterSave(model, sourceAwareDocument);
					});
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

			if (multi) {

				ClientNamespacedWriteModel model = BulkWriteSupport.updateMany(mongoNamespace, mappedQuery, mappedUpdate,
						updateOptions);
				return Mono.just(new WriteModelAndAfterSave(model, null));
			}
			ClientNamespacedWriteModel model = BulkWriteSupport.updateOne(mongoNamespace, mappedQuery, mappedUpdate,
					updateOptions);
			return Mono.just(new WriteModelAndAfterSave(model, null));
		}

		if (bulkOp instanceof Remove remove) {

			Class<?> domainType = remove.context().namespace().type();
			DeleteContext deleteContext = template.getQueryOperations().deleteQueryContext(remove.query());

			Document mappedQuery = deleteContext.getMappedQuery(template.getPersistentEntity(domainType));
			DeleteOptions deleteOptions = deleteContext.getDeleteOptions(domainType);

			if (remove instanceof RemoveFirst) {
				ClientNamespacedWriteModel model = BulkWriteSupport.removeOne(mongoNamespace, mappedQuery, deleteOptions);
				return Mono.just(new WriteModelAndAfterSave(model, null));
			} else {
				ClientNamespacedWriteModel model = BulkWriteSupport.removeMany(mongoNamespace, mappedQuery, deleteOptions);
				return Mono.just(new WriteModelAndAfterSave(model, null));
			}
		}

		if (bulkOp instanceof Replace replace) {

			return template.prepareObjectForSaveReactive(collectionName, replace.replacement(), template.getConverter())
					.map(sourceAwareDocument -> {

						UpdateContext updateContext = template.getQueryOperations().replaceSingleContext(replace.query(),
								MappedDocument.of(sourceAwareDocument.document()), replace.upsert());

						Document mappedQuery = updateContext
								.getMappedQuery(template.getPersistentEntity(replace.context().namespace().type()));
						UpdateOptions updateOptions = updateContext.getUpdateOptions(replace.context().namespace().type(),
								replace.query());

						ClientNamespacedWriteModel model = BulkWriteSupport.replaceOne(mongoNamespace, mappedQuery,
								sourceAwareDocument.document(), updateOptions);
						return new WriteModelAndAfterSave(model, sourceAwareDocument);
					});
		}

		return Mono.error(new IllegalStateException("Unknown bulk operation type: " + bulkOp.getClass()));
	}

	private record WriteModelAndAfterSave(ClientNamespacedWriteModel model, SourceAwareDocument<?> afterSave) {
	}
}
