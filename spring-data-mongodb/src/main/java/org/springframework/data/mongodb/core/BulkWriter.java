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

import java.util.Set;

import org.bson.Document;

import org.springframework.dao.DataAccessException;
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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;

/**
 * Internal API wrapping a {@link MongoTemplate} to encapsulate {@link Bulk} handling.
 *
 * @author Christoph Strobl
 * @since 5.1
 */
class BulkWriter extends BulkWriterSupport {

	private final MongoTemplate template;

	BulkWriter(MongoTemplate template) {
		super(template.getEntityOperations(), template.getQueryOperations(), template.getConverter().getMappingContext());
		this.template = template;
	}

	public BulkWriteResult write(String defaultDatabase, Bulk bulk, BulkWriteOptions options) {

		Set<TypedNamespace> namespaces = getTypedNamespaces(bulk);
		if (namespaces.size() == 1) {
			return writeToSingleCollection(defaultDatabase, bulk, options, namespaces.iterator().next());
		}
		return writeToMultipleCollections(defaultDatabase, bulk, options);
	}

	private BulkWriteResult writeToSingleCollection(String defaultDatabase, Bulk bulk,
			BulkWriteOptions options, TypedNamespace namespace) {

		MongoNamespace mongoNamespace = new MongoNamespace(defaultDatabase,
				resolveCollectionName(namespace));

		SingleCollectionCollector collector = new SingleCollectionCollector(mongoNamespace);
		buildWriteModels(bulk, collector);

		try {
			com.mongodb.bulk.BulkWriteResult bulkWriteResult = template.execute(collector.getNamespace().getCollectionName(),
					collection -> collection.bulkWrite(collector.getWriteModels(), new com.mongodb.client.model.BulkWriteOptions()
							.ordered(options.getOrder().equals(BulkWriteOptions.Order.ORDERED))));

			collector.getAfterSaveCallables().forEach(callable -> {
				template
						.maybeEmitEvent(new AfterSaveEvent<>(callable.source(), callable.document(), callable.collectionName()));
				template.maybeCallAfterSave(callable.source(), callable.document(), callable.collectionName());
			});
			return BulkWriteResult.from(bulkWriteResult);
		} catch (MongoBulkWriteException e) {
			DataAccessException dataAccessException = template.getExceptionTranslator().translateExceptionIfPossible(e);
			if (dataAccessException != null) {
				throw dataAccessException;
			}
			throw e;
		}
	}

	private BulkWriteResult writeToMultipleCollections(String defaultDatabase, Bulk bulk,
			BulkWriteOptions options) {

		MultiCollectionCollector collector = new MultiCollectionCollector(defaultDatabase);
		buildWriteModels(bulk, collector);

		try {

			ClientBulkWriteResult clientBulkWriteResult = template
					.doWithClient(client -> client.bulkWrite(collector.getWriteModels(), ClientBulkWriteOptions
							.clientBulkWriteOptions().ordered(options.getOrder().equals(BulkWriteOptions.Order.ORDERED))));

			collector.getAfterSaveCallables().forEach(callable -> {
				template
						.maybeEmitEvent(new AfterSaveEvent<>(callable.source(), callable.document(), callable.collectionName()));
				template.maybeCallAfterSave(callable.source(), callable.document(), callable.collectionName());
			});
			return BulkWriteResult.from(clientBulkWriteResult);
		} catch (MongoBulkWriteException e) {
			DataAccessException dataAccessException = template.getExceptionTranslator().translateExceptionIfPossible(e);
			if (dataAccessException != null) {
				throw dataAccessException;
			}
			throw e;
		}
	}

	private void buildWriteModels(Bulk bulk, WriteModelCollector collector) {

		for (BulkOperation bulkOp : bulk.operations()) {

			MongoNamespace namespace = collector.resolveNamespace(resolveCollectionName(bulkOp));
			MongoPersistentEntity<?> entity = getPersistentEntity(bulkOp.context());

			if (bulkOp instanceof Insert insert) {

				SourceAwareDocument<Object> sourceAwareDocument = template.prepareObjectForSave(namespace.getCollectionName(),
						insert.value());
				collector.addInsert(namespace, sourceAwareDocument.document(), sourceAwareDocument);
			} else if (bulkOp instanceof Update update) {

				boolean multi = !(bulkOp instanceof UpdateFirst);

				UpdateContext updateContext = queryOperations.updateContext(update.update(), update.query(),
						update.upsert());

				Document mappedQuery = updateContext.getMappedQuery(entity);
				Object mappedUpdate = updateContext.isAggregationUpdate() ? updateContext.getUpdatePipeline(entity)
						: updateContext.getMappedUpdate(entity);
				UpdateOptions updateOptions = updateContext.getUpdateOptions(entity, update.query());

				collector.addUpdate(namespace, multi, mappedQuery, mappedUpdate, updateOptions);
			} else if (bulkOp instanceof Remove remove) {

				DeleteContext deleteContext = queryOperations.deleteQueryContext(remove.query());
				Document mappedQuery = deleteContext.getMappedQuery(entity);
				DeleteOptions deleteOptions = deleteContext.getDeleteOptions(entity);

				collector.addRemove(namespace, remove instanceof RemoveFirst, mappedQuery, deleteOptions);
			} else if (bulkOp instanceof Replace replace) {

				SourceAwareDocument<Object> sourceAwareDocument = template.prepareObjectForSave(namespace.getCollectionName(),
						replace.replacement());

				UpdateContext updateContext = queryOperations.replaceSingleContext(replace.query(),
						MappedDocument.of(sourceAwareDocument.document()), replace.upsert());

				Document mappedQuery = updateContext.getMappedQuery(entity);
				UpdateOptions updateOptions = updateContext.getUpdateOptions(entity, replace.query());

				collector.addReplace(namespace, mappedQuery, sourceAwareDocument.document(), updateOptions,
						sourceAwareDocument);
			}
		}
	}

}
