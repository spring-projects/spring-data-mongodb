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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.MongoTemplate.SourceAwareDocument;
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
import org.springframework.data.mongodb.core.bulk.BulkOperationResult;
import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.util.StringUtils;

import com.mongodb.MongoBulkWriteException;
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
 * Internal API wrapping a {@link MongoTemplate} to encapsulate {@link Bulk} handling.
 *
 * @author Christoph Strobl
 * @since 5.1
 */
class BulkWriter {

	MongoTemplate template;

	BulkWriter(MongoTemplate template) {
		this.template = template;
	}

	public BulkOperationResult<?> write(String defaultDatabase, Bulk bulk, BulkWriteOptions options) {

		Set<TypedNamespace> namespaces = bulk.operations().stream().map(it -> it.context().namespace())
				.collect(Collectors.toSet());
		if (namespaces.size() == 1) {
			return writeToSingleCollection(defaultDatabase, bulk, options, namespaces.iterator().next());
		}
		return writeToMultipleCollections(defaultDatabase, bulk, options);
	}

	private BulkOperationResult<BulkWriteResult> writeToSingleCollection(String defaultDatabase, Bulk bulk,
			BulkWriteOptions options, TypedNamespace namespace) {

		MongoNamespace mongoNamespace = new MongoNamespace(defaultDatabase,
				StringUtils.hasText(namespace.collection()) ? namespace.collection()
						: template.getCollectionName(namespace.type()));

		SingleCollectionCollector collector = new SingleCollectionCollector(mongoNamespace);
		buildWriteModels(bulk, collector);

		try {
			BulkWriteResult bulkWriteResult = template.execute(collector.getNamespace().getCollectionName(),
					collection -> collection.bulkWrite(collector.getWriteModels(), new com.mongodb.client.model.BulkWriteOptions()
							.ordered(options.getOrder().equals(BulkWriteOptions.Order.ORDERED))));

			collector.getAfterSaveCallables().forEach(callable -> {
				template
						.maybeEmitEvent(new AfterSaveEvent<>(callable.source(), callable.document(), callable.collectionName()));
				template.maybeCallAfterSave(callable.source(), callable.document(), callable.collectionName());
			});
			return BulkOperationResult.from(bulkWriteResult);
		} catch (MongoBulkWriteException e) {
			DataAccessException dataAccessException = template.getExceptionTranslator().translateExceptionIfPossible(e);
			if (dataAccessException != null) {
				throw dataAccessException;
			}
			throw e;
		}
	}

	private BulkOperationResult<ClientBulkWriteResult> writeToMultipleCollections(String defaultDatabase, Bulk bulk,
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
			return BulkOperationResult.from(clientBulkWriteResult);
		} catch (MongoBulkWriteException e) {
			DataAccessException dataAccessException = template.getExceptionTranslator().translateExceptionIfPossible(e);
			if (dataAccessException != null) {
				throw dataAccessException;
			}
			throw e;
		}
	}

	private void buildWriteModels(Bulk bulk, WriteModelCollector<?> collector) {

		for (BulkOperation bulkOp : bulk.operations()) {

			MongoNamespace namespace = collector.resolveNamespace(bulkOp, it -> template.getCollectionName(it.type()));

			if (bulkOp instanceof Insert insert) {

				SourceAwareDocument<Object> sourceAwareDocument = template.prepareObjectForSave(namespace.getCollectionName(),
						insert.value(), template.getConverter());
				collector.addInsert(namespace, sourceAwareDocument.document(), sourceAwareDocument);
			} else if (bulkOp instanceof Update update) {

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
			} else if (bulkOp instanceof Remove remove) {

				Class<?> domainType = remove.context().namespace().type();
				DeleteContext deleteContext = template.getQueryOperations().deleteQueryContext(remove.query());

				Document mappedQuery = deleteContext.getMappedQuery(template.getPersistentEntity(domainType));
				DeleteOptions deleteOptions = deleteContext.getDeleteOptions(domainType);

				collector.addRemove(namespace, remove instanceof RemoveFirst, mappedQuery, deleteOptions);
			} else if (bulkOp instanceof Replace replace) {

				Class<?> domainType = replace.context().namespace().type();

				SourceAwareDocument<Object> sourceAwareDocument = template.prepareObjectForSave(namespace.getCollectionName(),
						replace.replacement(), template.getConverter());

				UpdateContext updateContext = template.getQueryOperations().replaceSingleContext(replace.query(),
						MappedDocument.of(sourceAwareDocument.document()), replace.upsert());

				Document mappedQuery = updateContext.getMappedQuery(template.getPersistentEntity(domainType));
				UpdateOptions updateOptions = updateContext.getUpdateOptions(domainType, replace.query());

				collector.addReplace(namespace, mappedQuery, sourceAwareDocument.document(), updateOptions,
						sourceAwareDocument);
			}
		}
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
		private MongoNamespace namespace;

		public SingleCollectionCollector(MongoNamespace namespace) {
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

		public MultiCollectionCollector(String defaultDatabaseName) {
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
