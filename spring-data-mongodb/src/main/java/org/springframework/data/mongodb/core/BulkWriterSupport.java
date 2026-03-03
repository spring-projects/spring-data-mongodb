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
import java.util.stream.Collectors;

import org.bson.Document;
import org.jspecify.annotations.Nullable;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.bulk.Bulk;
import org.springframework.data.mongodb.core.bulk.BulkOperation;
import org.springframework.data.mongodb.core.bulk.BulkOperationContext;
import org.springframework.data.mongodb.core.bulk.BulkOperationContext.TypedNamespace;
import org.springframework.data.mongodb.core.mapping.CollectionName;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;

/**
 * Base class for {@link BulkWriter} and {@link ReactiveBulkWriter}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 5.1
 */
abstract class BulkWriterSupport {

	final EntityOperations entityOperations;
	final QueryOperations queryOperations;
	final MappingContext<? extends MongoPersistentEntity<?>, ? extends MongoPersistentProperty> mappingContext;

	public BulkWriterSupport(EntityOperations entityOperations, QueryOperations queryOperations,
			MappingContext<? extends MongoPersistentEntity<?>, ? extends MongoPersistentProperty> mappingContext) {
		this.entityOperations = entityOperations;
		this.queryOperations = queryOperations;
		this.mappingContext = mappingContext;
	}

	static Set<TypedNamespace> getTypedNamespaces(Bulk bulk) {
		return bulk.operations().stream().map(it -> it.context().namespace()).collect(Collectors.toSet());
	}

	String resolveCollectionName(BulkOperation operation) {
		return resolveCollectionName(operation.context().namespace());
	}

	String resolveCollectionName(TypedNamespace namespace) {

		if (namespace.hasCollectionName()) {
			return namespace.getRequiredCollectionName().getCollectionName(entityOperations::getRequiredPersistentEntity);
		}
		return entityOperations.determineCollectionName(namespace.type());
	}

	@Nullable
	@SuppressWarnings("unchecked")
	MongoPersistentEntity<?> getPersistentEntity(BulkOperationContext context) {

		BulkOperationContext.TypedNamespace namespace = context.namespace();

		if (namespace.type() != null) {
			return mappingContext.getPersistentEntity(namespace.type());
		}

		if (namespace.hasCollectionName()) {
			CollectionName collectionName = namespace.getRequiredCollectionName();
			if (collectionName.getEntityClass() != Object.class) {
				return mappingContext.getPersistentEntity(collectionName.getEntityClass());
			}
		}

		return null;
	}

	/**
	 * Strategy interface to collect {@link WriteModel}s for a {@link Bulk} operation.
	 */
	interface WriteModelCollector {

		List<SourceAwareDocument<Object>> getAfterSaveCallables();

		MongoNamespace resolveNamespace(String collectionName);

		void addInsert(MongoNamespace namespace, Document document, SourceAwareDocument<Object> sourceDoc);

		void addUpdate(MongoNamespace namespace, boolean multi, Document query, Object update, UpdateOptions options);

		void addRemove(MongoNamespace namespace, boolean removeFirst, Document query, DeleteOptions options);

		void addReplace(MongoNamespace namespace, Document query, Document replacement, UpdateOptions options,
				SourceAwareDocument<Object> sourceDoc);

	}

	/**
	 * Collector for single-collection bulk operations.
	 */
	static class SingleCollectionCollector implements WriteModelCollector {

		private final List<WriteModel<Document>> writeModels = new ArrayList<>();
		private final List<SourceAwareDocument<Object>> afterSaveCallables = new ArrayList<>();
		private final MongoNamespace namespace;

		public SingleCollectionCollector(MongoNamespace namespace) {
			this.namespace = namespace;
		}

		MongoNamespace getNamespace() {
			return namespace;
		}

		List<WriteModel<Document>> getWriteModels() {
			return writeModels;
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

	}

	/**
	 * Collector for multi-collection bulk operations.
	 */
	static class MultiCollectionCollector implements WriteModelCollector {

		private final List<ClientNamespacedWriteModel> writeModels = new ArrayList<>();
		private final List<SourceAwareDocument<Object>> afterSaveCallables = new ArrayList<>();
		private final String defaultDatabaseName;

		public MultiCollectionCollector(String defaultDatabaseName) {
			this.defaultDatabaseName = defaultDatabaseName;
		}

		@Override
		public List<SourceAwareDocument<Object>> getAfterSaveCallables() {
			return afterSaveCallables;
		}

		List<ClientNamespacedWriteModel> getWriteModels() {
			return writeModels;
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

	}

}
