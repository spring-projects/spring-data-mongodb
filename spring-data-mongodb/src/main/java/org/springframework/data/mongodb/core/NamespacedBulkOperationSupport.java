/*
 * Copyright 2026. the original author or authors.
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

/*
 * Copyright 2026 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.NamespaceBulkOperations.NamespaceAwareBulkIOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.util.Pair;
import org.springframework.util.Assert;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCluster;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;

/**
 * @author Christoph Strobl
 */
class NamespacedBulkOperationSupport implements NamespaceAwareBulkIOperations {

	private final BulkMode bulkMode;
	private final NamespacedBulkOperationContext ctx;
	private final MongoOperations operations;
	private Namespace currentNamespace;
	private final List<SourceAwareWriteModelHolder> models = new ArrayList<>();
	private final Map<Namespace, MongoNamespace> namespaces = new HashMap<>();

	public NamespacedBulkOperationSupport(BulkMode mode, NamespacedBulkOperationContext ctx, MongoOperations operations) {

		this.bulkMode = mode;
		this.ctx = ctx;
		this.currentNamespace = new Namespace(ctx.database(), null);
		this.operations = operations;
	}

	@Override
	public NamespaceAwareBulkIOperations insert(Object document) {

		Assert.notNull(document, "Document must not be null");

		Namespace namespace = currentNamespace; // keep namespace steady
		maybeEmitEvent(new BeforeConvertEvent<>(document, namespace.collection()));
		Object source = maybeInvokeBeforeConvertCallback(document, namespace);
		Document mappedObject = getMappedObject(source);
		addModel(source, ClientNamespacedWriteModel.insertOne(mongoNamespace(namespace), mappedObject));

		return this;
	}

	@Override
	public NamespaceAwareBulkIOperations insert(List<?> documents) {

		documents.forEach(this::insert);
		return this;
	}

	@Override
	public BulkOperations updateOne(Query query, UpdateDefinition update) {
		return null;
	}

	@Override
	public BulkOperations updateOne(List<Pair<Query, UpdateDefinition>> updates) {
		return null;
	}

	@Override
	public BulkOperations updateMulti(Query query, UpdateDefinition update) {
		return null;
	}

	@Override
	public BulkOperations updateMulti(List<Pair<Query, UpdateDefinition>> updates) {
		return null;
	}

	@Override
	public BulkOperations upsert(Query query, UpdateDefinition update) {
		return null;
	}

	@Override
	public BulkOperations upsert(List<Pair<Query, Update>> updates) {
		return null;
	}

	@Override
	public BulkOperations remove(Query remove) {
		return null;
	}

	@Override
	public BulkOperations remove(List<Query> removes) {
		return null;
	}

	@Override
	public BulkOperations replaceOne(Query query, Object replacement, FindAndReplaceOptions options) {
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public ClientBulkWriteResult execute() {

		return operations.doWithClient(new MongoClusterCallback<>() {

			@Override
			public ClientBulkWriteResult doWithClient(MongoCluster cluster) throws MongoException, DataAccessException {

				ClientBulkWriteOptions cbws = ClientBulkWriteOptions.clientBulkWriteOptions()
						.ordered(NamespacedBulkOperationSupport.this.bulkMode.equals(BulkMode.ORDERED));

				return cluster.bulkWrite(models.stream().map(SourceAwareWriteModelHolder::model).toList(), cbws);
			}
		});

	}

	@Override
	public NamespaceAwareBulkIOperations inCollection(Class<?> type) {
		return inCollection(operations.getCollectionName(type));
	}

	@Override
	public NamespaceAwareBulkIOperations inCollection(String collection) {

		this.currentNamespace = new Namespace(currentNamespace.database(), collection);
		return this;
	}

	@Override
	public NamespaceBulkOperations switchDatabase(String databaseName) {

		this.currentNamespace = new Namespace(currentNamespace.database(), null);
		return this;
	}

	protected void maybeEmitEvent(ApplicationEvent event) {
		ctx.publishEvent(event);
	}

	private Document getMappedObject(Object source) {

		if (source instanceof Document document) {
			return document;
		}

		Document sink = new Document();
		ctx.mongoConverter().write(source, sink);
		return sink;
	}

	private Object maybeInvokeBeforeConvertCallback(Object value, Namespace namespace) {
		return ctx.callback(BeforeConvertCallback.class, value, namespace);
	}

	private void addModel(Object source, ClientNamespacedWriteModel model) {
		models.add(new SourceAwareWriteModelHolder(source, model));
	}

	MongoNamespace mongoNamespace(Namespace namespace) {
		return namespaces.computeIfAbsent(namespace, key -> new MongoNamespace(key.database(), key.collection()));
	}

	record Namespace(String database, @Nullable String collection) {

		public Object name() {
			return String.format("%s.%s", database, collection != null ? collection : "n/a");
		}
	}

	/**
	 * Value object chaining together an actual source with its {@link WriteModel} representation.
	 *
	 * @author Christoph Strobl
	 */
	record SourceAwareWriteModelHolder(Object source, ClientNamespacedWriteModel model) {

	}

	record NamespacedBulkOperationContext(String database, MongoConverter mongoConverter, QueryMapper queryMapper,
			UpdateMapper updateMapper, @Nullable ApplicationEventPublisher eventPublisher,
			@Nullable EntityCallbacks entityCallbacks) {

		public boolean skipEntityCallbacks() {
			return entityCallbacks == null;
		}

		public boolean skipEventPublishing() {
			return eventPublisher == null;
		}

		@SuppressWarnings({ "rawtypes", "NullAway" })
		public <T> T callback(Class<? extends EntityCallback> callbackType, T entity, Namespace namespace) {

			if (skipEntityCallbacks()) {
				return entity;
			}

			return entityCallbacks.callback(callbackType, entity, namespace.name());
		}

		@SuppressWarnings({ "rawtypes", "NullAway" })
		public <T> T callback(Class<? extends EntityCallback> callbackType, T entity, Document document,
				Namespace namespace) {

			if (skipEntityCallbacks()) {
				return entity;
			}

			return entityCallbacks.callback(callbackType, entity, document, namespace.name());
		}

		@SuppressWarnings("NullAway")
		public void publishEvent(ApplicationEvent event) {

			if (skipEventPublishing()) {
				return;
			}

			eventPublisher.publishEvent(event);
		}
	}

}
