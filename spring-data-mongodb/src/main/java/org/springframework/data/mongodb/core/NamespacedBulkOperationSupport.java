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
import java.util.function.Consumer;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.NamespaceBulkOperations.NamespaceAwareBulkOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.core.query.UpdateDefinition.ArrayFilter;
import org.springframework.data.util.Pair;
import org.springframework.util.Assert;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCluster;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientDeleteManyOptions;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.client.model.bulk.ClientReplaceOneOptions;
import com.mongodb.client.model.bulk.ClientUpdateManyOptions;
import com.mongodb.client.model.bulk.ClientUpdateOneOptions;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateManyOptions;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateOneOptions;

/**
 * @author Christoph Strobl
 */
class NamespacedBulkOperationSupport<T> implements NamespaceAwareBulkOperations<T> {

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
	public NamespaceAwareBulkOperations<T> insert(T document) {

		Assert.notNull(document, "Document must not be null");

		Namespace namespace = currentNamespace; // keep namespace steady
		maybeEmitEvent(new BeforeConvertEvent<>(document, namespace.collection()));
		Object source = maybeInvokeBeforeConvertCallback(document, namespace);
		Document mappedObject = getMappedObject(source);
		addModel(source, ClientNamespacedWriteModel.insertOne(mongoNamespace(namespace), mappedObject));

		return this;
	}

	@Override
	public NamespaceAwareBulkOperations<T> insert(List<? extends T> documents) {

		documents.forEach(this::insert);
		return this;
	}

	@Override
	public NamespaceAwareBulkOperations<T> updateOne(Query query, UpdateDefinition update) {
		update(currentNamespace, query, update, false, false);
		return this;
	}

	@Override
	public NamespaceAwareBulkOperations<T> updateOne(List<Pair<Query, UpdateDefinition>> updates) {

		for (Pair<Query, UpdateDefinition> update : updates) {
			update(currentNamespace, update.getFirst(), update.getSecond(), false, false);
		}

		return this;
	}

	@Override
	public NamespaceAwareBulkOperations<T> updateMulti(Query query, UpdateDefinition update) {

		update(currentNamespace, query, update, false, true);
		return this;
	}

	@Override
	public NamespaceAwareBulkOperations<T> updateMulti(List<Pair<Query, UpdateDefinition>> updates) {

		for (Pair<Query, UpdateDefinition> update : updates) {
			update(currentNamespace, update.getFirst(), update.getSecond(), false, true);
		}

		return this;
	}

	@Override
	public NamespaceAwareBulkOperations<T> upsert(Query query, UpdateDefinition update) {
		update(currentNamespace, query, update, true, true);
		return this;
	}

	@Override
	public NamespaceAwareBulkOperations<T> upsert(List<Pair<Query, Update>> updates) {
		for (Pair<Query, Update> update : updates) {
			upsert(update.getFirst(), update.getSecond());
		}
		return this;
	}

	@Override
	public NamespaceAwareBulkOperations<T> remove(Query query) {

		ClientDeleteManyOptions deleteOptions = ClientDeleteManyOptions.clientDeleteManyOptions();
		query.getCollation().map(Collation::toMongoCollation).ifPresent(deleteOptions::collation);

		addModel(query,
				ClientNamespacedWriteModel.deleteMany(mongoNamespace(currentNamespace), query.getQueryObject(), deleteOptions));

		return this;
	}

	@Override
	public NamespaceAwareBulkOperations<T> remove(List<Query> removes) {
		for (Query query : removes) {
			remove(query);
		}

		return this;
	}

	@Override
	public NamespaceAwareBulkOperations<T> replaceOne(Query query, Object replacement, FindAndReplaceOptions options) {
		Assert.notNull(query, "Query must not be null");
		Assert.notNull(replacement, "Replacement must not be null");
		Assert.notNull(options, "Options must not be null");

		ClientReplaceOneOptions replaceOptions = ClientReplaceOneOptions.clientReplaceOneOptions();
		replaceOptions.upsert(options.isUpsert());
		if (query.isSorted()) {
			replaceOptions.sort(query.getSortObject());
		}
		query.getCollation().map(Collation::toMongoCollation).ifPresent(replaceOptions::collation);

		Namespace ns = currentNamespace;
		maybeEmitEvent(new BeforeConvertEvent<>(replacement, ns.name()));
		Object source = maybeInvokeBeforeConvertCallback(replacement, ns);
		addModel(source, ClientNamespacedWriteModel.replaceOne(mongoNamespace(ns), query.getQueryObject(),
				getMappedObject(source), replaceOptions));

		return this;
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
	public <S> NamespaceAwareBulkOperations<S> inCollection(Class<S> type, Consumer<BulkOperationBase<S>> bulkActions) {
		NamespaceAwareBulkOperations<S> ops = inCollection(type);
		bulkActions.accept(ops);
		return ops;
	}

	@Override
	@SuppressWarnings("unchecked")
	public NamespaceAwareBulkOperations<Object> inCollection(String collection,
			Consumer<BulkOperationBase<Object>> bulkActions) {
		NamespaceAwareBulkOperations<Object> ops = inCollection(collection);
		bulkActions.accept(ops);
		return ops;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <S> NamespaceAwareBulkOperations<S> inCollection(Class<S> type) {
		return inCollection(operations.getCollectionName(type));
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public NamespaceAwareBulkOperations inCollection(String collection) {

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

	/**
	 * @param filterQuery The {@link Query} to read a potential {@link Collation} from. Must not be {@literal null}.
	 * @param update The {@link Update} to apply
	 * @param upsert flag to indicate if document should be upserted.
	 * @param multi flag to indicate if update might affect multiple documents.
	 * @return new instance of {@link UpdateOptions}.
	 */
	protected UpdateOptions computeUpdateOptions(Query filterQuery, UpdateDefinition update, boolean upsert,
			boolean multi) {

		UpdateOptions options = new UpdateOptions();
		options.upsert(upsert);

		if (update.hasArrayFilters()) {
			List<Document> list = new ArrayList<>(update.getArrayFilters().size());
			for (ArrayFilter arrayFilter : update.getArrayFilters()) {
				list.add(arrayFilter.asDocument());
			}
			options.arrayFilters(list);
		}

		if (!multi && filterQuery.isSorted()) {
			options.sort(filterQuery.getSortObject());
		}

		filterQuery.getCollation().map(Collation::toMongoCollation).ifPresent(options::collation);
		return options;
	}

	/**
	 * Performs update and upsert bulk operations.
	 *
	 * @param query the {@link Query} to determine documents to update.
	 * @param update the {@link Update} to perform, must not be {@literal null}.
	 * @param upsert whether to upsert.
	 * @param multi whether to issue a multi-update.
	 */
	private void update(Namespace namespace, Query query, UpdateDefinition update, boolean upsert, boolean multi) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");

		UpdateOptions options = computeUpdateOptions(query, update, upsert, multi);

		if (multi) {

			ClientUpdateManyOptions mayOptions = new ConcreteClientUpdateManyOptions();
			mayOptions.arrayFilters(options.getArrayFilters());
			mayOptions.collation(options.getCollation());
			mayOptions.upsert(options.isUpsert());
			mayOptions.hint(options.getHint());
			mayOptions.hintString(options.getHintString());

			addModel(update, ClientNamespacedWriteModel.updateMany(mongoNamespace(namespace), query.getQueryObject(),
					update.getUpdateObject(), mayOptions));
		} else {

			ClientUpdateOneOptions mayOptions = new ConcreteClientUpdateOneOptions();
			mayOptions.arrayFilters(options.getArrayFilters());
			mayOptions.collation(options.getCollation());
			mayOptions.upsert(options.isUpsert());
			mayOptions.hint(options.getHint());
			mayOptions.hintString(options.getHintString());
			addModel(update, ClientNamespacedWriteModel.updateOne(mongoNamespace(namespace), query.getQueryObject(),
					update.getUpdateObject(), mayOptions));
		}
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

		public String name() {
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
