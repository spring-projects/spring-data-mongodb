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
import java.util.function.Consumer;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.NamespaceBulkOperations.NamespaceAwareBulkOperations;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.aggregation.FieldLookupPolicy;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.core.query.UpdateDefinition.ArrayFilter;
import org.springframework.data.util.Pair;
import org.springframework.lang.CheckReturnValue;
import org.springframework.util.Assert;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCluster;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientDeleteManyOptions;
import com.mongodb.client.model.bulk.ClientNamespacedInsertOneModel;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.client.model.bulk.ClientReplaceOneOptions;
import com.mongodb.client.model.bulk.ClientUpdateOneOptions;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateManyOptions;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateOneOptions;

/**
 * NOT THREAD SAFE!!!
 * 
 * @author Christoph Strobl
 */
class NamespacedBulkOperationSupport<T> implements NamespaceAwareBulkOperations<T> {

	private final BulkMode bulkMode;
	private final MongoOperations operations;
	private Namespace currentNamespace;
	private final NamespacedBulkOperationPipeline bulkOperationPipeline;

	public NamespacedBulkOperationSupport(BulkMode mode, NamespacedBulkOperationContext ctx, MongoOperations operations) {

		this.bulkMode = mode;
		this.currentNamespace = new Namespace(ctx.database(), null, null);
		this.operations = operations;
		this.bulkOperationPipeline = new NamespacedBulkOperationPipeline(ctx);
	}

	@Override
	public NamespaceAwareBulkOperations<T> insert(T source) {

		Assert.notNull(source, "Document must not be null");

		bulkOperationPipeline.append(new NamespacedBulkInsert(currentNamespace, source));
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
		bulkOperationPipeline.append(new NamespacedBulkRemove(currentNamespace, query));
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

		this.bulkOperationPipeline.append(new NamespacedBulkReplace(currentNamespace, query, replacement, options));
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public ClientBulkWriteResult execute() {

		// TODO: exceptions need to be translated correctly
		return operations.doWithClient(new MongoClusterCallback<>() {

			@Override
			public ClientBulkWriteResult doWithClient(MongoCluster cluster) throws MongoException, DataAccessException {

				ClientBulkWriteOptions cbws = ClientBulkWriteOptions.clientBulkWriteOptions()
						.ordered(NamespacedBulkOperationSupport.this.bulkMode.equals(BulkMode.ORDERED));

				return cluster.bulkWrite(bulkOperationPipeline.models(), cbws);
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
	public NamespaceAwareBulkOperations<Object> inCollection(String collection,
			Consumer<BulkOperationBase<Object>> bulkActions) {
		NamespaceAwareBulkOperations<Object> ops = inCollection(collection);
		bulkActions.accept(ops);
		return ops;
	}

	@Override
	public <S> NamespaceAwareBulkOperations<S> inCollection(Class<S> type) {
		return inCollection(operations.getCollectionName(type), type);
	}

	@Override
	@SuppressWarnings("unchecked")
	public NamespaceAwareBulkOperations<Object> inCollection(String collection) {
		return changeCollection(collection, null);
	}

	@SuppressWarnings("unchecked")
	public <S> NamespaceAwareBulkOperations<S> inCollection(String collection, @Nullable Class<S> type) {
		return changeCollection(collection, type);
	}

	@Override
	public <S> NamespaceAwareBulkOperations<S> inCollection(String collection, Class<S> type,
			Consumer<BulkOperationBase<S>> bulkActions) {
		NamespaceAwareBulkOperations<S> ops = inCollection(collection, type);
		bulkActions.accept(ops);
		return ops;
	}

	@SuppressWarnings({ "rawtypes" })
	private NamespaceAwareBulkOperations changeCollection(String collection, @Nullable Class<?> type) {
		this.currentNamespace = new Namespace(currentNamespace.database(), collection, type);
		return this;
	}

	@Override
	public NamespaceBulkOperations switchDatabase(String databaseName) {

		this.currentNamespace = new Namespace(databaseName, null, null);
		return this;
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

		if (multi) {
			bulkOperationPipeline.append(new NamespacedBulkUpdateMany(namespace, query, update, upsert));
		} else {
			bulkOperationPipeline.append(new NamespacedBulkUpdateOne(namespace, query, update, upsert));
		}
	}

	record Namespace(String database, @Nullable String collection, @Nullable Class<?> type) {

		public String name() {
			return String.format("%s.%s", database, collection != null ? collection : "n/a");
		}
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

	abstract static class NamespacedBulkOperation {

		protected final Namespace namespace;

		public NamespacedBulkOperation(Namespace namespace) {
			this.namespace = namespace;
		}

		@CheckReturnValue
		abstract NamespacedBulkOperation map(NamespacedBulkOperationContext context);

		abstract ClientNamespacedWriteModel prepareForWrite(NamespacedBulkOperationContext context);

		MongoNamespace mongoNamespace() {
			return new MongoNamespace(namespace.database(), namespace.collection());
		}

		protected static Document mapQuery(NamespacedBulkOperationContext context, Document query, Namespace namespace) {
			if (namespace.type() == null) {
				return context.queryMapper().getMappedObject(query, (MongoPersistentEntity<?>) null);
			}

			MongoPersistentEntity<?> persistentEntity = context.updateMapper().getMappingContext()
					.getPersistentEntity(namespace.type());
			return context.queryMapper().getMappedObject(query, persistentEntity);
		}
	}

	static class NamespacedBulkInsert extends NamespacedBulkOperation {

		private final Object source;
		private final @Nullable Document mappedObject;

		public NamespacedBulkInsert(Namespace namespace, Object source) {
			this(namespace, source, null);
		}

		private NamespacedBulkInsert(Namespace namespace, Object source, @Nullable Document mappedObject) {
			super(namespace);
			this.source = source;
			this.mappedObject = mappedObject;
		}

		public Document getMappedObject() {
			if (mappedObject == null) {
				throw new IllegalStateException("No mapped object for namespace " + namespace.name());
			}
			return mappedObject;
		}

		NamespacedBulkInsert map(NamespacedBulkOperationContext context) {

			if (mappedObject != null) {
				return this;
			}

			context.publishEvent(new BeforeConvertEvent<>(source, namespace.name()));
			Object value = context.callback(BeforeConvertCallback.class, source, namespace);
			if (value instanceof Document document) {
				return new NamespacedBulkInsert(namespace, value, document);
			}
			Document sink = new Document();
			context.updateMapper().getConverter().write(value, sink);
			return new NamespacedBulkInsert(namespace, value, sink);
		}

		@Override
		ClientNamespacedInsertOneModel prepareForWrite(NamespacedBulkOperationContext context) {

			context.publishEvent(new BeforeSaveEvent<>(source, mappedObject, namespace.name()));
			context.callback(BeforeSaveCallback.class, source, mappedObject, namespace);
			return ClientNamespacedWriteModel.insertOne(mongoNamespace(), mappedObject);
		}
	}

	static abstract class BaseNamespacedBulkUpdate extends NamespacedBulkOperation {

		protected final Query query;
		protected final UpdateDefinition update;
		protected final boolean upsert;
		protected final @Nullable Object mappedUpdate;
		protected final @Nullable Document mappedQuery;

		public BaseNamespacedBulkUpdate(Namespace namespace, Query query, UpdateDefinition source, boolean upsert,
				@Nullable Object mappedUpdate, @Nullable Document mappedQuery) {
			super(namespace);
			this.query = query;
			this.update = source;
			this.upsert = upsert;
			this.mappedUpdate = mappedUpdate;
			this.mappedQuery = mappedQuery;
		}

		protected static Document mapUpdate(NamespacedBulkOperationContext context, Document updateObject,
				Namespace namespace) {

			if (namespace.type() == null) {
				return context.updateMapper().getMappedObject(updateObject, (MongoPersistentEntity<?>) null);
			}

			MongoPersistentEntity<?> persistentEntity = context.updateMapper().getMappingContext()
					.getPersistentEntity(namespace.type());
			return context.updateMapper().getMappedObject(updateObject, persistentEntity);
		}

		protected static List<Document> mapUpdatePipeline(NamespacedBulkOperationContext context, AggregationUpdate source,
				Namespace namespace) {

			Class<?> type = namespace.type();

			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext = context.queryMapper()
					.getMappingContext();
			AggregationOperationContext aggregationContext = new TypeBasedAggregationOperationContext(type, mappingContext,
					context.queryMapper(), FieldLookupPolicy.relaxed());

			return new AggregationUtil(context.queryMapper(), mappingContext).createPipeline(source, aggregationContext);
		}

		/**
		 * @param multi flag to indicate if update might affect multiple documents.
		 * @return new instance of {@link UpdateOptions}.
		 */
		protected UpdateOptions updateOptions(boolean multi) {

			UpdateOptions options = new UpdateOptions();
			options.upsert(upsert);

			if (update.hasArrayFilters()) {
				List<Document> list = new ArrayList<>(update.getArrayFilters().size());
				for (ArrayFilter arrayFilter : update.getArrayFilters()) {
					list.add(arrayFilter.asDocument());
				}
				options.arrayFilters(list);
			}

			if (!multi && query.isSorted()) {
				options.sort(query.getSortObject());
			}

			query.getCollation().map(Collation::toMongoCollation).ifPresent(options::collation);
			return options;
		}
	}

	static class NamespacedBulkUpdateOne extends BaseNamespacedBulkUpdate {

		public NamespacedBulkUpdateOne(Namespace namespace, Query query, UpdateDefinition source, boolean upsert) {
			this(namespace, query, source, upsert, null, null);
		}

		public NamespacedBulkUpdateOne(Namespace namespace, Query query, UpdateDefinition source, boolean upsert,
				@Nullable Object mappedUpdate, @Nullable Document mappedQuery) {
			super(namespace, query, source, upsert, mappedUpdate, mappedQuery);
		}

		@Override
		NamespacedBulkUpdateOne map(NamespacedBulkOperationContext context) {

			Object mappedUpdate = update instanceof AggregationUpdate aggregationUpdate
					? mapUpdatePipeline(context, aggregationUpdate, namespace)
					: mapUpdate(context, update.getUpdateObject(), namespace);
			Document mappedQuery = mapQuery(context, query.getQueryObject(), namespace);

			return new NamespacedBulkUpdateOne(namespace, query, update, upsert, mappedUpdate, mappedQuery);
		}

		@Override
		@SuppressWarnings("unchecked")
		ClientNamespacedWriteModel prepareForWrite(NamespacedBulkOperationContext context) {
			UpdateOptions options = updateOptions(false);
			ClientUpdateOneOptions updateOneOptions = new ConcreteClientUpdateOneOptions();
			updateOneOptions.arrayFilters(options.getArrayFilters());
			updateOneOptions.collation(options.getCollation());
			updateOneOptions.upsert(options.isUpsert());
			updateOneOptions.hint(options.getHint());
			updateOneOptions.hintString(options.getHintString());

			if (mappedUpdate instanceof List<?> pipeline) {
				return ClientNamespacedWriteModel.updateOne(mongoNamespace(), mappedQuery, (List<Document>) pipeline,
						updateOneOptions);
			}
			return ClientNamespacedWriteModel.updateOne(mongoNamespace(), mappedQuery, (Document) mappedUpdate,
					updateOneOptions);
		}

	}

	static class NamespacedBulkUpdateMany extends BaseNamespacedBulkUpdate {

		public NamespacedBulkUpdateMany(Namespace namespace, Query query, UpdateDefinition source, boolean upsert) {
			this(namespace, query, source, upsert, null, null);
		}

		public NamespacedBulkUpdateMany(Namespace namespace, Query query, UpdateDefinition source, boolean upsert,
				@Nullable Object mappedUpdate, @Nullable Document mappedQuery) {
			super(namespace, query, source, upsert, mappedUpdate, mappedQuery);
		}

		@Override
		NamespacedBulkUpdateMany map(NamespacedBulkOperationContext context) {

			Object mappedUpdate = update instanceof AggregationUpdate aggregationUpdate
					? mapUpdatePipeline(context, aggregationUpdate, namespace)
					: mapUpdate(context, update.getUpdateObject(), namespace);
			Document mappedQuery = mapQuery(context, query.getQueryObject(), namespace);

			return new NamespacedBulkUpdateMany(namespace, query, update, upsert, mappedUpdate, mappedQuery);
		}

		@Override
		@SuppressWarnings("unchecked")
		ClientNamespacedWriteModel prepareForWrite(NamespacedBulkOperationContext context) {

			UpdateOptions options = updateOptions(true);
			ConcreteClientUpdateManyOptions updateOneOptions = new ConcreteClientUpdateManyOptions();
			updateOneOptions.arrayFilters(options.getArrayFilters());
			updateOneOptions.collation(options.getCollation());
			updateOneOptions.upsert(options.isUpsert());
			updateOneOptions.hint(options.getHint());
			updateOneOptions.hintString(options.getHintString());

			if (mappedUpdate instanceof List<?> pipeline) {
				return ClientNamespacedWriteModel.updateMany(mongoNamespace(), mappedQuery, (List<Document>) pipeline,
						updateOneOptions);
			}
			return ClientNamespacedWriteModel.updateMany(mongoNamespace(), mappedQuery, (Document) mappedUpdate,
					updateOneOptions);
		}

	}

	static class NamespacedBulkRemove extends NamespacedBulkOperation {

		private final Query query;
		private final @Nullable Document mappedQuery;

		public NamespacedBulkRemove(Namespace namespace, Query query) {
			this(namespace, query, null);
		}

		public NamespacedBulkRemove(Namespace namespace, Query query, @Nullable Document mappedQuery) {
			super(namespace);
			this.query = query;
			this.mappedQuery = mappedQuery;
		}

		@Override
		NamespacedBulkRemove map(NamespacedBulkOperationContext context) {
			return new NamespacedBulkRemove(namespace, query, mapQuery(context, query.getQueryObject(), namespace));
		}

		@Override
		ClientNamespacedWriteModel prepareForWrite(NamespacedBulkOperationContext context) {

			ClientDeleteManyOptions deleteOptions = ClientDeleteManyOptions.clientDeleteManyOptions();
			query.getCollation().map(Collation::toMongoCollation).ifPresent(deleteOptions::collation);

			return ClientNamespacedWriteModel.deleteMany(mongoNamespace(), mappedQuery, deleteOptions);
		}
	}

	static class NamespacedBulkReplace extends NamespacedBulkOperation {

		private final Query query;
		private final Object replacement;
		private final FindAndReplaceOptions options;

		private final @Nullable Document mappedQuery;
		private final @Nullable Document mappedReplacement;

		public NamespacedBulkReplace(Namespace namespace, Query query, Object replacement, FindAndReplaceOptions options) {
			this(namespace, query, replacement, options, null, null);
		}

		public NamespacedBulkReplace(Namespace namespace, Query query, Object replacement, FindAndReplaceOptions options,
				@Nullable Document mappedQuery, @Nullable Document mappedReplacement) {
			super(namespace);
			this.query = query;
			this.replacement = replacement;
			this.options = options;
			this.mappedQuery = mappedQuery;
			this.mappedReplacement = mappedReplacement;
		}

		@Override
		NamespacedBulkOperation map(NamespacedBulkOperationContext context) {

			Document mappedQuery = mapQuery(context, query.getQueryObject(), namespace);
			context.publishEvent(new BeforeConvertEvent<>(replacement, namespace.name()));
			Object replacementSource = context.callback(BeforeConvertCallback.class, replacement, namespace);

			if (replacementSource instanceof Document mapped) {
				return new NamespacedBulkReplace(namespace, query, replacementSource, options, mappedQuery, mapped);
			}
			Document sink = new Document();
			context.updateMapper().getConverter().write(replacementSource, sink);

			return new NamespacedBulkReplace(namespace, query, replacementSource, options, mappedQuery, sink);
		}

		@Override
		ClientNamespacedWriteModel prepareForWrite(NamespacedBulkOperationContext context) {

			ClientReplaceOneOptions replaceOptions = ClientReplaceOneOptions.clientReplaceOneOptions();
			replaceOptions.upsert(options.isUpsert());
			if (query.isSorted()) {
				replaceOptions.sort(query.getSortObject());
			}
			query.getCollation().map(Collation::toMongoCollation).ifPresent(replaceOptions::collation);

			context.publishEvent(new BeforeSaveEvent<>(replacement, mappedReplacement, namespace.name()));
			context.callback(BeforeSaveCallback.class, replacement, mappedReplacement, namespace);

			return ClientNamespacedWriteModel.replaceOne(mongoNamespace(), mappedQuery, mappedReplacement, replaceOptions);
		}
	}

	static class NamespacedBulkOperationPipeline {

		List<NamespacedBulkOperation> pipeline = new ArrayList<>();
		NamespacedBulkOperationContext bulkOperationContext;

		public NamespacedBulkOperationPipeline(NamespacedBulkOperationContext bulkOperationContext) {
			this.bulkOperationContext = bulkOperationContext;
		}

		void append(NamespacedBulkOperation operation) {
			pipeline.add(operation.map(bulkOperationContext));
		}

		List<ClientNamespacedWriteModel> models() {
			return pipeline.stream().map(it -> it.prepareForWrite(bulkOperationContext)).toList();
		}
	}

}
