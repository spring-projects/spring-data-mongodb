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

import java.util.List;
import java.util.Objects;
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
import org.springframework.data.mongodb.core.QueryOperations.DeleteContext;
import org.springframework.data.mongodb.core.QueryOperations.QueryContext;
import org.springframework.data.mongodb.core.QueryOperations.UpdateContext;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.util.Pair;
import org.springframework.lang.CheckReturnValue;
import org.springframework.util.Assert;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCluster;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientDeleteManyOptions;
import com.mongodb.client.model.bulk.ClientNamespacedInsertOneModel;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.client.model.bulk.ClientReplaceOneOptions;
import com.mongodb.client.model.bulk.ClientUpdateManyOptions;
import com.mongodb.client.model.bulk.ClientUpdateOneOptions;
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
	private final BulkOperationPipelineSupport<NamespacedBulkOperationContext, ClientNamespacedWriteModel> pipeline;

	public NamespacedBulkOperationSupport(BulkMode mode, NamespacedBulkOperationContext ctx, MongoOperations operations) {

		this.bulkMode = mode;
		this.currentNamespace = new Namespace(ctx.database(), null, null);
		this.operations = operations;
		this.pipeline = new BulkOperationPipelineSupport<>(ctx);
	}

	@Override
	public NamespaceAwareBulkOperations<T> insert(T source) {

		Assert.notNull(source, "Document must not be null");

		pipeline.append(new NamespacedBulkInsert(currentNamespace, source));
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
		pipeline.append(new NamespacedBulkRemove(currentNamespace, query));
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

		this.pipeline.append(new NamespacedBulkReplace(currentNamespace, query, replacement, options));
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public ClientBulkWriteResult execute() {

		// TODO: exceptions need to be translated correctly
		ClientBulkWriteResult result = operations.doWithClient(new MongoClusterCallback<>() {

			@Override
			public ClientBulkWriteResult doWithClient(MongoCluster cluster) throws MongoException, DataAccessException {

				ClientBulkWriteOptions cbws = ClientBulkWriteOptions.clientBulkWriteOptions()
						.ordered(NamespacedBulkOperationSupport.this.bulkMode.equals(BulkMode.ORDERED));

				return cluster.bulkWrite(pipeline.models(), cbws);
			}
		});

		pipeline.postProcess();
		return result;
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
			pipeline.append(new NamespacedBulkUpdateMany(namespace, query, update, upsert));
		} else {
			pipeline.append(new NamespacedBulkUpdateOne(namespace, query, update, upsert));
		}
	}

	record Namespace(String database, @Nullable String collection, @Nullable Class<?> type) {

		public String name() {
			return String.format("%s.%s", database, collection != null ? collection : "n/a");
		}
	}

	record SourceAwareMappedDocument<T>(T source, Document mapped) {

	}

	static final class NamespacedBulkOperationContext {

		private final String database;
		private final MongoConverter mongoConverter;
		private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
		private final QueryMapper queryMapper;
		private final UpdateMapper updateMapper;
		private final @Nullable ApplicationEventPublisher eventPublisher;
		private final @Nullable EntityCallbacks entityCallbacks;
		private final QueryOperations queryOperations;

		public NamespacedBulkOperationContext(String database, MongoConverter mongoConverter, QueryMapper queryMapper,
				UpdateMapper updateMapper, @Nullable ApplicationEventPublisher eventPublisher,
				@Nullable EntityCallbacks entityCallbacks) {

			this.database = database;
			this.mongoConverter = mongoConverter;
			this.queryMapper = queryMapper;
			this.updateMapper = updateMapper;
			this.eventPublisher = eventPublisher;
			this.entityCallbacks = entityCallbacks;
			this.mappingContext = mongoConverter.getMappingContext();
			this.queryOperations = new QueryOperations(queryMapper, updateMapper,
					new EntityOperations(mongoConverter, queryMapper), new PropertyOperations(this.mappingContext),
					mongoConverter);
		}

		<T> SourceAwareMappedDocument<T> mapDomainObject(Namespace namespace, T source) {

			publishEvent(new BeforeConvertEvent<>(source, namespace.name()));
			T value = callback(BeforeConvertCallback.class, source, namespace);
			if (value instanceof Document document) {
				return new SourceAwareMappedDocument<>(value, document);
			}
			Document sink = new Document();
			mongoConverter.write(value, sink);
			return new SourceAwareMappedDocument<>(value, sink);
		}

		public Document mapQuery(Namespace namespace, Query query) {

			QueryContext queryContext = queryOperations().createQueryContext(query);
			if (namespace.type() == null) {
				return queryContext.getMappedQuery(null);
			}

			return queryContext.getMappedQuery(entity(namespace));
		}

		public Object mapUpdate(Namespace namespace, Query query, UpdateDefinition updateDefinition, boolean upsert) {

			UpdateContext updateContext = queryOperations.updateContext(updateDefinition, query, upsert);
			if (updateDefinition instanceof AggregationUpdate) {
				return updateContext.getUpdatePipeline(namespace.type());
			}
			return updateContext.getMappedUpdate(entity(namespace));
		}

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

		@Nullable
		MongoPersistentEntity<?> entity(Namespace namespace) {
			if (namespace.type() == null) {
				return null;
			}
			return mappingContext().getPersistentEntity(namespace.type());
		}

		MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext() {
			return mongoConverter.getMappingContext();
		}

		public String database() {
			return database;
		}

		public MongoConverter mongoConverter() {
			return mongoConverter;
		}

		public QueryMapper queryMapper() {
			return queryMapper;
		}

		public UpdateMapper updateMapper() {
			return updateMapper;
		}

		public @Nullable ApplicationEventPublisher eventPublisher() {
			return eventPublisher;
		}

		public @Nullable EntityCallbacks entityCallbacks() {
			return entityCallbacks;
		}

		public QueryOperations queryOperations() {
			return queryOperations;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (obj == null || obj.getClass() != this.getClass()) {
				return false;
			}
			var that = (NamespacedBulkOperationContext) obj;
			return Objects.equals(this.database, that.database) && Objects.equals(this.mongoConverter, that.mongoConverter)
					&& Objects.equals(this.queryMapper, that.queryMapper) && Objects.equals(this.updateMapper, that.updateMapper)
					&& Objects.equals(this.eventPublisher, that.eventPublisher)
					&& Objects.equals(this.entityCallbacks, that.entityCallbacks);
		}

		@Override
		public int hashCode() {
			return Objects.hash(database, mongoConverter, queryMapper, updateMapper, eventPublisher, entityCallbacks);
		}

		@Override
		public String toString() {
			return "NamespacedBulkOperationContext[" + "database=" + database + ", " + "mongoConverter=" + mongoConverter
					+ ", " + "queryMapper=" + queryMapper + ", " + "updateMapper=" + updateMapper + ", " + "eventPublisher="
					+ eventPublisher + ", " + "entityCallbacks=" + entityCallbacks + ']';
		}
	}

	abstract static class NamespacedBulkOperation
			implements BulkOperationPipelineSupport.BulkOperationPipelineItem<NamespacedBulkOperationContext, ClientNamespacedWriteModel> {

		protected final Namespace namespace;

		public NamespacedBulkOperation(Namespace namespace) {
			this.namespace = namespace;
		}

		@CheckReturnValue
		@Override
		public abstract NamespacedBulkOperation map(NamespacedBulkOperationContext context);

		@Override
		public abstract ClientNamespacedWriteModel prepareForWrite(NamespacedBulkOperationContext context);

		MongoNamespace mongoNamespace() {
			return new MongoNamespace(namespace.database(), namespace.collection());
		}

		@Override
		public void finish(NamespacedBulkOperationContext context) {}
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

		public NamespacedBulkInsert map(NamespacedBulkOperationContext context) {

			if (mappedObject != null) {
				return this;
			}

			SourceAwareMappedDocument<?> target = context.mapDomainObject(namespace, source);
			return new NamespacedBulkInsert(namespace, target.source, target.mapped);
		}

		@Override
		public ClientNamespacedInsertOneModel prepareForWrite(NamespacedBulkOperationContext context) {

			context.publishEvent(new BeforeSaveEvent<>(source, mappedObject, namespace.name()));
			context.callback(BeforeSaveCallback.class, source, mappedObject, namespace);
			return ClientNamespacedWriteModel.insertOne(mongoNamespace(), mappedObject);
		}

		@Override
		public void finish(NamespacedBulkOperationContext context) {
			context.publishEvent(new AfterSaveEvent<>(source, mappedObject, namespace.name()));
			context.callback(AfterSaveCallback.class, source, mappedObject, namespace);
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

		/**
		 * @return new instance of {@link UpdateOptions}.
		 */
		protected UpdateOptions updateOptions(NamespacedBulkOperationContext context) {

			UpdateContext updateContext = context.queryOperations().updateContext(update, query, upsert);
			return updateContext.getUpdateOptions(namespace.type(), query);
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
		public NamespacedBulkUpdateOne map(NamespacedBulkOperationContext context) {

			Object mappedUpdate = context.mapUpdate(namespace, query, update, upsert);
			Document mappedQuery = context.mapQuery(namespace, query);
			return new NamespacedBulkUpdateOne(namespace, query, update, upsert, mappedUpdate, mappedQuery);
		}

		@Override
		@SuppressWarnings("unchecked")
		public ClientNamespacedWriteModel prepareForWrite(NamespacedBulkOperationContext context) {

			UpdateOptions options = updateOptions(context);
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
		public NamespacedBulkUpdateMany map(NamespacedBulkOperationContext context) {

			Object mappedUpdate = context.mapUpdate(namespace, query, update, upsert);
			Document mappedQuery = context.mapQuery(namespace, query);

			return new NamespacedBulkUpdateMany(namespace, query, update, upsert, mappedUpdate, mappedQuery);
		}

		@Override
		@SuppressWarnings("unchecked")
		public ClientNamespacedWriteModel prepareForWrite(NamespacedBulkOperationContext context) {

			UpdateOptions options = updateOptions(context);
			ClientUpdateManyOptions updateOneOptions = ClientUpdateManyOptions.clientUpdateManyOptions();
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
		public NamespacedBulkRemove map(NamespacedBulkOperationContext context) {
			return new NamespacedBulkRemove(namespace, query, context.mapQuery(namespace, query));
		}

		@Override
		public ClientNamespacedWriteModel prepareForWrite(NamespacedBulkOperationContext context) {

			DeleteContext deleteContext = context.queryOperations().deleteQueryContext(query);
			DeleteOptions deleteOptions = deleteContext.getDeleteOptions(namespace.type());
			ClientDeleteManyOptions clientDeleteOptions = ClientDeleteManyOptions.clientDeleteManyOptions();
			clientDeleteOptions.collation(deleteOptions.getCollation());
			clientDeleteOptions.hint(deleteOptions.getHint());
			clientDeleteOptions.hintString(deleteOptions.getHintString());

			return ClientNamespacedWriteModel.deleteMany(mongoNamespace(), mappedQuery, clientDeleteOptions);
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
		public NamespacedBulkOperation map(NamespacedBulkOperationContext context) {

			SourceAwareMappedDocument<Object> target = context.mapDomainObject(namespace, replacement);
			Document mappedQuery = context.mapQuery(namespace, query);

			return new NamespacedBulkReplace(namespace, query, target.source(), options, mappedQuery, target.mapped());
		}

		@Override
		public ClientNamespacedWriteModel prepareForWrite(NamespacedBulkOperationContext context) {

			UpdateContext updateContext = context.queryOperations().replaceSingleContext(query,
					MappedDocument.of(mappedReplacement), options.isUpsert());
			UpdateOptions updateOptions = updateContext.getUpdateOptions(namespace.type(), query);

			ClientReplaceOneOptions replaceOptions = ClientReplaceOneOptions.clientReplaceOneOptions();
			replaceOptions.upsert(updateOptions.isUpsert());
			replaceOptions.sort(updateOptions.getSort());
			replaceOptions.hint(updateOptions.getHint());
			replaceOptions.hintString(updateOptions.getHintString());
			replaceOptions.collation(updateOptions.getCollation());

			context.publishEvent(new BeforeSaveEvent<>(replacement, mappedReplacement, namespace.name()));
			context.callback(BeforeSaveCallback.class, replacement, mappedReplacement, namespace);

			return ClientNamespacedWriteModel.replaceOne(mongoNamespace(), mappedQuery, mappedReplacement, replaceOptions);
		}

		@Override
		public void finish(NamespacedBulkOperationContext context) {
			context.publishEvent(new AfterSaveEvent<>(replacement, mappedReplacement, namespace.name()));
			context.callback(AfterSaveCallback.class, replacement, mappedReplacement, namespace);
		}
	}

}
