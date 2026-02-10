/*
 * Copyright 2015-present the original author or authors.
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
import java.util.Optional;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.MappedDocument;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.QueryOperations.DeleteContext;
import org.springframework.data.mongodb.core.QueryOperations.UpdateContext;
import org.springframework.data.util.Pair;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

/**
 * Default implementation for {@link BulkOperations}.
 *
 * @author Tobias Trelle
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Minsu Kim
 * @author Jens Schauder
 * @author Michail Nikolaev
 * @author Roman Puchkovskiy
 * @author Jacob Botuck
 * @since 1.9
 */
class DefaultBulkOperations extends BulkOperationsSupport implements BulkOperations {

	private final MongoOperations mongoOperations;
	private final String collectionName;
	private final BulkOperationContext bulkOperationContext;
	private final BulkOperationPipelineSupport<BulkOperationContext, WriteModel<Document>> pipeline;

	private @Nullable WriteConcern defaultWriteConcern;
	private BulkWriteOptions bulkOptions;

	/**
	 * Creates a new {@link DefaultBulkOperations} for the given {@link MongoOperations}, collection name and
	 * {@link BulkOperationContext}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param bulkOperationContext must not be {@literal null}.
	 * @since 2.0
	 */
	DefaultBulkOperations(MongoOperations mongoOperations, String collectionName,
			BulkOperationContext bulkOperationContext) {

		super(collectionName);
		Assert.notNull(mongoOperations, "MongoOperations must not be null");
		Assert.hasText(collectionName, "CollectionName must not be null nor empty");
		Assert.notNull(bulkOperationContext, "BulkOperationContext must not be null");

		this.mongoOperations = mongoOperations;
		this.collectionName = collectionName;
		this.bulkOperationContext = bulkOperationContext;
		this.pipeline = new BulkOperationPipelineSupport<>(bulkOperationContext);
		this.bulkOptions = getBulkWriteOptions(bulkOperationContext.bulkMode());
	}

	/**
	 * Configures the default {@link WriteConcern} to be used. Defaults to {@literal null}.
	 *
	 * @param defaultWriteConcern can be {@literal null}.
	 */
	void setDefaultWriteConcern(@Nullable WriteConcern defaultWriteConcern) {
		this.defaultWriteConcern = defaultWriteConcern;
	}

	@Override
	@Contract("_ -> this")
	public BulkOperations insert(Object document) {

		Assert.notNull(document, "Document must not be null");

		pipeline.append(new CollectionBulkInsert(document));

		return this;
	}

	@Override
	@Contract("_ -> this")
	public BulkOperations insert(List<? extends Object> documents) {

		Assert.notNull(documents, "Documents must not be null");

		documents.forEach(this::insert);

		return this;
	}

	@Override
	@Contract("_, _ -> this")
	public BulkOperations updateOne(Query query, UpdateDefinition update) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");

		return update(query, update, false, false);
	}

	@Override
	@Contract("_ -> this")
	public BulkOperations updateOne(List<Pair<Query, UpdateDefinition>> updates) {

		Assert.notNull(updates, "Updates must not be null");

		for (Pair<Query, UpdateDefinition> update : updates) {
			update(update.getFirst(), update.getSecond(), false, false);
		}

		return this;
	}

	@Override
	@Contract("_, _ -> this")
	public BulkOperations updateMulti(Query query, UpdateDefinition update) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");

		update(query, update, false, true);

		return this;
	}

	@Override
	@Contract("_ -> this")
	public BulkOperations updateMulti(List<Pair<Query, UpdateDefinition>> updates) {

		Assert.notNull(updates, "Updates must not be null");

		for (Pair<Query, UpdateDefinition> update : updates) {
			update(update.getFirst(), update.getSecond(), false, true);
		}

		return this;
	}

	@Override
	@Contract("_, _ -> this")
	public BulkOperations upsert(Query query, UpdateDefinition update) {
		return update(query, update, true, true);
	}

	@Override
	@Contract("_ -> this")
	public BulkOperations upsert(List<Pair<Query, Update>> updates) {

		for (Pair<Query, Update> update : updates) {
			upsert(update.getFirst(), update.getSecond());
		}

		return this;
	}

	@Override
	@Contract("_ -> this")
	public BulkOperations remove(Query query) {

		Assert.notNull(query, "Query must not be null");

		pipeline.append(new CollectionBulkRemove(query));

		return this;
	}

	@Override
	@Contract("_ -> this")
	public BulkOperations remove(List<Query> removes) {

		Assert.notNull(removes, "Removals must not be null");

		for (Query query : removes) {
			remove(query);
		}

		return this;
	}

	@Override
	@Contract("_, _, _ -> this")
	public BulkOperations replaceOne(Query query, Object replacement, FindAndReplaceOptions options) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(replacement, "Replacement must not be null");
		Assert.notNull(options, "Options must not be null");

		pipeline.append(new CollectionBulkReplace(query, replacement, options));

		return this;
	}

	@Override
	public com.mongodb.bulk.BulkWriteResult execute() {

		try {

			com.mongodb.bulk.BulkWriteResult result = mongoOperations.execute(collectionName, this::bulkWriteTo);

			Assert.state(result != null, "Result must not be null");

			pipeline.postProcess();

			return result;
		} finally {
			this.bulkOptions = getBulkWriteOptions(bulkOperationContext.bulkMode());
		}
	}

	private BulkWriteResult bulkWriteTo(MongoCollection<Document> collection) {

		if (defaultWriteConcern != null) {
			collection = collection.withWriteConcern(defaultWriteConcern);
		}

		try {

			return collection.bulkWrite(pipeline.models(), bulkOptions);
		} catch (RuntimeException ex) {

			if (ex instanceof MongoBulkWriteException mongoBulkWriteException) {

				if (mongoBulkWriteException.getWriteConcernError() != null) {
					throw new DataIntegrityViolationException(ex.getMessage(), ex);
				}
				throw new BulkOperationException(ex.getMessage(), mongoBulkWriteException);
			}

			throw ex;
		}
	}

	/**
	 * Performs update and upsert bulk operations.
	 *
	 * @param query the {@link Query} to determine documents to update.
	 * @param update the {@link Update} to perform, must not be {@literal null}.
	 * @param upsert whether to upsert.
	 * @param multi whether to issue a multi-update.
	 * @return the {@link BulkOperations} with the update registered.
	 */
	private BulkOperations update(Query query, UpdateDefinition update, boolean upsert, boolean multi) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");

		if (multi) {
			pipeline.append(new CollectionBulkUpdateMany(query, update, upsert));
		} else {
			pipeline.append(new CollectionBulkUpdateOne(query, update, upsert));
		}

		return this;
	}

	@Override
	protected void maybeEmitEvent(ApplicationEvent event) {
		bulkOperationContext.publishEvent(event);
	}

	@Override
	protected UpdateMapper updateMapper() {
		return bulkOperationContext.updateMapper();
	}

	@Override
	protected QueryMapper queryMapper() {
		return bulkOperationContext.queryMapper();
	}

	@Override
	protected Optional<? extends MongoPersistentEntity<?>> entity() {
		return Optional.ofNullable(bulkOperationContext.entity());
	}

	/**
	 * {@link BulkOperationContext} holds information about {@link BulkMode} the entity in use as well as references to
	 * {@link QueryMapper} and {@link UpdateMapper}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	record BulkOperationContext(BulkMode bulkMode, @Nullable MongoPersistentEntity<?> entity,
			QueryMapper queryMapper, UpdateMapper updateMapper, @Nullable ApplicationEventPublisher eventPublisher,
			@Nullable EntityCallbacks entityCallbacks, QueryOperations queryOperations, MongoConverter mongoConverter,
			String collectionName) {

		public boolean skipEntityCallbacks() {
			return entityCallbacks == null;
		}

		public boolean skipEventPublishing() {
			return eventPublisher == null;
		}

		@SuppressWarnings({ "rawtypes", "NullAway" })
		public <T> T callback(Class<? extends EntityCallback> callbackType, T entity, String collectionName) {

			if (skipEntityCallbacks()) {
				return entity;
			}

			return entityCallbacks.callback(callbackType, entity, collectionName);
		}

		@SuppressWarnings({ "rawtypes", "NullAway" })
		public <T> T callback(Class<? extends EntityCallback> callbackType, T entity, Document document,
				String collectionName) {

			if (skipEntityCallbacks()) {
				return entity;
			}

			return entityCallbacks.callback(callbackType, entity, document, collectionName);
		}

		@SuppressWarnings("NullAway")
		public void publishEvent(ApplicationEvent event) {

			if (skipEventPublishing()) {
				return;
			}

			eventPublisher.publishEvent(event);
		}

		SourceAwareMappedDocument mapDomainObject(Object source) {
			publishEvent(new BeforeConvertEvent<>(source, collectionName));
			Object value = callback(BeforeConvertCallback.class, source, collectionName);
			if (value instanceof Document doc) {
				return new SourceAwareMappedDocument(value, doc);
			}
			Document sink = new Document();
			mongoConverter.write(value, sink);
			return new SourceAwareMappedDocument(value, sink);
		}

		Document mapQuery(Query query) {
			org.springframework.data.mongodb.core.QueryOperations.QueryContext qctx = queryOperations.createQueryContext(query);
			return qctx.getMappedQuery(entity());
		}

		Object mapUpdate(Query query, UpdateDefinition update, boolean upsert) {
			UpdateContext uctx = queryOperations.updateContext(update, query, upsert);
			Class<?> entityClass = entity() != null ? entity().getType() : null;
			if (uctx.isAggregationUpdate()) {
				return uctx.getUpdatePipeline(entityClass);
			}
			return uctx.getMappedUpdate(entity);
		}

		UpdateOptions getUpdateOptions(Query query, UpdateDefinition update, boolean upsert, boolean multi) {
			UpdateContext uctx = multi
					? queryOperations.updateContext(update, query, upsert)
					: queryOperations.updateSingleContext(update, query, upsert);
			Class<?> entityClass = entity() != null ? entity().getType() : null;
			return uctx.getUpdateOptions(entityClass, query);
		}

		DeleteOptions getDeleteOptions(Query query) {
			DeleteContext dctx = queryOperations.deleteQueryContext(query);
			Class<?> entityClass = entity() != null ? entity().getType() : null;
			return dctx.getDeleteOptions(entityClass);
		}

		com.mongodb.client.model.ReplaceOptions getReplaceOptions(Query query, Document replacement, boolean upsert) {
			UpdateContext uctx = queryOperations.replaceSingleContext(query, MappedDocument.of(replacement), upsert);
			Class<?> entityClass = entity() != null ? entity().getType() : null;
			return uctx.getReplaceOptions(entityClass);
		}
	}

	record SourceAwareMappedDocument(Object source, Document mapped) {}

	abstract static class CollectionBulkOperation
			implements BulkOperationPipelineSupport.BulkOperationPipelineItem<BulkOperationContext, WriteModel<Document>> {}

	static final class CollectionBulkInsert extends CollectionBulkOperation {

		private final Object source;
		private @Nullable Document mappedDocument;

		CollectionBulkInsert(Object source) {
			this(source, null);
		}

		CollectionBulkInsert(Object source, @Nullable Document mappedDocument) {
			this.source = source;
			this.mappedDocument = mappedDocument;
		}

		@Override
		public BulkOperationPipelineSupport.BulkOperationPipelineItem<BulkOperationContext, WriteModel<Document>> map(
				BulkOperationContext ctx) {
			if (mappedDocument != null) {
				return this;
			}
			SourceAwareMappedDocument target = ctx.mapDomainObject(source);
			return new CollectionBulkInsert(target.source(), target.mapped());
		}

		@Override
		public WriteModel<Document> prepareForWrite(BulkOperationContext ctx) {
			if (mappedDocument == null) {
				SourceAwareMappedDocument target = ctx.mapDomainObject(source);
				this.mappedDocument = target.mapped();
			}
			ctx.publishEvent(new org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent<>(source, mappedDocument, ctx.collectionName()));
			ctx.callback(BeforeSaveCallback.class, source, mappedDocument, ctx.collectionName());
			return new InsertOneModel<>(mappedDocument);
		}

		@Override
		public void finish(BulkOperationContext ctx) {
			ctx.publishEvent(new org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent<>(source, mappedDocument, ctx.collectionName()));
			ctx.callback(AfterSaveCallback.class, source, mappedDocument, ctx.collectionName());
		}
	}

	static final class CollectionBulkUpdateOne extends CollectionBulkOperation {

		private final Query query;
		private final UpdateDefinition update;
		private final boolean upsert;
		private @Nullable Object mappedUpdate;
		private @Nullable Document mappedQuery;

		CollectionBulkUpdateOne(Query query, UpdateDefinition update, boolean upsert) {
			this(query, update, upsert, null, null);
		}

		CollectionBulkUpdateOne(Query query, UpdateDefinition update, boolean upsert,
				@Nullable Object mappedUpdate, @Nullable Document mappedQuery) {
			this.query = query;
			this.update = update;
			this.upsert = upsert;
			this.mappedUpdate = mappedUpdate;
			this.mappedQuery = mappedQuery;
		}

		@Override
		public BulkOperationPipelineSupport.BulkOperationPipelineItem<BulkOperationContext, WriteModel<Document>> map(
				BulkOperationContext ctx) {
			Object mappedUpdate = ctx.mapUpdate(query, update, upsert);
			Document mappedQuery = ctx.mapQuery(query);
			return new CollectionBulkUpdateOne(query, update, upsert, mappedUpdate, mappedQuery);
		}

		@Override
		@SuppressWarnings("unchecked")
		public WriteModel<Document> prepareForWrite(BulkOperationContext ctx) {
			UpdateOptions options = ctx.getUpdateOptions(query, update, upsert, false);
			if (mappedUpdate instanceof List<?> pipeline) {
				return new UpdateOneModel<>(mappedQuery, (List<Document>) pipeline, options);
			}
			return new UpdateOneModel<>(mappedQuery, (Document) mappedUpdate, options);
		}
	}

	static final class CollectionBulkUpdateMany extends CollectionBulkOperation {

		private final Query query;
		private final UpdateDefinition update;
		private final boolean upsert;
		private @Nullable Object mappedUpdate;
		private @Nullable Document mappedQuery;

		CollectionBulkUpdateMany(Query query, UpdateDefinition update, boolean upsert) {
			this(query, update, upsert, null, null);
		}

		CollectionBulkUpdateMany(Query query, UpdateDefinition update, boolean upsert,
				@Nullable Object mappedUpdate, @Nullable Document mappedQuery) {
			this.query = query;
			this.update = update;
			this.upsert = upsert;
			this.mappedUpdate = mappedUpdate;
			this.mappedQuery = mappedQuery;
		}

		@Override
		public BulkOperationPipelineSupport.BulkOperationPipelineItem<BulkOperationContext, WriteModel<Document>> map(
				BulkOperationContext ctx) {
			Object mappedUpdate = ctx.mapUpdate(query, update, upsert);
			Document mappedQuery = ctx.mapQuery(query);
			return new CollectionBulkUpdateMany(query, update, upsert, mappedUpdate, mappedQuery);
		}

		@Override
		@SuppressWarnings("unchecked")
		public WriteModel<Document> prepareForWrite(BulkOperationContext ctx) {
			UpdateOptions options = ctx.getUpdateOptions(query, update, upsert, true);
			if (mappedUpdate instanceof List<?> pipeline) {
				return new UpdateManyModel<>(mappedQuery, (List<Document>) pipeline, options);
			}
			return new UpdateManyModel<>(mappedQuery, (Document) mappedUpdate, options);
		}
	}

	static final class CollectionBulkRemove extends CollectionBulkOperation {

		private final Query query;
		private @Nullable Document mappedQuery;

		CollectionBulkRemove(Query query) {
			this(query, null);
		}

		CollectionBulkRemove(Query query, @Nullable Document mappedQuery) {
			this.query = query;
			this.mappedQuery = mappedQuery;
		}

		@Override
		public BulkOperationPipelineSupport.BulkOperationPipelineItem<BulkOperationContext, WriteModel<Document>> map(
				BulkOperationContext ctx) {
			return new CollectionBulkRemove(query, ctx.mapQuery(query));
		}

		@Override
		public WriteModel<Document> prepareForWrite(BulkOperationContext ctx) {
			DeleteOptions deleteOptions = ctx.getDeleteOptions(query);
			return new DeleteManyModel<>(mappedQuery, deleteOptions);
		}
	}

	static final class CollectionBulkReplace extends CollectionBulkOperation {

		private final Query query;
		private final Object replacement;
		private final FindAndReplaceOptions options;
		private @Nullable Document mappedQuery;
		private @Nullable Document mappedReplacement;

		CollectionBulkReplace(Query query, Object replacement, FindAndReplaceOptions options) {
			this(query, replacement, options, null, null);
		}

		CollectionBulkReplace(Query query, Object replacement, FindAndReplaceOptions options,
				@Nullable Document mappedQuery, @Nullable Document mappedReplacement) {
			this.query = query;
			this.replacement = replacement;
			this.options = options;
			this.mappedQuery = mappedQuery;
			this.mappedReplacement = mappedReplacement;
		}

		@Override
		public BulkOperationPipelineSupport.BulkOperationPipelineItem<BulkOperationContext, WriteModel<Document>> map(
				BulkOperationContext ctx) {
			SourceAwareMappedDocument target = ctx.mapDomainObject(replacement);
			Document mappedQuery = ctx.mapQuery(query);
			return new CollectionBulkReplace(query, target.source(), options, mappedQuery, target.mapped());
		}

		@Override
		public WriteModel<Document> prepareForWrite(BulkOperationContext ctx) {
			com.mongodb.client.model.ReplaceOptions replaceOptions = ctx.getReplaceOptions(query, mappedReplacement, options.isUpsert());
			ctx.publishEvent(new org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent<>(replacement, mappedReplacement, ctx.collectionName()));
			ctx.callback(BeforeSaveCallback.class, replacement, mappedReplacement, ctx.collectionName());
			return new ReplaceOneModel<>(mappedQuery, mappedReplacement, replaceOptions);
		}

		@Override
		public void finish(BulkOperationContext ctx) {
			ctx.publishEvent(new org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent<>(replacement, mappedReplacement, ctx.collectionName()));
			ctx.callback(AfterSaveCallback.class, replacement, mappedReplacement, ctx.collectionName());
		}
	}
}
