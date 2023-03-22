/*
 * Copyright 2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;
import org.springframework.data.mongodb.core.mapping.event.ReactiveAfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.core.query.UpdateDefinition.ArrayFilter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.*;
import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * Default implementation for {@link ReactiveBulkOperations}.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
class DefaultReactiveBulkOperations implements ReactiveBulkOperations {

	private final ReactiveMongoOperations mongoOperations;
	private final String collectionName;
	private final ReactiveBulkOperationContext bulkOperationContext;
	private final List<Mono<SourceAwareWriteModelHolder>> models = new ArrayList<>();

	private @Nullable WriteConcern defaultWriteConcern;

	private BulkWriteOptions bulkOptions;

	/**
	 * Creates a new {@link DefaultReactiveBulkOperations} for the given {@link MongoOperations}, collection name and
	 * {@link ReactiveBulkOperationContext}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param bulkOperationContext must not be {@literal null}.
	 */
	DefaultReactiveBulkOperations(ReactiveMongoOperations mongoOperations, String collectionName,
			ReactiveBulkOperationContext bulkOperationContext) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null");
		Assert.hasText(collectionName, "CollectionName must not be null nor empty");
		Assert.notNull(bulkOperationContext, "BulkOperationContext must not be null");

		this.mongoOperations = mongoOperations;
		this.collectionName = collectionName;
		this.bulkOperationContext = bulkOperationContext;
		this.bulkOptions = getBulkWriteOptions(bulkOperationContext.getBulkMode());
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
	public ReactiveBulkOperations insert(Object document) {

		Assert.notNull(document, "Document must not be null");

		this.models.add(Mono.just(document).flatMap(it -> {
			maybeEmitEvent(new BeforeConvertEvent<>(it, collectionName));
			return maybeInvokeBeforeConvertCallback(it);
		}).map(it -> new SourceAwareWriteModelHolder(it, new InsertOneModel(getMappedObject(it)))));

		return this;
	}

	@Override
	public ReactiveBulkOperations insert(List<? extends Object> documents) {

		Assert.notNull(documents, "Documents must not be null");

		documents.forEach(this::insert);

		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public ReactiveBulkOperations updateOne(Query query, UpdateDefinition update) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");

		update(query, update, false, false);
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public ReactiveBulkOperations updateMulti(Query query, UpdateDefinition update) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");

		update(query, update, false, true);
		return this;
	}

	@Override
	public ReactiveBulkOperations upsert(Query query, UpdateDefinition update) {
		return update(query, update, true, true);
	}

	@Override
	public ReactiveBulkOperations remove(Query query) {

		Assert.notNull(query, "Query must not be null");

		DeleteOptions deleteOptions = new DeleteOptions();
		query.getCollation().map(Collation::toMongoCollation).ifPresent(deleteOptions::collation);

		this.models.add(Mono.just(query)
				.map(it -> new SourceAwareWriteModelHolder(it, new DeleteManyModel<>(it.getQueryObject(), deleteOptions))));

		return this;
	}

	@Override
	public ReactiveBulkOperations remove(List<Query> removes) {

		Assert.notNull(removes, "Removals must not be null");

		for (Query query : removes) {
			remove(query);
		}

		return this;
	}

	@Override
	public ReactiveBulkOperations replaceOne(Query query, Object replacement, FindAndReplaceOptions options) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(replacement, "Replacement must not be null");
		Assert.notNull(options, "Options must not be null");

		ReplaceOptions replaceOptions = new ReplaceOptions();
		replaceOptions.upsert(options.isUpsert());
		query.getCollation().map(Collation::toMongoCollation).ifPresent(replaceOptions::collation);

		this.models.add(Mono.just(replacement).flatMap(it -> {
			maybeEmitEvent(new BeforeConvertEvent<>(it, collectionName));
			return maybeInvokeBeforeConvertCallback(it);
		}).map(it -> new SourceAwareWriteModelHolder(it,
				new ReplaceOneModel<>(getMappedQuery(query.getQueryObject()), getMappedObject(it), replaceOptions))));

		return this;
	}

	@Override
	public Mono<BulkWriteResult> execute() {

		try {

			Mono<BulkWriteResult> result = mongoOperations.execute(collectionName, this::bulkWriteTo).next();
			return result;
		} finally {
			this.bulkOptions = getBulkWriteOptions(bulkOperationContext.getBulkMode());
		}
	}

	private Mono<BulkWriteResult> bulkWriteTo(MongoCollection<Document> collection) {

		if (defaultWriteConcern != null) {
			collection = collection.withWriteConcern(defaultWriteConcern);
		}

		try {

			Flux<SourceAwareWriteModelHolder> concat = Flux.concat(models).flatMap(it -> {
				if (it.getModel()instanceof InsertOneModel<Document> insertOneModel) {

					Document target = insertOneModel.getDocument();
					maybeEmitBeforeSaveEvent(it);
					return maybeInvokeBeforeSaveCallback(it.getSource(), target)
							.map(afterCallback -> new SourceAwareWriteModelHolder(afterCallback, mapWriteModel(insertOneModel)));
				} else if (it.getModel()instanceof ReplaceOneModel<Document> replaceOneModel) {

					Document target = replaceOneModel.getReplacement();
					maybeEmitBeforeSaveEvent(it);
					return maybeInvokeBeforeSaveCallback(it.getSource(), target)
							.map(afterCallback -> new SourceAwareWriteModelHolder(afterCallback, mapWriteModel(replaceOneModel)));
				}
				return Mono.just(new SourceAwareWriteModelHolder(it.getSource(), mapWriteModel(it.getModel())));
			});
			MongoCollection theCollection = collection;
			return concat.collectList().flatMap(it -> {

				return Mono
						.from(theCollection.bulkWrite(
								it.stream().map(SourceAwareWriteModelHolder::getModel).collect(Collectors.toList()), bulkOptions))
						.doOnSuccess(state -> {
							it.forEach(saved -> {
								maybeEmitAfterSaveEvent(saved);
							});
						}).flatMap(state -> {
							List<Mono<Object>> monos = it.stream().map(saved -> {
								return maybeInvokeAfterSaveCallback(saved);
							}).collect(Collectors.toList());

							return Flux.concat(monos).then(Mono.just(state));
						});
			});
		} catch (RuntimeException ex) {

			if (ex instanceof MongoBulkWriteException) {

				MongoBulkWriteException mongoBulkWriteException = (MongoBulkWriteException) ex;
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
	private ReactiveBulkOperations update(Query query, UpdateDefinition update, boolean upsert, boolean multi) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");

		UpdateOptions options = computeUpdateOptions(query, update, upsert);

		this.models.add(Mono.just(update).map(it -> {
			if (multi) {
				return new SourceAwareWriteModelHolder(update,
						new UpdateManyModel<>(query.getQueryObject(), it.getUpdateObject(), options));
			}
			return new SourceAwareWriteModelHolder(update,
					new UpdateOneModel<>(query.getQueryObject(), it.getUpdateObject(), options));
		}));

		return this;
	}

	private WriteModel<Document> mapWriteModel(WriteModel<Document> writeModel) {

		if (writeModel instanceof UpdateOneModel) {

			UpdateOneModel<Document> model = (UpdateOneModel<Document>) writeModel;

			return new UpdateOneModel<>(getMappedQuery(model.getFilter()), getMappedUpdate(model.getUpdate()),
					model.getOptions());
		}

		if (writeModel instanceof UpdateManyModel) {

			UpdateManyModel<Document> model = (UpdateManyModel<Document>) writeModel;

			return new UpdateManyModel<>(getMappedQuery(model.getFilter()), getMappedUpdate(model.getUpdate()),
					model.getOptions());
		}

		if (writeModel instanceof DeleteOneModel) {

			DeleteOneModel<Document> model = (DeleteOneModel<Document>) writeModel;

			return new DeleteOneModel<>(getMappedQuery(model.getFilter()), model.getOptions());
		}

		if (writeModel instanceof DeleteManyModel) {

			DeleteManyModel<Document> model = (DeleteManyModel<Document>) writeModel;

			return new DeleteManyModel<>(getMappedQuery(model.getFilter()), model.getOptions());
		}

		return writeModel;
	}

	private Bson getMappedUpdate(Bson update) {
		return bulkOperationContext.getUpdateMapper().getMappedObject(update, bulkOperationContext.getEntity());
	}

	private Bson getMappedQuery(Bson query) {
		return bulkOperationContext.getQueryMapper().getMappedObject(query, bulkOperationContext.getEntity());
	}

	private Document getMappedObject(Object source) {

		if (source instanceof Document) {
			return (Document) source;
		}

		Document sink = new Document();

		mongoOperations.getConverter().write(source, sink);
		return sink;
	}

	private void maybeEmitBeforeSaveEvent(SourceAwareWriteModelHolder holder) {

		if (holder.getModel() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) holder.getModel()).getDocument();
			maybeEmitEvent(new BeforeSaveEvent<>(holder.getSource(), target, collectionName));
		} else if (holder.getModel() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) holder.getModel()).getReplacement();
			maybeEmitEvent(new BeforeSaveEvent<>(holder.getSource(), target, collectionName));
		}
	}

	private void maybeEmitAfterSaveEvent(SourceAwareWriteModelHolder holder) {

		if (holder.getModel() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) holder.getModel()).getDocument();
			maybeEmitEvent(new AfterSaveEvent<>(holder.getSource(), target, collectionName));
		} else if (holder.getModel() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) holder.getModel()).getReplacement();
			maybeEmitEvent(new AfterSaveEvent<>(holder.getSource(), target, collectionName));
		}
	}

	private Mono<Object> maybeInvokeAfterSaveCallback(SourceAwareWriteModelHolder holder) {

		if (holder.getModel() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) holder.getModel()).getDocument();
			return maybeInvokeAfterSaveCallback(holder.getSource(), target);
		} else if (holder.getModel() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) holder.getModel()).getReplacement();
			return maybeInvokeAfterSaveCallback(holder.getSource(), target);
		}
		return Mono.just(holder.getSource());
	}

	private <E extends MongoMappingEvent<T>, T> E maybeEmitEvent(E event) {

		if (bulkOperationContext.getEventPublisher() == null) {
			return event;
		}

		bulkOperationContext.getEventPublisher().publishEvent(event);
		return event;
	}

	private Mono<Object> maybeInvokeBeforeConvertCallback(Object value) {

		if (bulkOperationContext.getEntityCallbacks() == null) {
			return Mono.just(value);
		}

		return bulkOperationContext.getEntityCallbacks().callback(ReactiveBeforeConvertCallback.class, value,
				collectionName);
	}

	private Mono<Object> maybeInvokeBeforeSaveCallback(Object value, Document mappedDocument) {

		if (bulkOperationContext.getEntityCallbacks() == null) {
			return Mono.just(value);
		}

		return bulkOperationContext.getEntityCallbacks().callback(ReactiveBeforeSaveCallback.class, value, mappedDocument,
				collectionName);
	}

	private Mono<Object> maybeInvokeAfterSaveCallback(Object value, Document mappedDocument) {

		if (bulkOperationContext.getEntityCallbacks() == null) {
			return Mono.just(value);
		}

		return bulkOperationContext.getEntityCallbacks().callback(ReactiveAfterSaveCallback.class, value, mappedDocument,
				collectionName);
	}

	private static BulkWriteOptions getBulkWriteOptions(BulkMode bulkMode) {

		BulkWriteOptions options = new BulkWriteOptions();

		switch (bulkMode) {
			case ORDERED:
				return options.ordered(true);
			case UNORDERED:
				return options.ordered(false);
		}

		throw new IllegalStateException("BulkMode was null");
	}

	/**
	 * @param filterQuery The {@link Query} to read a potential {@link Collation} from. Must not be {@literal null}.
	 * @param update The {@link Update} to apply
	 * @param upsert flag to indicate if document should be upserted.
	 * @return new instance of {@link UpdateOptions}.
	 */
	private static UpdateOptions computeUpdateOptions(Query filterQuery, UpdateDefinition update, boolean upsert) {

		UpdateOptions options = new UpdateOptions();
		options.upsert(upsert);

		if (update.hasArrayFilters()) {
			List<Document> list = new ArrayList<>(update.getArrayFilters().size());
			for (ArrayFilter arrayFilter : update.getArrayFilters()) {
				list.add(arrayFilter.asDocument());
			}
			options.arrayFilters(list);
		}

		filterQuery.getCollation().map(Collation::toMongoCollation).ifPresent(options::collation);
		return options;
	}

	/**
	 * {@link ReactiveBulkOperationContext} holds information about {@link BulkMode} the entity in use as well as
	 * references to {@link QueryMapper} and {@link UpdateMapper}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static final class ReactiveBulkOperationContext {

		private final BulkMode bulkMode;
		private final Optional<? extends MongoPersistentEntity<?>> entity;
		private final QueryMapper queryMapper;
		private final UpdateMapper updateMapper;
		private final ApplicationEventPublisher eventPublisher;
		private final ReactiveEntityCallbacks entityCallbacks;

		ReactiveBulkOperationContext(BulkMode bulkMode, Optional<? extends MongoPersistentEntity<?>> entity,
				QueryMapper queryMapper, UpdateMapper updateMapper, ApplicationEventPublisher eventPublisher,
				ReactiveEntityCallbacks entityCallbacks) {

			this.bulkMode = bulkMode;
			this.entity = entity;
			this.queryMapper = queryMapper;
			this.updateMapper = updateMapper;
			this.eventPublisher = eventPublisher;
			this.entityCallbacks = entityCallbacks;
		}

		public BulkMode getBulkMode() {
			return this.bulkMode;
		}

		public Optional<? extends MongoPersistentEntity<?>> getEntity() {
			return this.entity;
		}

		public QueryMapper getQueryMapper() {
			return this.queryMapper;
		}

		public UpdateMapper getUpdateMapper() {
			return this.updateMapper;
		}

		public ApplicationEventPublisher getEventPublisher() {
			return this.eventPublisher;
		}

		public ReactiveEntityCallbacks getEntityCallbacks() {
			return this.entityCallbacks;
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			ReactiveBulkOperationContext that = (ReactiveBulkOperationContext) o;

			if (bulkMode != that.bulkMode)
				return false;
			if (!ObjectUtils.nullSafeEquals(this.entity, that.entity)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(this.queryMapper, that.queryMapper)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(this.updateMapper, that.updateMapper)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(this.eventPublisher, that.eventPublisher)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(this.entityCallbacks, that.entityCallbacks);
		}

		@Override
		public int hashCode() {
			int result = bulkMode != null ? bulkMode.hashCode() : 0;
			result = 31 * result + ObjectUtils.nullSafeHashCode(entity);
			result = 31 * result + ObjectUtils.nullSafeHashCode(queryMapper);
			result = 31 * result + ObjectUtils.nullSafeHashCode(updateMapper);
			result = 31 * result + ObjectUtils.nullSafeHashCode(eventPublisher);
			result = 31 * result + ObjectUtils.nullSafeHashCode(entityCallbacks);
			return result;
		}

		public String toString() {
			return "DefaultBulkOperations.BulkOperationContext(bulkMode=" + this.getBulkMode() + ", entity="
					+ this.getEntity() + ", queryMapper=" + this.getQueryMapper() + ", updateMapper=" + this.getUpdateMapper()
					+ ", eventPublisher=" + this.getEventPublisher() + ", entityCallbacks=" + this.getEntityCallbacks() + ")";
		}
	}

	/**
	 * Value object chaining together an actual source with its {@link WriteModel} representation.
	 *
	 * @since 4.1
	 * @author Christoph Strobl
	 */
	private static final class SourceAwareWriteModelHolder {

		private final Object source;
		private final WriteModel<Document> model;

		SourceAwareWriteModelHolder(Object source, WriteModel<Document> model) {

			this.source = source;
			this.model = model;
		}

		public Object getSource() {
			return this.source;
		}

		public WriteModel<Document> getModel() {
			return this.model;
		}
	}
}
