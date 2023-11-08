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
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.ReactiveAfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * Default implementation for {@link ReactiveBulkOperations}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 4.1
 */
class DefaultReactiveBulkOperations extends BulkOperationsSupport implements ReactiveBulkOperations {

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

		super(collectionName);

		Assert.notNull(mongoOperations, "MongoOperations must not be null");
		Assert.hasText(collectionName, "CollectionName must not be null nor empty");
		Assert.notNull(bulkOperationContext, "BulkOperationContext must not be null");

		this.mongoOperations = mongoOperations;
		this.collectionName = collectionName;
		this.bulkOperationContext = bulkOperationContext;
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
	public ReactiveBulkOperations insert(Object document) {

		Assert.notNull(document, "Document must not be null");

		this.models.add(Mono.just(document).flatMap(it -> {
			maybeEmitEvent(new BeforeConvertEvent<>(it, collectionName));
			return maybeInvokeBeforeConvertCallback(it);
		}).map(it -> new SourceAwareWriteModelHolder(it, new InsertOneModel<>(getMappedObject(it)))));

		return this;
	}

	@Override
	public ReactiveBulkOperations insert(List<? extends Object> documents) {

		Assert.notNull(documents, "Documents must not be null");

		documents.forEach(this::insert);

		return this;
	}

	@Override
	public ReactiveBulkOperations updateOne(Query query, UpdateDefinition update) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");

		update(query, update, false, false);
		return this;
	}

	@Override
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
			return mongoOperations.execute(collectionName, this::bulkWriteTo).next();
		} finally {
			this.bulkOptions = getBulkWriteOptions(bulkOperationContext.bulkMode());
		}
	}

	private Mono<BulkWriteResult> bulkWriteTo(MongoCollection<Document> collection) {

		if (defaultWriteConcern != null) {
			collection = collection.withWriteConcern(defaultWriteConcern);
		}

		Flux<SourceAwareWriteModelHolder> concat = Flux.concat(models).flatMapSequential(it -> {

			if (it.model()instanceof InsertOneModel<Document> iom) {

				Document target = iom.getDocument();
				maybeEmitBeforeSaveEvent(it);
				return maybeInvokeBeforeSaveCallback(it.source(), target)
						.map(afterCallback -> new SourceAwareWriteModelHolder(afterCallback, mapWriteModel(afterCallback, iom)));
			} else if (it.model()instanceof ReplaceOneModel<Document> rom) {

				Document target = rom.getReplacement();
				maybeEmitBeforeSaveEvent(it);
				return maybeInvokeBeforeSaveCallback(it.source(), target)
						.map(afterCallback -> new SourceAwareWriteModelHolder(afterCallback, mapWriteModel(afterCallback, rom)));
			}

			return Mono.just(new SourceAwareWriteModelHolder(it.source(), mapWriteModel(it.source(), it.model())));
		});

		MongoCollection<Document> theCollection = collection;
		return concat.collectList().flatMap(it -> {

			return Mono
					.from(theCollection
							.bulkWrite(it.stream().map(SourceAwareWriteModelHolder::model).collect(Collectors.toList()), bulkOptions))
					.doOnSuccess(state -> {
						it.forEach(this::maybeEmitAfterSaveEvent);
					}).flatMap(state -> {
						List<Mono<Object>> monos = it.stream().map(this::maybeInvokeAfterSaveCallback).collect(Collectors.toList());

						return Flux.concat(monos).then(Mono.just(state));
					});
		});
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
		return bulkOperationContext.entity();
	}

	private Document getMappedObject(Object source) {

		if (source instanceof Document) {
			return (Document) source;
		}

		Document sink = new Document();

		mongoOperations.getConverter().write(source, sink);
		return sink;
	}

	private Mono<Object> maybeInvokeAfterSaveCallback(SourceAwareWriteModelHolder holder) {

		if (holder.model() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) holder.model()).getDocument();
			return maybeInvokeAfterSaveCallback(holder.source(), target);
		} else if (holder.model() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) holder.model()).getReplacement();
			return maybeInvokeAfterSaveCallback(holder.source(), target);
		}
		return Mono.just(holder.source());
	}

	private Mono<Object> maybeInvokeBeforeConvertCallback(Object value) {
		return bulkOperationContext.callback(ReactiveBeforeConvertCallback.class, value, collectionName);
	}

	private Mono<Object> maybeInvokeBeforeSaveCallback(Object value, Document mappedDocument) {
		return bulkOperationContext.callback(ReactiveBeforeSaveCallback.class, value, mappedDocument, collectionName);
	}

	private Mono<Object> maybeInvokeAfterSaveCallback(Object value, Document mappedDocument) {
		return bulkOperationContext.callback(ReactiveAfterSaveCallback.class, value, mappedDocument, collectionName);
	}

	/**
	 * {@link ReactiveBulkOperationContext} holds information about {@link BulkMode} the entity in use as well as
	 * references to {@link QueryMapper} and {@link UpdateMapper}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	record ReactiveBulkOperationContext(BulkMode bulkMode, Optional<? extends MongoPersistentEntity<?>> entity,
			QueryMapper queryMapper, UpdateMapper updateMapper, @Nullable ApplicationEventPublisher eventPublisher,
			@Nullable ReactiveEntityCallbacks entityCallbacks) {

		public boolean skipEntityCallbacks() {
			return entityCallbacks == null;
		}

		public boolean skipEventPublishing() {
			return eventPublisher == null;
		}

		@SuppressWarnings("rawtypes")
		public <T> Mono<T> callback(Class<? extends EntityCallback> callbackType, T entity, String collectionName) {

			if (skipEntityCallbacks()) {
				return Mono.just(entity);
			}

			return entityCallbacks.callback(callbackType, entity, collectionName);
		}

		@SuppressWarnings("rawtypes")
		public <T> Mono<T> callback(Class<? extends EntityCallback> callbackType, T entity, Document document,
				String collectionName) {

			if (skipEntityCallbacks()) {
				return Mono.just(entity);
			}

			return entityCallbacks.callback(callbackType, entity, document, collectionName);
		}

		public void publishEvent(ApplicationEvent event) {

			if (skipEventPublishing()) {
				return;
			}

			eventPublisher.publishEvent(event);
		}
	}

}
