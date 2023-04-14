/*
 * Copyright 2015-2023 the original author or authors.
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
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.aggregation.RelaxedTypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.core.query.UpdateDefinition.ArrayFilter;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;

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
class DefaultBulkOperations implements BulkOperations {

	private final MongoOperations mongoOperations;
	private final String collectionName;
	private final BulkOperationContext bulkOperationContext;
	private final List<SourceAwareWriteModelHolder> models = new ArrayList<>();

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
	public BulkOperations insert(Object document) {

		Assert.notNull(document, "Document must not be null");

		maybeEmitEvent(new BeforeConvertEvent<>(document, collectionName));
		Object source = maybeInvokeBeforeConvertCallback(document);
		addModel(source, new InsertOneModel<>(getMappedObject(source)));

		return this;
	}

	@Override
	public BulkOperations insert(List<? extends Object> documents) {

		Assert.notNull(documents, "Documents must not be null");

		documents.forEach(this::insert);

		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public BulkOperations updateOne(Query query, UpdateDefinition update) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");

		return update(query, update, false, false);
	}

	@Override
	public BulkOperations updateOne(List<Pair<Query, UpdateDefinition>> updates) {

		Assert.notNull(updates, "Updates must not be null");

		for (Pair<Query, UpdateDefinition> update : updates) {
			update(update.getFirst(), update.getSecond(), false, false);
		}

		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public BulkOperations updateMulti(Query query, UpdateDefinition update) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");

		update(query, update, false, true);

		return this;
	}

	@Override
	public BulkOperations updateMulti(List<Pair<Query, UpdateDefinition>> updates) {

		Assert.notNull(updates, "Updates must not be null");

		for (Pair<Query, UpdateDefinition> update : updates) {
			update(update.getFirst(), update.getSecond(), false, true);
		}

		return this;
	}

	@Override
	public BulkOperations upsert(Query query, UpdateDefinition update) {
		return update(query, update, true, true);
	}

	@Override
	public BulkOperations upsert(List<Pair<Query, Update>> updates) {

		for (Pair<Query, Update> update : updates) {
			upsert(update.getFirst(), update.getSecond());
		}

		return this;
	}

	@Override
	public BulkOperations remove(Query query) {

		Assert.notNull(query, "Query must not be null");

		DeleteOptions deleteOptions = new DeleteOptions();
		query.getCollation().map(Collation::toMongoCollation).ifPresent(deleteOptions::collation);

		addModel(query, new DeleteManyModel<>(query.getQueryObject(), deleteOptions));

		return this;
	}

	@Override
	public BulkOperations remove(List<Query> removes) {

		Assert.notNull(removes, "Removals must not be null");

		for (Query query : removes) {
			remove(query);
		}

		return this;
	}

	@Override
	public BulkOperations replaceOne(Query query, Object replacement, FindAndReplaceOptions options) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(replacement, "Replacement must not be null");
		Assert.notNull(options, "Options must not be null");

		ReplaceOptions replaceOptions = new ReplaceOptions();
		replaceOptions.upsert(options.isUpsert());
		query.getCollation().map(Collation::toMongoCollation).ifPresent(replaceOptions::collation);

		maybeEmitEvent(new BeforeConvertEvent<>(replacement, collectionName));
		Object source = maybeInvokeBeforeConvertCallback(replacement);
		addModel(source,
				new ReplaceOneModel<>(getMappedQuery(query.getQueryObject()), getMappedObject(source), replaceOptions));

		return this;
	}

	@Override
	public com.mongodb.bulk.BulkWriteResult execute() {

		try {

			com.mongodb.bulk.BulkWriteResult result = mongoOperations.execute(collectionName, this::bulkWriteTo);

			Assert.state(result != null, "Result must not be null");

			models.forEach(this::maybeEmitAfterSaveEvent);
			models.forEach(this::maybeInvokeAfterSaveCallback);

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

			return collection.bulkWrite( //
					models.stream() //
							.map(this::extractAndMapWriteModel) //
							.collect(Collectors.toList()), //
					bulkOptions);
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

	private WriteModel<Document> extractAndMapWriteModel(SourceAwareWriteModelHolder it) {

		maybeEmitBeforeSaveEvent(it);

		if (it.model() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) it.model()).getDocument();
			maybeInvokeBeforeSaveCallback(it.source(), target);
		} else if (it.model() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) it.model()).getReplacement();
			maybeInvokeBeforeSaveCallback(it.source(), target);
		}

		return mapWriteModel(it.source(), it.model());
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

		UpdateOptions options = computeUpdateOptions(query, update, upsert);

		if (multi) {
			addModel(update, new UpdateManyModel<>(query.getQueryObject(), update.getUpdateObject(), options));
		} else {
			addModel(update, new UpdateOneModel<>(query.getQueryObject(), update.getUpdateObject(), options));
		}

		return this;
	}

	private WriteModel<Document> mapWriteModel(Object source, WriteModel<Document> writeModel) {

		if (writeModel instanceof UpdateOneModel<Document> model) {

			if (source instanceof AggregationUpdate aggregationUpdate) {

				List<Document> pipeline = mapUpdatePipeline(aggregationUpdate);
				return new UpdateOneModel<>(getMappedQuery(model.getFilter()), pipeline, model.getOptions());
			}

			return new UpdateOneModel<>(getMappedQuery(model.getFilter()), getMappedUpdate(model.getUpdate()),
					model.getOptions());
		}

		if (writeModel instanceof UpdateManyModel<Document> model) {

			if (source instanceof AggregationUpdate aggregationUpdate) {

				List<Document> pipeline = mapUpdatePipeline(aggregationUpdate);
				return new UpdateManyModel<>(getMappedQuery(model.getFilter()), pipeline, model.getOptions());
			}

			return new UpdateManyModel<>(getMappedQuery(model.getFilter()), getMappedUpdate(model.getUpdate()),
					model.getOptions());
		}

		if (writeModel instanceof DeleteOneModel<Document> model) {
			return new DeleteOneModel<>(getMappedQuery(model.getFilter()), model.getOptions());
		}

		if (writeModel instanceof DeleteManyModel<Document> model) {
			return new DeleteManyModel<>(getMappedQuery(model.getFilter()), model.getOptions());
		}

		return writeModel;
	}

	private List<Document> mapUpdatePipeline(AggregationUpdate source) {

		Class<?> type = bulkOperationContext.entity().isPresent()
				? bulkOperationContext.entity().map(PersistentEntity::getType).get()
				: Object.class;
		AggregationOperationContext context = new RelaxedTypeBasedAggregationOperationContext(type,
				bulkOperationContext.updateMapper().getMappingContext(), bulkOperationContext.queryMapper());

		return new AggregationUtil(bulkOperationContext.queryMapper(),
				bulkOperationContext.queryMapper().getMappingContext()).createPipeline(source, context);
	}

	private Bson getMappedUpdate(Bson update) {
		return bulkOperationContext.updateMapper().getMappedObject(update, bulkOperationContext.entity());
	}

	private Bson getMappedQuery(Bson query) {
		return bulkOperationContext.queryMapper().getMappedObject(query, bulkOperationContext.entity());
	}

	private Document getMappedObject(Object source) {

		if (source instanceof Document) {
			return (Document) source;
		}

		Document sink = new Document();

		mongoOperations.getConverter().write(source, sink);
		return sink;
	}

	private void addModel(Object source, WriteModel<Document> model) {
		models.add(new SourceAwareWriteModelHolder(source, model));
	}

	private void maybeEmitBeforeSaveEvent(SourceAwareWriteModelHolder holder) {

		if (holder.model() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) holder.model()).getDocument();
			maybeEmitEvent(new BeforeSaveEvent<>(holder.source(), target, collectionName));
		} else if (holder.model() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) holder.model()).getReplacement();
			maybeEmitEvent(new BeforeSaveEvent<>(holder.source(), target, collectionName));
		}
	}

	private void maybeEmitAfterSaveEvent(SourceAwareWriteModelHolder holder) {

		if (holder.model() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) holder.model()).getDocument();
			maybeEmitEvent(new AfterSaveEvent<>(holder.source(), target, collectionName));
		} else if (holder.model() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) holder.model()).getReplacement();
			maybeEmitEvent(new AfterSaveEvent<>(holder.source(), target, collectionName));
		}
	}

	private void maybeInvokeAfterSaveCallback(SourceAwareWriteModelHolder holder) {

		if (holder.model() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) holder.model()).getDocument();
			maybeInvokeAfterSaveCallback(holder.source(), target);
		} else if (holder.model() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) holder.model()).getReplacement();
			maybeInvokeAfterSaveCallback(holder.source(), target);
		}
	}

	private void maybeEmitEvent(MongoMappingEvent<?> event) {
		bulkOperationContext.publishEvent(event);
	}

	private Object maybeInvokeBeforeConvertCallback(Object value) {
		return bulkOperationContext.callback(BeforeConvertCallback.class, value, collectionName);
	}

	private Object maybeInvokeBeforeSaveCallback(Object value, Document mappedDocument) {
		return bulkOperationContext.callback(BeforeSaveCallback.class, value, mappedDocument, collectionName);
	}

	private Object maybeInvokeAfterSaveCallback(Object value, Document mappedDocument) {
		return bulkOperationContext.callback(AfterSaveCallback.class, value, mappedDocument, collectionName);
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
	 * {@link BulkOperationContext} holds information about {@link BulkMode} the entity in use as well as references to
	 * {@link QueryMapper} and {@link UpdateMapper}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	record BulkOperationContext(BulkMode bulkMode, Optional<? extends MongoPersistentEntity<?>> entity,
			QueryMapper queryMapper, UpdateMapper updateMapper, @Nullable ApplicationEventPublisher eventPublisher,
			@Nullable EntityCallbacks entityCallbacks) {

		public boolean skipEventPublishing() {
			return eventPublisher == null;
		}

		public boolean skipEntityCallbacks() {
			return entityCallbacks == null;
		}

		@SuppressWarnings("rawtypes")
		public <T> T callback(Class<? extends EntityCallback> callbackType, T entity, String collectionName) {

			if (skipEntityCallbacks()) {
				return entity;
			}

			return entityCallbacks.callback(callbackType, entity, collectionName);
		}

		@SuppressWarnings("rawtypes")
		public <T> T callback(Class<? extends EntityCallback> callbackType, T entity, Document document,
				String collectionName) {

			if (skipEntityCallbacks()) {
				return entity;
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

	/**
	 * Value object chaining together an actual source with its {@link WriteModel} representation.
	 *
	 * @author Christoph Strobl
	 * @since 2.2
	 */
	record SourceAwareWriteModelHolder(Object source, WriteModel<Document> model) {

	}
}
