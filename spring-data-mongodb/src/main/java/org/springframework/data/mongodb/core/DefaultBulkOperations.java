/*
 * Copyright 2015-2021 the original author or authors.
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mongodb.BulkOperationException;
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
import org.springframework.util.ObjectUtils;

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

		Assert.notNull(mongoOperations, "MongoOperations must not be null!");
		Assert.hasText(collectionName, "CollectionName must not be null nor empty!");
		Assert.notNull(bulkOperationContext, "BulkOperationContext must not be null!");

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
	public BulkOperations insert(Object document) {

		Assert.notNull(document, "Document must not be null!");

		maybeEmitEvent(new BeforeConvertEvent<>(document, collectionName));
		Object source = maybeInvokeBeforeConvertCallback(document);
		addModel(source, new InsertOneModel<>(getMappedObject(source)));

		return this;
	}

	@Override
	public BulkOperations insert(List<? extends Object> documents) {

		Assert.notNull(documents, "Documents must not be null!");

		documents.forEach(this::insert);

		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public BulkOperations updateOne(Query query, Update update) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(update, "Update must not be null!");

		return updateOne(Collections.singletonList(Pair.of(query, update)));
	}

	@Override
	public BulkOperations updateOne(List<Pair<Query, Update>> updates) {

		Assert.notNull(updates, "Updates must not be null!");

		for (Pair<Query, Update> update : updates) {
			update(update.getFirst(), update.getSecond(), false, false);
		}

		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public BulkOperations updateMulti(Query query, Update update) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(update, "Update must not be null!");

		return updateMulti(Collections.singletonList(Pair.of(query, update)));
	}

	@Override
	public BulkOperations updateMulti(List<Pair<Query, Update>> updates) {

		Assert.notNull(updates, "Updates must not be null!");

		for (Pair<Query, Update> update : updates) {
			update(update.getFirst(), update.getSecond(), false, true);
		}

		return this;
	}

	@Override
	public BulkOperations upsert(Query query, Update update) {
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

		Assert.notNull(query, "Query must not be null!");

		DeleteOptions deleteOptions = new DeleteOptions();
		query.getCollation().map(Collation::toMongoCollation).ifPresent(deleteOptions::collation);

		addModel(query, new DeleteManyModel<>(query.getQueryObject(), deleteOptions));

		return this;
	}

	@Override
	public BulkOperations remove(List<Query> removes) {

		Assert.notNull(removes, "Removals must not be null!");

		for (Query query : removes) {
			remove(query);
		}

		return this;
	}

	@Override
	public BulkOperations replaceOne(Query query, Object replacement, FindAndReplaceOptions options) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(replacement, "Replacement must not be null!");
		Assert.notNull(options, "Options must not be null!");

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

			Assert.state(result != null, "Result must not be null.");

			models.forEach(this::maybeEmitAfterSaveEvent);
			models.forEach(this::maybeInvokeAfterSaveCallback);

			return result;
		} finally {
			this.bulkOptions = getBulkWriteOptions(bulkOperationContext.getBulkMode());
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

	private WriteModel<Document> extractAndMapWriteModel(SourceAwareWriteModelHolder it) {

		maybeEmitBeforeSaveEvent(it);

		if (it.getModel() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) it.getModel()).getDocument();
			maybeInvokeBeforeSaveCallback(it.getSource(), target);
		} else if (it.getModel() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) it.getModel()).getReplacement();
			maybeInvokeBeforeSaveCallback(it.getSource(), target);
		}

		return mapWriteModel(it.getModel());
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
	private BulkOperations update(Query query, Update update, boolean upsert, boolean multi) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(update, "Update must not be null!");

		UpdateOptions options = computeUpdateOptions(query, update, upsert);

		if (multi) {
			addModel(update, new UpdateManyModel<>(query.getQueryObject(), update.getUpdateObject(), options));
		} else {
			addModel(update, new UpdateOneModel<>(query.getQueryObject(), update.getUpdateObject(), options));
		}

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

	private void addModel(Object source, WriteModel<Document> model) {
		models.add(new SourceAwareWriteModelHolder(source, model));
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

	private void maybeInvokeAfterSaveCallback(SourceAwareWriteModelHolder holder) {

		if (holder.getModel() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) holder.getModel()).getDocument();
			maybeInvokeAfterSaveCallback(holder.getSource(), target);
		} else if (holder.getModel() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) holder.getModel()).getReplacement();
			maybeInvokeAfterSaveCallback(holder.getSource(), target);
		}
	}

	private <E extends MongoMappingEvent<T>, T> E maybeEmitEvent(E event) {

		if (bulkOperationContext.getEventPublisher() == null) {
			return event;
		}

		bulkOperationContext.getEventPublisher().publishEvent(event);
		return event;
	}

	private Object maybeInvokeBeforeConvertCallback(Object value) {

		if (bulkOperationContext.getEntityCallbacks() == null) {
			return value;
		}

		return bulkOperationContext.getEntityCallbacks().callback(BeforeConvertCallback.class, value, collectionName);
	}

	private Object maybeInvokeBeforeSaveCallback(Object value, Document mappedDocument) {

		if (bulkOperationContext.getEntityCallbacks() == null) {
			return value;
		}

		return bulkOperationContext.getEntityCallbacks().callback(BeforeSaveCallback.class, value, mappedDocument,
				collectionName);
	}

	private Object maybeInvokeAfterSaveCallback(Object value, Document mappedDocument) {

		if (bulkOperationContext.getEntityCallbacks() == null) {
			return value;
		}

		return bulkOperationContext.getEntityCallbacks().callback(AfterSaveCallback.class, value, mappedDocument,
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

		throw new IllegalStateException("BulkMode was null!");
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
	 * {@link BulkOperationContext} holds information about
	 * {@link org.springframework.data.mongodb.core.BulkOperations.BulkMode} the entity in use as well as references to
	 * {@link QueryMapper} and {@link UpdateMapper}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static final class BulkOperationContext {

		private final BulkMode bulkMode;
		private final Optional<? extends MongoPersistentEntity<?>> entity;
		private final QueryMapper queryMapper;
		private final UpdateMapper updateMapper;
		private final ApplicationEventPublisher eventPublisher;
		private final EntityCallbacks entityCallbacks;

		BulkOperationContext(BulkOperations.BulkMode bulkMode, Optional<? extends MongoPersistentEntity<?>> entity,
				QueryMapper queryMapper, UpdateMapper updateMapper, ApplicationEventPublisher eventPublisher,
				EntityCallbacks entityCallbacks) {

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

		public EntityCallbacks getEntityCallbacks() {
			return this.entityCallbacks;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			BulkOperationContext that = (BulkOperationContext) o;

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
	 * @since 2.2
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

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			SourceAwareWriteModelHolder that = (SourceAwareWriteModelHolder) o;

			if (!ObjectUtils.nullSafeEquals(this.source, that.source)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(this.model, that.model);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(model);
			result = 31 * result + ObjectUtils.nullSafeHashCode(source);
			return result;
		}

		public String toString() {
			return "DefaultBulkOperations.SourceAwareWriteModelHolder(source=" + this.getSource() + ", model="
					+ this.getModel() + ")";
		}
	}
}
