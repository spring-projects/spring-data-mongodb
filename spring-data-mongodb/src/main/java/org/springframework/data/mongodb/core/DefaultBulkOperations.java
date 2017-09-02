/*
 * Copyright 2015-2017 the original author or authors.
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

import lombok.NonNull;
import lombok.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.BulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.InsertOneModel;
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
 * @since 1.9
 */
class DefaultBulkOperations implements BulkOperations {

	private final MongoOperations mongoOperations;
	private final String collectionName;
	private final BulkOperationContext bulkOperationContext;
	private final List<WriteModel<Document>> models = new ArrayList<>();

	private PersistenceExceptionTranslator exceptionTranslator;
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
		this.exceptionTranslator = new MongoExceptionTranslator();
		this.bulkOptions = getBulkWriteOptions(bulkOperationContext.getBulkMode());
	}

	/**
	 * Configures the {@link PersistenceExceptionTranslator} to be used. Defaults to {@link MongoExceptionTranslator}.
	 *
	 * @param exceptionTranslator can be {@literal null}.
	 */
	public void setExceptionTranslator(@Nullable PersistenceExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator == null ? new MongoExceptionTranslator() : exceptionTranslator;
	}

	/**
	 * Configures the default {@link WriteConcern} to be used. Defaults to {@literal null}.
	 *
	 * @param defaultWriteConcern can be {@literal null}.
	 */
	void setDefaultWriteConcern(@Nullable WriteConcern defaultWriteConcern) {
		this.defaultWriteConcern = defaultWriteConcern;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#insert(java.lang.Object)
	 */
	@Override
	public BulkOperations insert(Object document) {

		Assert.notNull(document, "Document must not be null!");

		if (document instanceof Document) {

			models.add(new InsertOneModel<>((Document) document));
			return this;
		}

		Document sink = new Document();
		mongoOperations.getConverter().write(document, sink);

		models.add(new InsertOneModel<>(sink));

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#insert(java.util.List)
	 */
	@Override
	public BulkOperations insert(List<? extends Object> documents) {

		Assert.notNull(documents, "Documents must not be null!");

		documents.forEach(this::insert);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#updateOne(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public BulkOperations updateOne(Query query, Update update) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(update, "Update must not be null!");

		return updateOne(Collections.singletonList(Pair.of(query, update)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#updateOne(java.util.List)
	 */
	@Override
	public BulkOperations updateOne(List<Pair<Query, Update>> updates) {

		Assert.notNull(updates, "Updates must not be null!");

		for (Pair<Query, Update> update : updates) {
			update(update.getFirst(), update.getSecond(), false, false);
		}

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#updateMulti(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public BulkOperations updateMulti(Query query, Update update) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(update, "Update must not be null!");

		return updateMulti(Collections.singletonList(Pair.of(query, update)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#updateMulti(java.util.List)
	 */
	@Override
	public BulkOperations updateMulti(List<Pair<Query, Update>> updates) {

		Assert.notNull(updates, "Updates must not be null!");

		for (Pair<Query, Update> update : updates) {
			update(update.getFirst(), update.getSecond(), false, true);
		}

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#upsert(org.springframework.data.mongodb.core.query.Query, org.springframework.data.mongodb.core.query.Update)
	 */
	@Override
	public BulkOperations upsert(Query query, Update update) {
		return update(query, update, true, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#upsert(java.util.List)
	 */
	@Override
	public BulkOperations upsert(List<Pair<Query, Update>> updates) {

		for (Pair<Query, Update> update : updates) {
			upsert(update.getFirst(), update.getSecond());
		}

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#remove(org.springframework.data.mongodb.core.query.Query)
	 */
	@Override
	public BulkOperations remove(Query query) {

		Assert.notNull(query, "Query must not be null!");

		DeleteOptions deleteOptions = new DeleteOptions();
		query.getCollation().map(Collation::toMongoCollation).ifPresent(deleteOptions::collation);

		models.add(new DeleteManyModel<>(query.getQueryObject(), deleteOptions));

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#remove(java.util.List)
	 */
	@Override
	public BulkOperations remove(List<Query> removes) {

		Assert.notNull(removes, "Removals must not be null!");

		for (Query query : removes) {
			remove(query);
		}

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#executeBulk()
	 */
	@Override
	public com.mongodb.bulk.BulkWriteResult execute() {

		try {

			MongoCollection<Document> collection = mongoOperations.getCollection(collectionName);
			if (defaultWriteConcern != null) {
				collection = collection.withWriteConcern(defaultWriteConcern);
			}

			return collection.bulkWrite(models.stream().map(this::mapWriteModel).collect(Collectors.toList()), bulkOptions);

		} catch (BulkWriteException o_O) {

			DataAccessException toThrow = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw toThrow == null ? o_O : toThrow;

		} finally {
			this.bulkOptions = getBulkWriteOptions(bulkOperationContext.getBulkMode());
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
	private BulkOperations update(Query query, Update update, boolean upsert, boolean multi) {

		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(update, "Update must not be null!");

		UpdateOptions options = new UpdateOptions();
		options.upsert(upsert);
		query.getCollation().map(Collation::toMongoCollation).ifPresent(options::collation);

		if (multi) {
			models.add(new UpdateManyModel<>(query.getQueryObject(), update.getUpdateObject(), options));
		} else {
			models.add(new UpdateOneModel<>(query.getQueryObject(), update.getUpdateObject(), options));
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
	 * {@link BulkOperationContext} holds information about
	 * {@link org.springframework.data.mongodb.core.BulkOperations.BulkMode} the entity in use as well as references to
	 * {@link QueryMapper} and {@link UpdateMapper}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@Value
	static class BulkOperationContext {

		@NonNull BulkMode bulkMode;
		@NonNull Optional<? extends MongoPersistentEntity<?>> entity;
		@NonNull QueryMapper queryMapper;
		@NonNull UpdateMapper updateMapper;
	}
}
