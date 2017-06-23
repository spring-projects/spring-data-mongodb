/*
 * Copyright 2015-2016 the original author or authors.
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

import lombok.Data;

import java.util.Arrays;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Pair;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteRequestBuilder;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

/**
 * Default implementation for {@link BulkOperations}.
 *
 * @author Tobias Trelle
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @since 1.9
 */
class DefaultBulkOperations implements BulkOperations {

	private final MongoOperations mongoOperations;
	private final String collectionName;
	private final BulkOperationContext bulkOperationContext;

	private WriteConcernResolver writeConcernResolver;
	private PersistenceExceptionTranslator exceptionTranslator;
	private WriteConcern defaultWriteConcern;

	private BulkWriteOperation bulk;

	/**
	 * Creates a new {@link DefaultBulkOperations} for the given {@link MongoOperations}, collection name and
	 * {@link BulkOperationContext}.
	 *
	 * @param mongoOperations must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @param bulkOperationContext must not be {@literal null}.
	 * @since 1.10.5
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
		this.bulk = initBulkOperation();
	}

	/**
	 * Configures the {@link PersistenceExceptionTranslator} to be used. Defaults to {@link MongoExceptionTranslator}.
	 *
	 * @param exceptionTranslator can be {@literal null}.
	 */
	public void setExceptionTranslator(PersistenceExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator == null ? new MongoExceptionTranslator() : exceptionTranslator;
	}

	/**
	 * Configures the {@link WriteConcernResolver} to be used. Defaults to {@link DefaultWriteConcernResolver}.
	 *
	 * @param writeConcernResolver can be {@literal null}.
	 */
	public void setWriteConcernResolver(WriteConcernResolver writeConcernResolver) {
		this.writeConcernResolver = writeConcernResolver == null ? DefaultWriteConcernResolver.INSTANCE
				: writeConcernResolver;
	}

	/**
	 * Configures the default {@link WriteConcern} to be used. Defaults to {@literal null}.
	 *
	 * @param defaultWriteConcern can be {@literal null}.
	 */
	public void setDefaultWriteConcern(WriteConcern defaultWriteConcern) {
		this.defaultWriteConcern = defaultWriteConcern;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#insert(java.lang.Object)
	 */
	@Override
	public BulkOperations insert(Object document) {

		Assert.notNull(document, "Document must not be null!");

		if (document instanceof DBObject) {

			bulk.insert((DBObject) document);
			return this;
		}

		DBObject sink = new BasicDBObject();
		mongoOperations.getConverter().write(document, sink);
		bulk.insert(sink);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.BulkOperations#insert(java.util.List)
	 */
	@Override
	public BulkOperations insert(List<? extends Object> documents) {

		Assert.notNull(documents, "Documents must not be null!");

		for (Object document : documents) {
			insert(document);
		}

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

		return updateOne(Arrays.asList(Pair.of(query, update)));
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

		return updateMulti(Arrays.asList(Pair.of(query, update)));
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

		bulk.find(getMappedQuery(query.getQueryObject())).remove();

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
	public BulkWriteResult execute() {

		MongoAction action = new MongoAction(defaultWriteConcern, MongoActionOperation.BULK, collectionName,
				bulkOperationContext.getEntityType(), null, null);
		WriteConcern writeConcern = writeConcernResolver.resolve(action);

		try {
			return writeConcern == null ? bulk.execute() : bulk.execute(writeConcern);
		} catch (BulkWriteException o_O) {

			DataAccessException toThrow = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw toThrow == null ? o_O : toThrow;

		} finally {
			this.bulk = initBulkOperation();
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

		BulkWriteRequestBuilder builder = bulk.find(query.getQueryObject());

		if (upsert) {

			if (multi) {
				builder.upsert().update(getMappedUpdate(update.getUpdateObject()));
			} else {
				builder.upsert().updateOne(getMappedUpdate(update.getUpdateObject()));
			}

		} else {

			if (multi) {
				builder.update(getMappedUpdate(update.getUpdateObject()));
			} else {
				builder.updateOne(getMappedUpdate(update.getUpdateObject()));
			}
		}

		return this;
	}

	private final BulkWriteOperation initBulkOperation() {

		DBCollection collection = mongoOperations.getCollection(collectionName);

		switch (bulkOperationContext.getBulkMode()) {
			case ORDERED:
				return collection.initializeOrderedBulkOperation();
			case UNORDERED:
				return collection.initializeUnorderedBulkOperation();
		}

		throw new IllegalStateException("BulkMode was null!");
	}

	private DBObject getMappedUpdate(DBObject update) {
		return bulkOperationContext.getUpdateMapper().getMappedObject(update, bulkOperationContext.getEntity());
	}

	private DBObject getMappedQuery(DBObject query) {
		return bulkOperationContext.getQueryMapper().getMappedObject(query, bulkOperationContext.getEntity());
	}

	/**
	 * {@link BulkOperationContext} holds information about
	 * {@link org.springframework.data.mongodb.core.BulkOperations.BulkMode} the entity in use as well as references to
	 * {@link QueryMapper} and {@link UpdateMapper}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@Data
	static class BulkOperationContext {

		final BulkMode bulkMode;
		final MongoPersistentEntity<?> entity;
		final QueryMapper queryMapper;
		final UpdateMapper updateMapper;

		Class<?> getEntityType() {
			return entity != null ? entity.getType() : null;
		}
	}
}
