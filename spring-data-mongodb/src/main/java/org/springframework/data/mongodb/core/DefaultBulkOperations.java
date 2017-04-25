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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.client.model.DeleteOptions;
import org.bson.Document;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Pair;
import org.springframework.util.Assert;

import com.mongodb.BulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
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
 * @since 1.9
 */
class DefaultBulkOperations implements BulkOperations {

	private final MongoOperations mongoOperations;
	private final BulkMode bulkMode;
	private final String collectionName;

	private PersistenceExceptionTranslator exceptionTranslator;
	private WriteConcernResolver writeConcernResolver;
	private WriteConcern defaultWriteConcern;

	private BulkWriteOptions bulkOptions;

	List<WriteModel<Document>> models = new ArrayList<>();

	/**
	 * Creates a new {@link DefaultBulkOperations} for the given {@link MongoOperations}, {@link BulkMode}, collection
	 * name and {@link WriteConcern}.
	 *
	 * @param mongoOperations The underlying {@link MongoOperations}, must not be {@literal null}.
	 * @param bulkMode must not be {@literal null}.
	 * @param collectionName Name of the collection to work on, must not be {@literal null} or empty.
	 * @param entityType the entity type, can be {@literal null}.
	 */
	DefaultBulkOperations(MongoOperations mongoOperations, BulkMode bulkMode, String collectionName,
			Class<?> entityType) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null!");
		Assert.notNull(bulkMode, "BulkMode must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		this.mongoOperations = mongoOperations;
		this.bulkMode = bulkMode;
		this.collectionName = collectionName;

		this.exceptionTranslator = new MongoExceptionTranslator();
		this.writeConcernResolver = DefaultWriteConcernResolver.INSTANCE;

		this.bulkOptions = initBulkOperation();
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

		DeleteOptions deleteOptions = new DeleteOptions();
		query.getCollation().map(Collation::toMongoCollation).ifPresent(deleteOptions::collation);

		models.add(new DeleteManyModel(query.getQueryObject(), deleteOptions));
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

			return collection.bulkWrite(models, bulkOptions);

		} catch (BulkWriteException o_O) {

			DataAccessException toThrow = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw toThrow == null ? o_O : toThrow;

		} finally {
			this.bulkOptions = initBulkOperation();
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

	private final BulkWriteOptions initBulkOperation() {

		BulkWriteOptions options = new BulkWriteOptions();
		switch (bulkMode) {
			case ORDERED:
				return options.ordered(true);
			case UNORDERED:
				return options.ordered(false);
		}
		throw new IllegalStateException("BulkMode was null!");
	}
}
