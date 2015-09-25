/*
 * Copyright 2015 the original author or authors.
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

import java.util.List;

import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.util.Tuple;
import org.springframework.util.Assert;

import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

/**
 * Default implementation for {@link BulkOperations}.
 * 
 * @author Tobias Trelle
 */
public class DefaultBulkOperations implements BulkOperations {

	private final MongoOperations mongoOperations;
	private final BulkMode bulkMode;
	private final String collectionName;
	private final WriteConcern writeConcernDefault;
	private BulkWriteOperation bulk;

	/**
	 * Creates a new {@link DefaultBulkOperations}.
	 * 
	 * @param mongoOperations The underlying Mongo operations.
	 * @param bulkMode The bulk mode (ordered or unordered).
	 * @param collectionName Name of the collection to work on.
	 * @param writeConcernDefault The default write concern for all executions.
	 */
	public DefaultBulkOperations(MongoOperations mongoOperations, BulkMode bulkMode, String collectionName,
			WriteConcern writeConcernDefault) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null!");
		Assert.notNull(collectionName, "Collection name can not be null!");

		this.mongoOperations = mongoOperations;
		this.bulkMode = bulkMode;
		this.collectionName = collectionName;
		this.writeConcernDefault = writeConcernDefault;
		initBulkOp();
	}

	@Override
	public BulkOperations insert(Object document) {
		bulk.insert((DBObject) mongoOperations.getConverter().convertToMongoType(document));

		return this;
	}

	@Override
	public BulkOperations insert(List<? extends Object> documents) {
		for (Object document : documents) {
			insert(document);
		}

		return this;
	}

	@Override
	public BulkOperations updateOne(Query query, Update update) {
		return update(query, update, false, false);
	}

	@Override
	public BulkOperations updateOne(List<Tuple<Query, Update>> updates) {
		for (Tuple<Query, Update> update : updates) {
			update(update.getFirst(), update.getSecond(), false, false);
		}
		return this;
	}

	@Override
	public BulkOperations updateMulti(Query query, Update update) {
		return update(query, update, false, true);
	}

	@Override
	public BulkOperations updateMulti(List<Tuple<Query, Update>> updates) {
		for (Tuple<Query, Update> update : updates) {
			update(update.getFirst(), update.getSecond(), false, true);
		}
		return this;
	}

	@Override
	public BulkOperations upsert(Query query, Update update) {
		return update(query, update, true, true);
	}

	@Override
	public BulkOperations upsert(List<Tuple<Query, Update>> updates) {
		for (Tuple<Query, Update> update : updates) {
			upsert(update.getFirst(), update.getSecond());
		}
		return this;
	}

	/**
	 * Performs update and upsert bulk operations.
	 * 
	 * @param query Criteria to match documents.
	 * @param update Update to perform.
	 * @param upsert Upsert flag.
	 * @param multi Multi update flag.
	 * @param writeConcern The write concern to use.
	 * 
	 * @return Self reference.
	 * 
	 * @throws BulkOperationException if an error occured during bulk processing.
	 */
	protected BulkOperations update(Query query, Update pdate, boolean upsert, boolean multi) {
		if (upsert) {
			if (multi) {
				bulk.find(query.getQueryObject()).upsert().update(pdate.getUpdateObject());
			} else {
				bulk.find(query.getQueryObject()).upsert().updateOne(pdate.getUpdateObject());
			}
		} else {
			if (multi) {
				bulk.find(query.getQueryObject()).update(pdate.getUpdateObject());
			} else {
				bulk.find(query.getQueryObject()).updateOne(pdate.getUpdateObject());
			}
		}

		return this;
	}

	@Override
	public BulkOperations remove(Query remove) {
		bulk.find(remove.getQueryObject()).remove();

		return this;
	}

	@Override
	public BulkOperations remove(List<Query> removes) {
		for (Query query : removes) {
			remove(query);
		}
		return this;
	}

	@Override
	public BulkWriteResult executeBulk() {
		return executeBulk(writeConcernDefault);
	}

	@Override
	public BulkWriteResult executeBulk(WriteConcern writeConcern) {
		try {
			if (writeConcern != null) {
				return bulk.execute(writeConcern);
			} else {
				return bulk.execute();
			}
		} catch (BulkWriteException e) {
			throw new BulkOperationException("Bulk operation did not complete", e);
		} finally {
			// reset bulk for future use
			initBulkOp();
		}

	}

	private void initBulkOp() {
		this.bulk = createBulkOperation(bulkMode, mongoOperations.getCollection(collectionName));
	}

	private BulkWriteOperation createBulkOperation(BulkMode mode, DBCollection collection) {
		switch (mode) {
		case ORDERED:
			return collection.initializeOrderedBulkOperation();
		case UNORDERED:
			return collection.initializeUnorderedBulkOperation();
		default:
			return null;
		}
	}

}
