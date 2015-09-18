/*
 * Copyright 2011-2015 the original author or authors.
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

import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.util.Tuple;
import org.springframework.util.Assert;

import com.mongodb.BulkUpdateRequestBuilder;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteRequestBuilder;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

/**
 * Default implementation for {@link BulkOperations}.
 * 
 * @author Tobias Trelle
 */
public class DefaultBulkOperations implements BulkOperations {

	private final MongoOperations mongoOperations;
	private final String collectionName;
	private final WriteConcern writeConcern;

	/**
	 * Creates a new {@link DefaultBulkOperations}.
	 */
	public DefaultBulkOperations(MongoOperations mongoOperations, String collectionName, WriteConcern writeConcernDefault) {

		Assert.notNull(mongoOperations, "MongoOperations must not be null!");
		Assert.notNull(collectionName, "Collection name can not be null!");

		this.mongoOperations = mongoOperations;
		this.collectionName = collectionName;
		this.writeConcern = writeConcernDefault;
	}

	@Override
	public int insert(final BulkMode mode, final List<? extends Object> documents) {
		return mongoOperations.execute(collectionName, new CollectionCallback<Integer>() {
			public Integer doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				final BulkWriteOperation bulk = getBulkOperation(mode, collection);
				final MongoConverter converter = mongoOperations.getConverter();

				for (Object document : documents) {
					bulk.insert((DBObject) converter.convertToMongoType(document));
				}

				return executeBulk(bulk).getInsertedCount();
			}
		});

	}

	@Override
	public int updateOne(BulkMode mode, List<Tuple<Query, Update>> updates) {
		return update(mode, updates, false, false).getModifiedCount();
	}

	@Override
	public int updateMulti(BulkMode mode, List<Tuple<Query, Update>> updates) {
		return update(mode, updates, false, true).getModifiedCount();
	}

	@Override
	public BulkWriteResult upsert(final BulkMode mode, final List<Tuple<Query, Update>> updates) {
		return update(mode, updates, true, false);
	}

	@Override
	public int remove(BulkMode mode, List<Query> removes) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	protected BulkWriteResult update(final BulkMode mode, final List<Tuple<Query, Update>> updates, final boolean upsert,
			final boolean multi) {
		return mongoOperations.execute(collectionName, new CollectionCallback<BulkWriteResult>() {
			public BulkWriteResult doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				final BulkWriteOperation bulk = getBulkOperation(mode, collection);

				for (Tuple<Query, Update> update : updates) {
					Query q = update.getFirst();
					Update u = update.getSecond();

					if (upsert) {
						if (multi) {
							bulk.find( q.getQueryObject() ).upsert().update( u.getUpdateObject() );
						} else {
							bulk.find( q.getQueryObject() ).upsert().updateOne( u.getUpdateObject() );
						}
					} else {
						if (multi) {
							bulk.find( q.getQueryObject() ).update( u.getUpdateObject() );
						} else {
							bulk.find( q.getQueryObject() ).updateOne( u.getUpdateObject() );
						}
					}
				}

				return executeBulk(bulk);
			}
		});
	}

	private BulkWriteResult executeBulk(BulkWriteOperation bulk) {
		try {
			if (writeConcern != null) {
				return bulk.execute(writeConcern);
			} else {
				return bulk.execute();
			}
		} catch (BulkWriteException e) {
			throw new BulkOperationException("Bulk operation did not complete", e);
		}

	}

	private BulkWriteOperation getBulkOperation(BulkMode mode, DBCollection collection) {
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
