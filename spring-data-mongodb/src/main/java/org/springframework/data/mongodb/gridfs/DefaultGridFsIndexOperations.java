/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.gridfs;

import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.util.Assert;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

/**
 * Default implementation of {@link GridFsIndexOperations}.
 * 
 * @author Aparna Chaudhary
 */
public class DefaultGridFsIndexOperations implements GridFsIndexOperations {

	private final GridFsOperations gridFsOperations;

	/**
	 * Creates a new {@link DefaultGridFsIndexOperations}.
	 * 
	 * @param gridFsOperations must not be {@literal null}.
	 */
	public DefaultGridFsIndexOperations(GridFsOperations gridFsOperations) {
		Assert.notNull(gridFsOperations, "GridFsOperations must not be null!");
		this.gridFsOperations = gridFsOperations;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsIndexOperations#ensureIndex(org.springframework.data.mongodb.core.index.IndexDefinition)
	 */
	@Override
	public void ensureIndex(final IndexDefinition indexDefinition) {
		gridFsOperations.execute(new CollectionCallback<Object>() {
			@Override
			public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				DBObject indexOptions = indexDefinition.getIndexOptions();
				if (indexOptions != null) {
					collection.ensureIndex(indexDefinition.getIndexKeys(), indexOptions);
				} else {
					collection.ensureIndex(indexDefinition.getIndexKeys());
				}
				return null;
			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsIndexOperations#dropIndex(java.lang.String)
	 */
	@Override
	public void dropIndex(final String name) {
		gridFsOperations.execute(new CollectionCallback<Void>() {
			@Override
			public Void doInCollection(DBCollection collection) throws MongoException, DataAccessException {
				collection.dropIndex(name);
				return null;
			}
		});
	}

}
