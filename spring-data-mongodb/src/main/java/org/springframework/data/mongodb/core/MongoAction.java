/*
 * Copyright 2011 the original author or authors.
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

import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

/**
 * Represents an action taken against the collection. Used by {@link WriteConcernResolver} to determine a custom
 * WriteConcern based on this information.
 * 
 * Properties that will always be not-null are collectionName and defaultWriteConcern. The EntityClass is null only for
 * the MongoActionOperaton.INSERT_LIST.
 * 
 * <ul>
 * <li>INSERT, SAVE have null query</li>
 * <li>REMOVE has null document</li>
 * <li>INSERT_LIST has null entityClass, document, and query</li>
 * </ul>
 * 
 * @author Mark Pollack
 * 
 */
public class MongoAction {

	private String collectionName;

	private WriteConcern defaultWriteConcern;

	private Class<?> entityClass;

	private MongoActionOperation mongoActionOperation;

	private DBObject query;

	private DBObject document;

	/**
	 * Create an instance of a MongoAction
	 * 
	 * @param defaultWriteConcern the default write concern
	 * @param mongoActionOperation action being taken against the collection
	 * @param collectionName the collection name
	 * @param entityClass the POJO that is being operated against
	 * @param document the converted DBObject from the POJO or Spring Update object
	 * @param query the converted DBOjbect from the Spring Query object
	 */
	public MongoAction(WriteConcern defaultWriteConcern, MongoActionOperation mongoActionOperation,
			String collectionName, Class<?> entityClass, DBObject document, DBObject query) {
		super();
		this.defaultWriteConcern = defaultWriteConcern;
		this.mongoActionOperation = mongoActionOperation;
		this.collectionName = collectionName;
		this.entityClass = entityClass;
		this.query = query;
		this.document = document;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public WriteConcern getDefaultWriteConcern() {
		return defaultWriteConcern;
	}

	public Class<?> getEntityClass() {
		return entityClass;
	}

	public MongoActionOperation getMongoActionOperation() {
		return mongoActionOperation;
	}

	public DBObject getQuery() {
		return query;
	}

	public DBObject getDocument() {
		return document;
	}

}
