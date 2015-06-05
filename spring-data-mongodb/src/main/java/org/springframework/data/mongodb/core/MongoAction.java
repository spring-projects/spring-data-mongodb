/*
 * Copyright 2011-2012 the original author or authors.
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

import org.springframework.util.Assert;

import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

/**
 * Represents an action taken against the collection. Used by {@link WriteConcernResolver} to determine a custom
 * {@link WriteConcern} based on this information.
 * <ul>
 * <li>INSERT, SAVE have null query</li>
 * <li>REMOVE has null document</li>
 * <li>INSERT_LIST has null entityType, document, and query</li>
 * </ul>
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class MongoAction {

	private final String collectionName;
	private final WriteConcern defaultWriteConcern;
	private final Class<?> entityType;
	private final MongoActionOperation mongoActionOperation;
	private final DBObject query;
	private final DBObject document;

	/**
	 * Create an instance of a {@link MongoAction}.
	 *
	 * @param defaultWriteConcern the default write concern.
	 * @param mongoActionOperation action being taken against the collection
	 * @param collectionName the collection name, must not be {@literal null} or empty.
	 * @param entityType the POJO that is being operated against
	 * @param document the converted DBObject from the POJO or Spring Update object
	 * @param query the converted DBObject from the Spring Query object
	 */
	public MongoAction(WriteConcern defaultWriteConcern, MongoActionOperation mongoActionOperation,
			String collectionName, Class<?> entityType, DBObject document, DBObject query) {

		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		this.defaultWriteConcern = defaultWriteConcern;
		this.mongoActionOperation = mongoActionOperation;
		this.collectionName = collectionName;
		this.entityType = entityType;
		this.query = query;
		this.document = document;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public WriteConcern getDefaultWriteConcern() {
		return defaultWriteConcern;
	}

	/**
	 * @deprecated use {@link #getEntityType()} instead.
	 */
	@Deprecated
	public Class<?> getEntityClass() {
		return entityType;
	}

	public Class<?> getEntityType() {
		return entityType;
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
