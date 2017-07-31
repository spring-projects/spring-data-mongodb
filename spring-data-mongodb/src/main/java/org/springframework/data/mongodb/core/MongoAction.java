/*
 * Copyright 2011-2017 the original author or authors.
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

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

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
 * @author Christoph Strobl
 */
public class MongoAction {

	private final String collectionName;
	private final MongoActionOperation mongoActionOperation;

	private final @Nullable WriteConcern defaultWriteConcern;
	private final @Nullable Class<?> entityType;
	private final @Nullable Document query;
	private final @Nullable Document document;

	/**
	 * Create an instance of a {@link MongoAction}.
	 * 
	 * @param defaultWriteConcern the default write concern. Can be {@literal null}.
	 * @param mongoActionOperation action being taken against the collection. Must not be {@literal null}.
	 * @param collectionName the collection name, must not be {@literal null} or empty.
	 * @param entityType the POJO that is being operated against. Can be {@literal null}.
	 * @param document the converted Document from the POJO or Spring Update object. Can be {@literal null}.
	 * @param query the converted Document from the Spring Query object. Can be {@literal null}.
	 */
	public MongoAction(@Nullable WriteConcern defaultWriteConcern, MongoActionOperation mongoActionOperation,
			String collectionName, @Nullable Class<?> entityType, @Nullable Document document, @Nullable Document query) {

		Assert.hasText(collectionName, "Collection name must not be null or empty!");
		Assert.notNull(mongoActionOperation, "MongoActionOperation must not be null!");

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

	@Nullable
	public WriteConcern getDefaultWriteConcern() {
		return defaultWriteConcern;
	}

	@Nullable
	public Class<?> getEntityType() {
		return entityType;
	}

	@Nullable
	public MongoActionOperation getMongoActionOperation() {
		return mongoActionOperation;
	}

	@Nullable
	public Document getQuery() {
		return query;
	}

	@Nullable
	public Document getDocument() {
		return document;
	}

}
