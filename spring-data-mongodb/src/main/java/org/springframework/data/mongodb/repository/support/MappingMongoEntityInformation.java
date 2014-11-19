/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.support;

import java.io.Serializable;

import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.repository.core.support.PersistentEntityInformation;

/**
 * {@link MongoEntityInformation} implementation using a {@link MongoPersistentEntity} instance to lookup the necessary
 * information. Can be configured with a custom collection to be returned which will trump the one returned by the
 * {@link MongoPersistentEntity} if given.
 * 
 * @author Oliver Gierke
 */
public class MappingMongoEntityInformation<T, ID extends Serializable> extends PersistentEntityInformation<T, ID>
		implements MongoEntityInformation<T, ID> {

	private final MongoPersistentEntity<T> entityMetadata;
	private final String customCollectionName;

	/**
	 * Creates a new {@link MappingMongoEntityInformation} for the given {@link MongoPersistentEntity}.
	 * 
	 * @param entity must not be {@literal null}.
	 */
	public MappingMongoEntityInformation(MongoPersistentEntity<T> entity) {
		this(entity, null);
	}

	/**
	 * Creates a new {@link MappingMongoEntityInformation} for the given {@link MongoPersistentEntity} and custom
	 * collection name.
	 * 
	 * @param entity must not be {@literal null}.
	 * @param customCollectionName can be {@literal null}.
	 */
	public MappingMongoEntityInformation(MongoPersistentEntity<T> entity, String customCollectionName) {

		super(entity);

		this.entityMetadata = entity;
		this.customCollectionName = customCollectionName;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoEntityInformation#getCollectionName()
	 */
	public String getCollectionName() {
		return customCollectionName == null ? entityMetadata.getCollection() : customCollectionName;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoEntityInformation#getIdAttribute()
	 */
	public String getIdAttribute() {
		return entityMetadata.getIdProperty().getName();
	}
}
