/*
 * Copyright 2012-2018 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.util.Assert;

/**
 * Bean based implementation of {@link MongoEntityMetadata}.
 *
 * @author Oliver Gierke
 */
class SimpleMongoEntityMetadata<T> implements MongoEntityMetadata<T> {

	private final Class<T> type;
	private final MongoPersistentEntity<?> collectionEntity;

	/**
	 * Creates a new {@link SimpleMongoEntityMetadata} using the given type and {@link MongoPersistentEntity} to use for
	 * collection lookups.
	 *
	 * @param type must not be {@literal null}.
	 * @param collectionEntity must not be {@literal null} or empty.
	 */
	public SimpleMongoEntityMetadata(Class<T> type, MongoPersistentEntity<?> collectionEntity) {

		Assert.notNull(type, "Type must not be null!");
		Assert.notNull(collectionEntity, "Collection entity must not be null or empty!");

		this.type = type;
		this.collectionEntity = collectionEntity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.EntityMetadata#getJavaType()
	 */
	public Class<T> getJavaType() {
		return type;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.MongoEntityMetadata#getCollectionName()
	 */
	public String getCollectionName() {
		return collectionEntity.getCollection();
	}
}
