/*
 * Copyright 2012 the original author or authors.
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
    // One of both won't be null
	private final String collectionName;
    private final MongoPersistentEntity<T> persistentEntityRef;

	/**
	 * Creates a new {@link SimpleMongoEntityMetadata} using the given type and collection name.
	 * 
	 * @param type must not be {@literal null}.
	 * @param collectionName must not be {@literal null} or empty.
	 */
    @Deprecated
	public SimpleMongoEntityMetadata(Class<T> type, String collectionName) {

		Assert.notNull(type, "Type must not be null!");
		Assert.hasText(collectionName, "Collection name must not be null or empty!");

		this.type = type;
		this.collectionName = collectionName;
        this.persistentEntityRef = null;
	}
    
    /**
     * <p>
     * NEW way of getting collectionName, due to improvements in performance done in SpEL
     * it seems to me a plausible solution to DATAMONGO-1043
     * http://docs.spring.io/spring-framework/docs/current/spring-framework-reference/html/expressions.html#expressions-compiler-configuration
	 * </p>
     * Creates a new {@link SimpleMongoEntityMetadata} using the given type and collection persistent entity 
     * reference in order to retrieve the collection name at runtime(this allows us to
     * customize at runtime the collection based on a thread local variable for instance)
	 * 
	 * @param type must not be {@literal null}.
	 * @param persistentEntityRef must not be {@literal null}
	 */
    public SimpleMongoEntityMetadata(Class<T> type, MongoPersistentEntity<T> persistentEntityRef) {
        Assert.notNull(type, "Type must not be null!");
        Assert.notNull(persistentEntityRef, "PersistentEntityRef must not be null!");

        this.type = type;
		this.collectionName = null;
        this.persistentEntityRef = persistentEntityRef;
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
		return this.persistentEntityRef != null ? this.persistentEntityRef.getCollection() : 
                                                  collectionName;
	}
}
