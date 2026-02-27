/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.mapping;

import org.springframework.util.Assert;

/**
 * Abstraction to determine the collection name for a given entity class. The collection name can be either derived from
 * an entity or can be a simple string.
 *
 * @author Mark Paluch
 * @since 5.1
 */
public interface CollectionName {

	/**
	 * Returns the collection name.
	 */
	String getCollectionName();

	/**
	 * Returns the entity class for which the collection name is derived.
	 */
	Class<?> getEntityClass();

	/**
	 * Create a new {@link CollectionName} for the given {@code collectionName}.
	 */
	static CollectionName just(String collectionName) {

		Assert.hasText(collectionName, "Collection name must not be null or empty");
		return new CollectionNames.StaticCollectionName(collectionName);
	}

	/**
	 * Create a new {@link CollectionName} that shall be derived from the given {@code entityClass}.
	 */
	static CollectionName from(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity class for collection name derivation must not be null or empty");
		return new CollectionNames.DerivedCollectionName(entityClass);
	}

}
