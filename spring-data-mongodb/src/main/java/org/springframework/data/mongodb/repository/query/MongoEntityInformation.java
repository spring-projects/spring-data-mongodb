/*
 * Copyright 2011-2019 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.lang.Nullable;

/**
 * Mongo specific {@link EntityInformation}.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public interface MongoEntityInformation<T, ID> extends EntityInformation<T, ID> {

	/**
	 * Returns the name of the collection the entity shall be persisted to.
	 *
	 * @return
	 */
	String getCollectionName();

	/**
	 * Returns the attribute that the id will be persisted to.
	 *
	 * @return
	 */
	String getIdAttribute();

	/**
	 * Returns whether the entity uses optimistic locking.
	 *
	 * @return true if the entity defines a {@link org.springframework.data.annotation.Version} property.
	 * @since 2.2
	 */
	default boolean isVersioned() {
		return false;
	}

	/**
	 * Returns the version value for the entity or {@literal null} if the entity is not {@link #isVersioned() versioned}.
	 *
	 * @param entity must not be {@literal null}
	 * @return can be {@literal null}.
	 * @since 2.2
	 */
	@Nullable
	default Object getVersion(T entity) {
		return null;
	}

	/**
	 * Returns whether the entity defines a specific collation.
	 *
	 * @return {@literal true} if the entity defines a collation.
	 * @since 2.2
	 */
	default boolean hasCollation() {
		return getCollation() != null;
	}

	/**
	 * Return the collation for the entity or {@literal null} if {@link #hasCollation() not defined}.
	 * 
	 * @return can be {@literal null}.
	 * @since 2.2
	 */
	@Nullable
	Collation getCollation();
}
