/*
 * Copyright 2011-2023 the original author or authors.
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

import java.util.Collection;

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.MutablePersistentEntity;
import org.springframework.lang.Nullable;

/**
 * MongoDB specific {@link PersistentEntity} abstraction.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public interface MongoPersistentEntity<T> extends MutablePersistentEntity<T, MongoPersistentProperty> {

	/**
	 * Returns the collection the entity shall be persisted to.
	 *
	 * @return
	 */
	String getCollection();

	/**
	 * Returns the default language to be used for this entity.
	 *
	 * @return
	 * @since 1.6
	 */
	String getLanguage();

	/**
	 * Returns the property holding text score value.
	 *
	 * @return {@literal null} if not present.
	 * @see #hasTextScoreProperty()
	 * @since 1.6
	 */
	@Nullable
	MongoPersistentProperty getTextScoreProperty();

	/**
	 * Returns whether the entity has a {@link TextScore} property.
	 *
	 * @return true if property annotated with {@link TextScore} is present.
	 * @since 1.6
	 */
	boolean hasTextScoreProperty();

	/**
	 * Returns the collation of the entity evaluating a potential SpEL expression within the current context.
	 *
	 * @return {@literal null} if not set.
	 * @since 2.2
	 */
	@Nullable
	org.springframework.data.mongodb.core.query.Collation getCollation();

	/**
	 * @return {@literal true} if the entity is annotated with
	 *         {@link org.springframework.data.mongodb.core.query.Collation}.
	 * @since 2.2
	 */
	default boolean hasCollation() {
		return getCollation() != null;
	}

	/**
	 * Get the entities shard key if defined.
	 *
	 * @return {@link ShardKey#none()} if not not set.
	 * @since 3.0
	 */
	ShardKey getShardKey();

	/**
	 * @return {@literal true} if the {@link #getShardKey() shard key} is sharded.
	 * @since 3.0
	 */
	default boolean isSharded() {
		return getShardKey().isSharded();
	}

	/**
	 * @return {@literal true} if the entity should be unwrapped.
	 * @since 3.2
	 */
	default boolean isUnwrapped() {
		return false;
	}

	/**
	 * @return the resolved encryption keyIds if applicable. An empty {@link Collection} if no keyIds specified.
	 *         {@literal null} no {@link Encrypted} annotation found.
	 * @since 3.3
	 */
	@Nullable
	Collection<Object> getEncryptionKeyIds();
}
