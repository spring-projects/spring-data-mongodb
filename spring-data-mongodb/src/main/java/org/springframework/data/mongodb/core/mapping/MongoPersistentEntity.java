/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.lang.Nullable;

/**
 * MongoDB specific {@link PersistentEntity} abstraction.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public interface MongoPersistentEntity<T> extends PersistentEntity<T, MongoPersistentProperty> {

	/**
	 * Returns the collection the entity shall be persisted to.
	 *
	 * @return
	 */
	String getCollection();

	/**
	 * Returns the default language to be used for this entity.
	 *
	 * @since 1.6
	 * @return
	 */
	String getLanguage();

	/**
	 * Returns the property holding text score value.
	 *
	 * @since 1.6
	 * @see #hasTextScoreProperty()
	 * @return {@literal null} if not present.
	 */
	@Nullable
	MongoPersistentProperty getTextScoreProperty();

	/**
	 * Returns whether the entity has a {@link TextScore} property.
	 *
	 * @since 1.6
	 * @return true if property annotated with {@link TextScore} is present.
	 */
	boolean hasTextScoreProperty();

	/**
	 * Returns the entities {@literal id} type of {@literal null} if the entity has no {@literal id} property.
	 *
	 * @return {@literal null} if the entity does not have an {@link #hasIdProperty() id property}.
	 * @since 2.2
	 */
	@Nullable
	default Class<?> getIdType() {

		if (!hasIdProperty()) {
			return null;
		}

		return getIdProperty().getIdType();
	}

}
