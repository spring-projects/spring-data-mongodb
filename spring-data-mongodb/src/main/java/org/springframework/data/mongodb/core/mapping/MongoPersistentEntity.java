/*
 * Copyright 2011-2014 the original author or authors.
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

}
