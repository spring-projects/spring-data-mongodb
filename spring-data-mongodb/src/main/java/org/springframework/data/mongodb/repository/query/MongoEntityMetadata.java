/*
 * Copyright 2012-2024 the original author or authors.
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

import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.repository.core.EntityMetadata;

/**
 * Extension of {@link EntityMetadata} to additionally expose the collection name an entity shall be persisted to.
 *
 * @author Oliver Gierke
 */
public interface MongoEntityMetadata<T> extends EntityMetadata<T> {

	/**
	 * Returns the name of the collection the entity shall be persisted to.
	 *
	 * @return
	 */
	String getCollectionName();

	/**
	 * Returns the {@link MongoPersistentEntity} that supposed to determine the collection to be queried.
	 *
	 * @return
	 * @since 2.0.4
	 */
	MongoPersistentEntity<?> getCollectionEntity();
}
