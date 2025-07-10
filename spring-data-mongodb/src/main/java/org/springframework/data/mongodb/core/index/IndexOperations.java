/*
 * Copyright 2011-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import java.util.List;

/**
 * Index operations on a collection.
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Jens Schauder
 */
public interface IndexOperations {

	/**
	 * Ensure that an index for the provided {@link IndexDefinition} exists for the collection indicated by the entity
	 * class. If not it will be created.
	 *
	 * @param indexDefinition must not be {@literal null}.
	 * @return the index name.
	 * @deprecated since 4.5, in favor of {@link #createIndex(IndexDefinition)}.
	 */
	@Deprecated(since = "4.5", forRemoval = true)
	default String ensureIndex(IndexDefinition indexDefinition) {
		return createIndex(indexDefinition);
	}

	/**
	 * Create the index for the provided {@link IndexDefinition} for the collection indicated by the entity class. If the
	 * index does not exist it will be created. Might error if the collection already defines an index with the same name
	 * but different settings.
	 *
	 * @param indexDefinition must not be {@literal null}.
	 * @return the index name.
	 * @since 4.5
	 */
	String createIndex(IndexDefinition indexDefinition);

	/**
	 * Alters the index with given {@literal name}.
	 *
	 * @param name name of index to change.
	 * @param options index options.
	 * @since 4.1
	 */
	void alterIndex(String name, IndexOptions options);

	/**
	 * Drops an index from this collection.
	 *
	 * @param name name of index to drop
	 */
	void dropIndex(String name);

	/**
	 * Drops all indices from this collection.
	 */
	void dropAllIndexes();

	/**
	 * Returns the index information on the collection.
	 *
	 * @return index information on the collection
	 */
	List<IndexInfo> getIndexInfo();
}
