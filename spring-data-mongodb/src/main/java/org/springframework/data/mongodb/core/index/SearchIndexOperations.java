/*
 * Copyright 2024. the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import java.util.List;

/**
 * Search Index operations on a collection for Atlas Search.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 4.5
 */
public interface SearchIndexOperations {

	/**
	 * Ensure that an index for the provided {@link SearchIndexDefinition} exists for the collection indicated by the
	 * entity class. If not it will be created.
	 *
	 * @param indexDefinition must not be {@literal null}.
	 * @return the index name.
	 */
	String ensureIndex(SearchIndexDefinition indexDefinition);

	/**
	 * Alters the search {@code index}.
	 * <p>
	 * Note that Atlas Search does not support updating Vector Search Indices resulting in
	 * {@link UnsupportedOperationException}.
	 *
	 * @param index the index definition.
	 */
	void updateIndex(SearchIndexDefinition index);

	/**
	 * Check whether an index with the {@code name} exists.
	 *
	 * @param name name of index to check for presence.
	 * @return {@literal true} if the index exists; {@literal false} otherwise.
	 */
	boolean exists(String name);

	/**
	 * Drops an index from this collection.
	 *
	 * @param name name of index to drop.
	 */
	void dropIndex(String name);

	/**
	 * Drops all search indices from this collection.
	 */
	void dropAllIndexes();

	/**
	 * Returns the index information on the collection.
	 *
	 * @return index information on the collection
	 */
	List<IndexInfo> getIndexInfo();
}
