/*
 * Copyright 2025 the original author or authors.
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

import org.springframework.dao.DataAccessException;

/**
 * Search Index operations on a collection for Atlas Search.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 4.5
 * @see VectorIndex
 */
public interface SearchIndexOperations {

	/**
	 * Create the index for the given {@link SearchIndexDefinition} in the collection indicated by the entity class.
	 *
	 * @param indexDefinition must not be {@literal null}.
	 * @return the index name.
	 */
	// TODO: keep or just go with createIndex?
	default String ensureIndex(SearchIndexDefinition indexDefinition) {
		return createIndex(indexDefinition);
	}

	/**
	 * Create the index for the given {@link SearchIndexDefinition} in the collection indicated by the entity class.
	 *
	 * @param indexDefinition must not be {@literal null}.
	 * @return the index name.
	 */
	String createIndex(SearchIndexDefinition indexDefinition);

	/**
	 * Alters the search index matching the index {@link SearchIndexDefinition#getName() name}.
	 * <p>
	 * Atlas Search might not support updating indices which raises a {@link DataAccessException}.
	 *
	 * @param indexDefinition the index definition.
	 */
	// TODO: keep or remove since it does not work reliably?
	void updateIndex(SearchIndexDefinition indexDefinition);

	/**
	 * Check whether an index with the given {@code indexName} exists for the collection indicated by the entity class. To
	 * ensure an existing index is queryable it is recommended to check its {@link #status(String) status}.
	 *
	 * @param indexName name of index to check for presence.
	 * @return {@literal true} if the index exists; {@literal false} otherwise.
	 */
	boolean exists(String indexName);

	/**
	 * Check the actual {@link SearchIndexStatus status} of an index.
	 *
	 * @param indexName name of index to get the status for.
	 * @return the current status of the index or {@link SearchIndexStatus#DOES_NOT_EXIST} if the index cannot be found.
	 */
	SearchIndexStatus status(String indexName);

	/**
	 * Drops an index from the collection indicated by the entity class.
	 *
	 * @param indexName name of index to drop.
	 */
	void dropIndex(String indexName);

	/**
	 * Drops all search indices from the collection indicated by the entity class.
	 */
	void dropAllIndexes();
}
