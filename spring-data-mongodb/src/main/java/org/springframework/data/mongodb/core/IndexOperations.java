/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.List;

import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexInfo;


/**
 * Index operations on a collection.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public interface IndexOperations {

	/**
	 * Ensure that an index for the provided {@link IndexDefinition} exists for the collection indicated by the entity
	 * class. If not it will be created.
	 * 
	 * @param indexDefinition must not be {@literal null}.
	 */
	void ensureIndex(IndexDefinition indexDefinition);

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
	 * Clears all indices that have not yet been applied to this collection.
	 */
	void resetIndexCache();

	/**
	 * Returns the index information on the collection.
	 * 
	 * @return index information on the collection
	 */
	List<IndexInfo> getIndexInfo();
}
