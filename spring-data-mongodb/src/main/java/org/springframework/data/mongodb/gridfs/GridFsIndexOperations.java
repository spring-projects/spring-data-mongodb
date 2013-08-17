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
package org.springframework.data.mongodb.gridfs;

import org.springframework.data.mongodb.core.index.IndexDefinition;

/**
 * Index operations for GridFs files collection.
 * 
 * @author Aparna Chaudhary
 */
public interface GridFsIndexOperations {

	/**
	 * Ensure that an index for the provided {@link IndexDefinition} exists for the files collection. If not it will be
	 * created.
	 * 
	 * @param indexDefinition must not be {@literal null}.
	 */
	void ensureIndex(IndexDefinition indexDefinition);

	/**
	 * Drops an index from the files collection.
	 * 
	 * @param name name of index to drop
	 */
	void dropIndex(String name);

}
