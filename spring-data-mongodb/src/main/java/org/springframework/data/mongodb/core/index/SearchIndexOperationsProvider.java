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

/**
 * Provider interface to obtain {@link SearchIndexOperations} by MongoDB collection name or entity type.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 4.5
 */
public interface SearchIndexOperationsProvider {

	/**
	 * Returns the operations that can be performed on search indexes.
	 *
	 * @param collectionName name of the MongoDB collection, must not be {@literal null}.
	 * @return index operations on the named collection
	 */
	SearchIndexOperations searchIndexOps(String collectionName);

	/**
	 * Returns the operations that can be performed on search indexes.
	 *
	 * @param type the type used for field mapping.
	 * @return index operations on the named collection
	 */
	SearchIndexOperations searchIndexOps(Class<?> type);

	/**
	 * Returns the operations that can be performed on search indexes.
	 *
	 * @param collectionName name of the MongoDB collection, must not be {@literal null}.
	 * @param type the type used for field mapping. Can be {@literal null}.
	 * @return index operations on the named collection
	 */
	SearchIndexOperations searchIndexOps(Class<?> type, String collectionName);
}
