/*
 * Copyright 2016-2024 the original author or authors.
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

import org.springframework.lang.Nullable;

/**
 * Provider interface to obtain {@link IndexOperations} by MongoDB collection name.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 2.0
 */
@FunctionalInterface
public interface IndexOperationsProvider {

	/**
	 * Returns the operations that can be performed on indexes.
	 *
	 * @param collectionName name of the MongoDB collection, must not be {@literal null}.
	 * @return index operations on the named collection
	 */
	default IndexOperations indexOps(String collectionName) {
		return indexOps(collectionName, null);
	}

	/**
	 * Returns the operations that can be performed on indexes.
	 *
	 * @param collectionName name of the MongoDB collection, must not be {@literal null}.
	 * @param type the type used for field mapping. Can be {@literal null}.
	 * @return index operations on the named collection
	 * @since 3.2
	 */
	IndexOperations indexOps(String collectionName, @Nullable Class<?> type);
}
