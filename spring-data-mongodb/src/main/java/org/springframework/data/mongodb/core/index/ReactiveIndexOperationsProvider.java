/*
 * Copyright 2018 the original author or authors.
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
 * Provider interface to obtain {@link ReactiveIndexOperations} by MongoDB collection name.
 *
 * @author Mark Paluch
 * @since 2.1
 */
@FunctionalInterface
public interface ReactiveIndexOperationsProvider {

	/**
	 * Returns the operations that can be performed on indexes.
	 *
	 * @param collectionName name of the MongoDB collection, must not be {@literal null}.
	 * @return index operations on the named collection
	 */
	ReactiveIndexOperations indexOps(String collectionName);
}
