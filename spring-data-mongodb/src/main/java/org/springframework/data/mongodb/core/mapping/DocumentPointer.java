/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

/**
 * A custom pointer to a linked document to be used along with {@link DocumentReference} for storing the linkage value.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
@FunctionalInterface
public interface DocumentPointer<T> {

	/**
	 * The actual pointer value. This can be any simple type, like a {@link String} or {@link org.bson.types.ObjectId} or
	 * a {@link org.bson.Document} holding more information like the target collection, multiple fields forming the key,
	 * etc.
	 *
	 * @return the value stored in MongoDB and used for constructing the {@link DocumentReference#lookup() lookup query}.
	 */
	T getPointer();
}
