/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.springframework.util.Assert;

import com.mongodb.client.MongoCollection;

/**
 * Interface for functional preparation of a {@link MongoCollection}.
 *
 * @author Mark Paluch
 * @since 4.1
 */
public interface CollectionPreparer<T> {

	/**
	 * Returns a preparer that always returns its input collection.
	 *
	 * @return a preparer that always returns its input collection.
	 */
	static <T> CollectionPreparer<T> identity() {
		return it -> it;
	}

	/**
	 * Prepare the {@code collection}.
	 *
	 * @param collection the collection to prepare.
	 * @return the prepared collection.
	 */
	T prepare(T collection);

	/**
	 * Returns a composed {@code CollectionPreparer} that first applies this preparer to the collection, and then applies
	 * the {@code after} preparer to the result. If evaluation of either function throws an exception, it is relayed to
	 * the caller of the composed function.
	 *
	 * @param after the collection preparer to apply after this function is applied.
	 * @return a composed {@code CollectionPreparer} that first applies this preparer and then applies the {@code after}
	 *         preparer.
	 */
	default CollectionPreparer<T> andThen(CollectionPreparer<T> after) {
		Assert.notNull(after, "After CollectionPreparer must not be null");
		return c -> after.prepare(prepare(c));
	}

}
