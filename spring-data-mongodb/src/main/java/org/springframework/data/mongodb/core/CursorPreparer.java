/*
 * Copyright 2002-2023 the original author or authors.
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

import java.util.function.Function;

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

/**
 * Simple callback interface to allow customization of a {@link FindIterable}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public interface CursorPreparer extends ReadPreferenceAware {

	/**
	 * Default {@link CursorPreparer} just passing on the given {@link FindIterable}.
	 *
	 * @since 2.2
	 */
	CursorPreparer NO_OP_PREPARER = (iterable -> iterable);

	/**
	 * Prepare the given cursor (apply limits, skips and so on). Returns the prepared cursor.
	 *
	 * @param iterable must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	FindIterable<Document> prepare(FindIterable<Document> iterable);

	/**
	 * Apply query specific settings to {@link MongoCollection} and initiate a find operation returning a
	 * {@link FindIterable} via the given {@link Function find} function.
	 *
	 * @param collection must not be {@literal null}.
	 * @param find must not be {@literal null}.
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException if one of the required arguments is {@literal null}.
	 * @since 2.2
	 */
	default FindIterable<Document> initiateFind(MongoCollection<Document> collection,
			Function<MongoCollection<Document>, FindIterable<Document>> find) {

		Assert.notNull(collection, "Collection must not be null");
		Assert.notNull(find, "Find function must not be null");

		if (hasReadPreference()) {
			collection = collection.withReadPreference(getReadPreference());
		}

		return prepare(find.apply(collection));
	}

	/**
	 * @return the {@link ReadPreference} to apply or {@literal null} if none defined.
	 * @since 2.2
	 */
	@Override
	@Nullable
	default ReadPreference getReadPreference() {
		return null;
	}
}
