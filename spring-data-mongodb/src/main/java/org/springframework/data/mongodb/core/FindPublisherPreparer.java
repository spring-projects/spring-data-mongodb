/*
 * Copyright 2016-2023 the original author or authors.
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
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * Simple callback interface to allow customization of a {@link FindPublisher}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Konstantin Volivach
 */
public interface FindPublisherPreparer extends ReadPreferenceAware {

	/**
	 * Default {@link FindPublisherPreparer} just passing on the given {@link FindPublisher}.
	 *
	 * @since 2.2
	 */
	FindPublisherPreparer NO_OP_PREPARER = (findPublisher -> findPublisher);

	/**
	 * Prepare the given cursor (apply limits, skips and so on). Returns the prepared cursor.
	 *
	 * @param findPublisher must not be {@literal null}.
	 */
	FindPublisher<Document> prepare(FindPublisher<Document> findPublisher);

	/**
	 * Apply query specific settings to {@link MongoCollection} and initiate a find operation returning a
	 * {@link FindPublisher} via the given {@link Function find} function.
	 *
	 * @param collection must not be {@literal null}.
	 * @param find must not be {@literal null}.
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException if one of the required arguments is {@literal null}.
	 * @since 2.2
	 */
	default FindPublisher<Document> initiateFind(MongoCollection<Document> collection,
			Function<MongoCollection<Document>, FindPublisher<Document>> find) {

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
