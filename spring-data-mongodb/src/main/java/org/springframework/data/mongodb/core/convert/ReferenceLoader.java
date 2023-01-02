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
package org.springframework.data.mongodb.core.convert;

import java.util.Collections;
import java.util.Iterator;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.ReferenceCollection;
import org.springframework.lang.Nullable;

import com.mongodb.client.MongoCollection;

/**
 * The {@link ReferenceLoader} obtains raw {@link Document documents} for linked entities via a
 * {@link ReferenceLoader.DocumentReferenceQuery}.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
public interface ReferenceLoader {

	/**
	 * Obtain a single {@link Document} matching the given {@literal referenceQuery} in the {@literal context}.
	 *
	 * @param referenceQuery must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @return the matching {@link Document} or {@literal null} if none found.
	 */
	@Nullable
	default Document fetchOne(DocumentReferenceQuery referenceQuery, ReferenceCollection context) {

		Iterator<Document> it = fetchMany(referenceQuery, context).iterator();
		return it.hasNext() ? it.next() : null;
	}

	/**
	 * Obtain multiple {@link Document} matching the given {@literal referenceQuery} in the {@literal context}.
	 *
	 * @param referenceQuery must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @return the matching {@link Document} or {@literal null} if none found.
	 */
	Iterable<Document> fetchMany(DocumentReferenceQuery referenceQuery, ReferenceCollection context);

	/**
	 * The {@link DocumentReferenceQuery} defines the criteria by which {@link Document documents} should be matched
	 * applying potentially given order criteria.
	 */
	interface DocumentReferenceQuery {

		/**
		 * Get the query to obtain matching {@link Document documents}.
		 *
		 * @return never {@literal null}.
		 */
		Bson getQuery();

		/**
		 * Get the sort criteria for ordering results.
		 *
		 * @return an empty {@link Document} by default. Never {@literal null}.
		 */
		default Bson getSort() {
			return new Document();
		}

		default Iterable<Document> apply(MongoCollection<Document> collection) {
			return restoreOrder(collection.find(getQuery()).sort(getSort()));
		}

		/**
		 * Restore the order of fetched documents.
		 *
		 * @param documents must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		default Iterable<Document> restoreOrder(Iterable<Document> documents) {
			return documents;
		}

		static DocumentReferenceQuery forSingleDocument(Bson bson) {

			return new DocumentReferenceQuery() {

				@Override
				public Bson getQuery() {
					return bson;
				}

				@Override
				public Iterable<Document> apply(MongoCollection<Document> collection) {

					Document result = collection.find(getQuery()).sort(getSort()).limit(1).first();
					return result != null ? Collections.singleton(result) : Collections.emptyList();
				}
			};
		}

		static DocumentReferenceQuery forManyDocuments(Bson bson) {

			return new DocumentReferenceQuery() {

				@Override
				public Bson getQuery() {
					return bson;
				}

				@Override
				public Iterable<Document> apply(MongoCollection<Document> collection) {
					return collection.find(getQuery()).sort(getSort());
				}
			};
		}
	}
}
