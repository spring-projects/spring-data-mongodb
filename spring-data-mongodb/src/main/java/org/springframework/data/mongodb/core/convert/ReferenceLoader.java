/*
 * Copyright 2021 the original author or authors.
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
 * @author Christoph Strobl
 */
public interface ReferenceLoader {

	@Nullable
	default Document fetchOne(DocumentReferenceQuery filter, ReferenceCollection context) {

		Iterator<Document> it = fetchMany(filter, context).iterator();
		return it.hasNext() ? it.next() : null;
	}

	Iterable<Document> fetchMany(DocumentReferenceQuery filter, ReferenceCollection context);

	interface DocumentReferenceQuery {

		Bson getFilter();

		default Bson getSort() {
			return new Document();
		}

		// TODO: Move apply method into something else that holds the collection and knows about single item/multi-item
		// processing
		default Iterable<Document> apply(MongoCollection<Document> collection) {
			return restoreOrder(collection.find(getFilter()).sort(getSort()));
		}

		default Iterable<Document> restoreOrder(Iterable<Document> documents) {
			return documents;
		}

		static DocumentReferenceQuery forSingleDocument(Bson bson) {

			return new DocumentReferenceQuery() {

				@Override
				public Bson getFilter() {
					return bson;
				}

				@Override
				public Iterable<Document> apply(MongoCollection<Document> collection) {

					Document result = collection.find(getFilter()).sort(getSort()).limit(1).first();
					return result != null ? Collections.singleton(result) : Collections.emptyList();
				}
			};
		}

		static DocumentReferenceQuery forManyDocuments(Bson bson) {

			return new DocumentReferenceQuery() {

				@Override
				public Bson getFilter() {
					return bson;
				}

				@Override
				public Iterable<Document> apply(MongoCollection<Document> collection) {
					return collection.find(getFilter()).sort(getSort());
				}
			};
		}
	}

}
