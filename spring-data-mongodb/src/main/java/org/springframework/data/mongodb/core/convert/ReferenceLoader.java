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

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.ReferenceContext;
import org.springframework.lang.Nullable;

import com.mongodb.client.MongoCollection;

/**
 * @author Christoph Strobl
 */
public interface ReferenceLoader {

	@Nullable
	default Document fetch(ReferenceFilter filter, ReferenceContext context) {
		return bulkFetch(filter, context).findFirst().orElse(null);
	}

	Stream<Document> bulkFetch(ReferenceFilter filter, ReferenceContext context);

	interface ReferenceFilter {

		Bson getFilter();

		default Bson getSort() {
			return new Document();
		}

		default Stream<Document> apply(MongoCollection<Document> collection) {
			return restoreOrder(StreamSupport.stream(collection.find(getFilter()).sort(getSort()).spliterator(), false));
		}

		default Stream<Document> restoreOrder(Stream<Document> stream) {
			return stream;
		}

		static ReferenceFilter referenceFilter(Bson bson) {
			return () -> bson;
		}

		static ReferenceFilter singleReferenceFilter(Bson bson) {

			return new ReferenceFilter() {

				@Override
				public Bson getFilter() {
					return bson;
				}

				@Override
				public Stream<Document> apply(MongoCollection<Document> collection) {

					Document result = collection.find(getFilter()).sort(getSort()).limit(1).first();
					return result != null ? Stream.of(result) : Stream.empty();
				}
			};
		}
	}

}
