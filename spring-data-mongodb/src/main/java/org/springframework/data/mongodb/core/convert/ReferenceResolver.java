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

import org.bson.Document;
import org.springframework.data.mongodb.core.convert.ReferenceLoader.DocumentReferenceQuery;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

import com.mongodb.DBRef;

/**
 * @author Christoph Strobl
 */
public interface ReferenceResolver {

	@Nullable
	Object resolveReference(MongoPersistentProperty property, Object source, ReferenceReader referenceReader,
			LookupFunction lookupFunction, ResultConversionFunction resultConversionFunction);

	default Object resolveReference(MongoPersistentProperty property, Object source, ReferenceReader referenceReader,
			ResultConversionFunction resultConversionFunction) {

		return resolveReference(property, source, referenceReader, (filter, ctx) -> {
			if (property.isCollectionLike() || property.isMap()) {
				return getReferenceLoader().bulkFetch(filter, ctx);

			}

			Object target = getReferenceLoader().fetch(filter, ctx);
			return target == null ? Collections.emptyList() : Collections.singleton(getReferenceLoader().fetch(filter, ctx));
		}, resultConversionFunction);
	}

	ReferenceLoader getReferenceLoader();

	class ReferenceCollection {

		@Nullable
		private final String database;
		private final String collection;

		public ReferenceCollection(@Nullable String database, String collection) {

			this.database = database;
			this.collection = collection;
		}

		static ReferenceCollection fromDBRef(DBRef dbRef) {
			return new ReferenceCollection(dbRef.getDatabaseName(), dbRef.getCollectionName());
		}

		public String getCollection() {
			return collection;
		}

		@Nullable
		public String getDatabase() {
			return database;
		}
	}

	@FunctionalInterface
	interface LookupFunction {
		Iterable<Document> apply(DocumentReferenceQuery referenceQuery, ReferenceCollection referenceCollection);
	}

	@FunctionalInterface
	interface ResultConversionFunction {
		Object apply(Object source, TypeInformation property);
	}
}
