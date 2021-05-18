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

import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.DBRef;

/**
 * @author Christoph Strobl
 */
public interface ReferenceResolver {

	@Nullable
	Object resolveReference(MongoPersistentProperty property, Object source,
			ReferenceLookupDelegate referenceLookupDelegate, MongoEntityReader entityReader);

	ReferenceLoader getReferenceLoader();

	class ReferenceCollection {

		@Nullable
		private final String database;
		private final String collection;

		public ReferenceCollection(@Nullable String database, String collection) {

			Assert.hasText(collection, "Collection must not be empty or null");

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
	interface MongoEntityReader {
		Object read(Object source, TypeInformation<?> property);
	}
}
