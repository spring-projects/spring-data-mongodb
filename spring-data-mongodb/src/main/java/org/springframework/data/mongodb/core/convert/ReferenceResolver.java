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

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.DBRef;

/**
 * The {@link ReferenceResolver} allows to load and convert linked entities.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
@FunctionalInterface
public interface ReferenceResolver {

	/**
	 * Resolve the association defined via the given property from a given source value. May return a
	 * {@link LazyLoadingProxy proxy instance} in case of a lazy loading association. The resolved value is assignable to
	 * {@link PersistentProperty#getType()}.
	 *
	 * @param property the association defining property.
	 * @param source the association source value.
	 * @param referenceLookupDelegate the lookup executing component.
	 * @param entityReader conversion function capable of constructing entities from raw source.
	 * @return can be {@literal null}.
	 */
	@Nullable
	Object resolveReference(MongoPersistentProperty property, Object source,
			ReferenceLookupDelegate referenceLookupDelegate, MongoEntityReader entityReader);

	/**
	 * {@link ReferenceCollection} is a value object that contains information about the target database and collection
	 * name of an association.
	 */
	class ReferenceCollection {

		@Nullable //
		private final String database;
		private final String collection;

		/**
		 * @param database can be {@literal null} to indicate the configured default
		 *          {@link MongoDatabaseFactory#getMongoDatabase() database} should be used.
		 * @param collection the target collection name. Must not be {@literal null}.
		 */
		public ReferenceCollection(@Nullable String database, String collection) {

			Assert.hasText(collection, "Collection must not be empty or null");

			this.database = database;
			this.collection = collection;
		}

		/**
		 * Create a new instance of {@link ReferenceCollection} from the given {@link DBRef}.
		 *
		 * @param dbRef must not be {@literal null}.
		 * @return new instance of {@link ReferenceCollection}.
		 */
		public static ReferenceCollection fromDBRef(DBRef dbRef) {
			return new ReferenceCollection(dbRef.getDatabaseName(), dbRef.getCollectionName());
		}

		/**
		 * Get the target collection name.
		 *
		 * @return never {@literal null}.
		 */
		public String getCollection() {
			return collection;
		}

		/**
		 * Get the target database name. If {@literal null} the default database should be used.
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		public String getDatabase() {
			return database;
		}
	}

	/**
	 * Domain type conversion callback interface that allows to read the {@code source} object into a mapped object.
	 */
	@FunctionalInterface
	interface MongoEntityReader {

		/**
		 * Read values from the given source into an object defined via the given {@link TypeInformation}.
		 *
		 * @param source never {@literal null}.
		 * @param typeInformation information about the desired target type.
		 * @return never {@literal null}.
		 */
		Object read(Object source, TypeInformation<?> typeInformation);
	}
}
