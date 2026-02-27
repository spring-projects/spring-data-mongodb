/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb.core.bulk;

import org.jspecify.annotations.Nullable;

import org.springframework.data.mongodb.core.mapping.CollectionName;
import org.springframework.util.Assert;

/**
 * Context for a {@link BulkOperation}, providing the target namespace (database and collection) and optional domain
 * type used for mapping.
 *
 * @author Christoph Strobl
 * @since 5.1
 */
public interface BulkOperationContext {

	/**
	 * Returns the target namespace for this bulk operation.
	 *
	 * @return the {@link TypedNamespace}; never {@literal null}.
	 */
	TypedNamespace namespace();

	/**
	 * Value object holding namespace (database and collection) information and an optional domain type used for mapping
	 * {@link BulkOperation bulk operations}.
	 * <p>
	 * <strong>NOTE:</strong> Provide at least either {@literal type} or {@literal collection}. An explicit
	 * {@literal collection} name takes precedence over the collection name derived from {@literal type}.
	 *
	 * @param type target domain type for mapping queries and updates; used to derive collection name when
	 *          {@literal collection} is {@literal null}; may be {@literal null}.
	 * @param database target database; use {@literal null} for the configured default database.
	 * @param collectionName target collection name; may be {@literal null}.
	 */
	record TypedNamespace(@Nullable Class<?> type, @Nullable String database, @Nullable CollectionName collectionName) {

		public boolean hasCollectionName() {
			return collectionName != null;
		}

		public CollectionName getRequiredCollectionName() {
			Assert.state(hasCollectionName(), "Collection name is required but not present");
			return collectionName;
		}

		public Class<?> getDomainType() {

			if (type != null) {
				return type;
			}

			if (collectionName != null && collectionName.getEntityClass() != Object.class) {
				return collectionName.getEntityClass();
			}

			return Object.class;
		}
	}
}
