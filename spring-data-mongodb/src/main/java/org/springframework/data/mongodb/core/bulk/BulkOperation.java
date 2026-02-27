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

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;

/**
 * A single operation (insert, update, replace, or remove) within a {@link Bulk}. Each operation has a
 * {@link #context()} that provides the target namespace (database and collection).
 *
 * @author Christoph Strobl
 * @since 5.1
 */
public interface BulkOperation {

	/**
	 * Returns the context for this operation.
	 *
	 * @return the {@link BulkOperationContext}.
	 */
	BulkOperationContext context();

	/**
	 * Insert operation: insert a single document.
	 */
	interface Insert extends BulkOperation {

		/**
		 * Returns the document to insert.
		 *
		 * @return the document.
		 */
		Object value();

	}

	/**
	 * Update operation: update documents matching the {@link #query()}.
	 */
	interface Update extends BulkOperation {

		/**
		 * Returns the update definition to apply.
		 *
		 * @return the update.
		 */
		UpdateDefinition update();

		/**
		 * Returns the query that selects which documents to update.
		 *
		 * @return the query.
		 */
		Query query();

		/**
		 * Returns whether to perform an upsert if no document matches.
		 *
		 * @return {@literal true} for upsert.
		 */
		boolean upsert();

	}

	/**
	 * Update-one operation: update the first document matching the {@link #query()}.
	 */
	interface UpdateFirst extends Update {}

	/**
	 * Remove operation: delete documents matching the {@link #query()}.
	 */
	interface Remove extends BulkOperation {

		/**
		 * Returns the query that selects which documents to remove.
		 *
		 * @return the query.
		 */
		Query query();

	}

	/**
	 * Remove-one operation: delete the first document matching the {@link #query()}.
	 */
	interface RemoveFirst extends Remove {}

	/**
	 * Replace operation: replace the document matching the {@link #query()} with the {@link #replacement()} document.
	 */
	interface Replace extends BulkOperation {

		/**
		 * Returns the query that selects the document to replace.
		 *
		 * @return the query.
		 */
		Query query();

		/**
		 * Returns the replacement document.
		 *
		 * @return the replacement.
		 */
		Object replacement();

		/**
		 * Returns whether to perform an upsert if no document matches.
		 *
		 * @return {@literal true} for upsert.
		 */
		boolean upsert();

	}

}
