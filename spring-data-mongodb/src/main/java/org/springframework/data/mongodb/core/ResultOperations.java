/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import org.bson.Document;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.lang.Nullable;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * {@link ResultOperations} is intended to share logic depending on {@link DeleteResult} and
 * {@link com.mongodb.client.result.UpdateResult} between reactive and imperative implementations.
 *
 * @author Christoph Strobl
 * @since 2.2
 */
class ResultOperations {

	/**
	 * Decide if a given {@link DeleteResult} needs further inspection. Returns {@literal true} if the delete of a
	 * {@link org.springframework.data.annotation.Version versioned} entity did not remove any documents although the
	 * operation has been {@link DeleteResult#wasAcknowledged() acknowledged}.
	 *
	 * @param deleteResult must not be {@literal null}.
	 * @param query the actual query used for the delete operation.
	 * @param entity can be {@literal null}.
	 * @return {@literal true} if it cannot be decided if nothing got deleted because of a potential
	 *         {@link org.springframework.data.annotation.Version version} mismatch, or if the document did not exist in
	 *         first place.
	 */
	static boolean isUndecidedDeleteResult(DeleteResult deleteResult, org.bson.Document query,
			@Nullable MongoPersistentEntity<?> entity) {

		if (!deleteResult.wasAcknowledged() || deleteResult.getDeletedCount() > 0) {
			return false;
		}

		if (entity == null) {
			return false;
		}

		if (!entity.hasVersionProperty()) {
			return false;
		}

		return containsVersionProperty(query, entity);
	}

	static OptimisticLockingFailureException newDeleteVersionedOptimisticLockingException(Object id,
			String collectionName, Object expectedVersion, Object actualVersion) {

		throw new OptimisticLockingFailureException(
				String.format("The entity with id %s in %s has changed and cannot be deleted! " + System.lineSeparator() + //
						"Expected version %s but was %s.", id, collectionName, expectedVersion, actualVersion));
	}

	private static boolean containsVersionProperty(Document document, MongoPersistentEntity<?> entity) {
		return document.containsKey(entity.getRequiredVersionProperty().getFieldName());
	}
}
