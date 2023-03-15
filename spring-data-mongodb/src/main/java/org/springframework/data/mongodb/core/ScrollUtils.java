/*
 * Copyright 2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.bson.BsonNull;
import org.bson.Document;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Window;
import org.springframework.data.mongodb.core.EntityOperations.Entity;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Utilities to run scroll queries and create {@link Window} results.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 4.1
 */
class ScrollUtils {

	/**
	 * Create the actual query to run keyset-based pagination. Affects projection, sorting, and the criteria.
	 *
	 * @param query
	 * @param idPropertyName
	 * @return
	 */
	static KeySetScrollQuery createKeysetPaginationQuery(Query query, String idPropertyName) {

		Document sortObject = query.isSorted() ? query.getSortObject() : new Document();
		sortObject.put(idPropertyName, 1);

		// make sure we can extract the keyset
		Document fieldsObject = query.getFieldsObject();
		if (!fieldsObject.isEmpty()) {
			for (String field : sortObject.keySet()) {
				fieldsObject.put(field, 1);
			}
		}

		Document queryObject = query.getQueryObject();

		List<Document> or = (List<Document>) queryObject.getOrDefault("$or", new ArrayList<>());

		// TODO: reverse scrolling
		Map<String, Object> keysetValues = query.getKeyset().getKeys();
		Document keysetSort = new Document();
		List<String> sortKeys = new ArrayList<>(sortObject.keySet());

		if (!keysetValues.isEmpty() && !keysetValues.keySet().containsAll(sortKeys)) {
			throw new IllegalStateException("KeysetScrollPosition does not contain all keyset values");
		}

		// first query doesn't come with a keyset
		if (!keysetValues.isEmpty()) {

			// build matrix query for keyset paging that contains sort^2 queries
			// reflecting a query that follows sort order semantics starting from the last returned keyset
			for (int i = 0; i < sortKeys.size(); i++) {

				Document sortConstraint = new Document();

				for (int j = 0; j < sortKeys.size(); j++) {

					String sortSegment = sortKeys.get(j);
					int sortOrder = sortObject.getInteger(sortSegment);
					Object o = keysetValues.get(sortSegment);

					if (j >= i) { // tail segment
						if(o instanceof BsonNull) {
							throw new IllegalStateException("Cannot resume from KeysetScrollPosition. Offending key: '%s' is 'null'".formatted(sortSegment));
						}
						sortConstraint.put(sortSegment, new Document(sortOrder == 1 ? "$gt" : "$lt", o));
						break;
					}

					sortConstraint.put(sortSegment, o);
				}

				if (!sortConstraint.isEmpty()) {
					or.add(sortConstraint);
				}
			}
		}

		if (!keysetSort.isEmpty()) {
			or.add(keysetSort);
		}
		if (!or.isEmpty()) {
			queryObject.put("$or", or);
		}

		return new KeySetScrollQuery(queryObject, fieldsObject, sortObject);
	}

	static <T> Window<T> createWindow(Document sortObject, int limit, List<T> result, EntityOperations operations) {

		IntFunction<KeysetScrollPosition> positionFunction = value -> {

			T last = result.get(value);
			Entity<T> entity = operations.forEntity(last);

			Map<String, Object> keys = entity.extractKeys(sortObject);
			return KeysetScrollPosition.of(keys);
		};

		return createWindow(result, limit, positionFunction);
	}

	static <T> Window<T> createWindow(List<T> result, int limit, IntFunction<? extends ScrollPosition> positionFunction) {
		return Window.from(getSubList(result, limit), positionFunction, hasMoreElements(result, limit));
	}

	static boolean hasMoreElements(List<?> result, int limit) {
		return !result.isEmpty() && result.size() > limit;
	}

	static <T> List<T> getSubList(List<T> result, int limit) {

		if (limit > 0 && result.size() > limit) {
			return result.subList(0, limit);
		}

		return result;
	}

	record KeySetScrollQuery(Document query, Document fields, Document sort) {

	}

}
