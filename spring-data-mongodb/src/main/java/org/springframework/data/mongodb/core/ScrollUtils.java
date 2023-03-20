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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.bson.BsonNull;
import org.bson.Document;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.KeysetScrollPosition.Direction;
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
	static KeysetScrollQuery createKeysetPaginationQuery(Query query, String idPropertyName) {

		KeysetScrollPosition keyset = query.getKeyset();
		KeysetScrollDirector director = KeysetScrollDirector.of(keyset.getDirection());
		Document sortObject = director.getSortObject(idPropertyName, query);
		Document fieldsObject = director.getFieldsObject(query.getFieldsObject(), sortObject);
		Document queryObject = director.createQuery(keyset, query.getQueryObject(), sortObject);

		return new KeysetScrollQuery(queryObject, fieldsObject, sortObject);
	}

	static <T> Window<T> createWindow(Query query, List<T> result, Class<?> sourceType, EntityOperations operations) {

		Document sortObject = query.getSortObject();
		KeysetScrollPosition keyset = query.getKeyset();
		KeysetScrollDirector director = KeysetScrollDirector.of(keyset.getDirection());

		director.postPostProcessResults(result);

		IntFunction<KeysetScrollPosition> positionFunction = value -> {

			T last = result.get(value);
			Entity<T> entity = operations.forEntity(last);

			Map<String, Object> keys = entity.extractKeys(sortObject, sourceType);
			return KeysetScrollPosition.of(keys);
		};

		return createWindow(result, query.getLimit(), positionFunction);
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

	record KeysetScrollQuery(Document query, Document fields, Document sort) {

	}

	/**
	 * Director for keyset scrolling.
	 */
	static class KeysetScrollDirector {

		private static final KeysetScrollDirector forward = new KeysetScrollDirector();
		private static final KeysetScrollDirector reverse = new ReverseKeysetScrollDirector();

		/**
		 * Factory method to obtain the right {@link KeysetScrollDirector}.
		 *
		 * @param direction
		 * @return
		 */
		public static KeysetScrollDirector of(KeysetScrollPosition.Direction direction) {
			return direction == Direction.Forward ? forward : reverse;
		}

		public Document getSortObject(String idPropertyName, Query query) {

			Document sortObject = query.isSorted() ? query.getSortObject() : new Document();
			sortObject.put(idPropertyName, 1);

			return sortObject;
		}

		public Document getFieldsObject(Document fieldsObject, Document sortObject) {

			// make sure we can extract the keyset
			if (!fieldsObject.isEmpty()) {
				for (String field : sortObject.keySet()) {
					fieldsObject.put(field, 1);
				}
			}

			return fieldsObject;
		}

		public Document createQuery(KeysetScrollPosition keyset, Document queryObject, Document sortObject) {

			Map<String, Object> keysetValues = keyset.getKeys();
			List<Document> or = (List<Document>) queryObject.getOrDefault("$or", new ArrayList<>());
			List<String> sortKeys = new ArrayList<>(sortObject.keySet());

			// first query doesn't come with a keyset
			if (keysetValues.isEmpty()) {
				return queryObject;
			}

			if (!keysetValues.keySet().containsAll(sortKeys)) {
				throw new IllegalStateException("KeysetScrollPosition does not contain all keyset values");
			}

			// build matrix query for keyset paging that contains sort^2 queries
			// reflecting a query that follows sort order semantics starting from the last returned keyset
			for (int i = 0; i < sortKeys.size(); i++) {

				Document sortConstraint = new Document();

				for (int j = 0; j < sortKeys.size(); j++) {

					String sortSegment = sortKeys.get(j);
					int sortOrder = sortObject.getInteger(sortSegment);
					Object o = keysetValues.get(sortSegment);

					if (j >= i) { // tail segment
						if (o instanceof BsonNull) {
							throw new IllegalStateException(
									"Cannot resume from KeysetScrollPosition. Offending key: '%s' is 'null'".formatted(sortSegment));
						}
						sortConstraint.put(sortSegment, new Document(getComparator(sortOrder), o));
						break;
					}

					sortConstraint.put(sortSegment, o);
				}

				if (!sortConstraint.isEmpty()) {
					or.add(sortConstraint);
				}
			}

			if (!or.isEmpty()) {
				queryObject.put("$or", or);
			}

			return queryObject;
		}

		public <T> void postPostProcessResults(List<T> result) {

		}

		protected String getComparator(int sortOrder) {
			return sortOrder == 1 ? "$gt" : "$lt";
		}
	}

	/**
	 * Reverse scrolling director variant applying {@link KeysetScrollPosition.Direction#Backward}. In reverse scrolling,
	 * we need to flip directions for the actual query so that we do not get everything from the top position and apply
	 * the limit but rather flip the sort direction, apply the limit and then reverse the result to restore the actual
	 * sort order.
	 */
	private static class ReverseKeysetScrollDirector extends KeysetScrollDirector {

		@Override
		public Document getSortObject(String idPropertyName, Query query) {

			Document sortObject = super.getSortObject(idPropertyName, query);

			// flip sort direction for backward scrolling

			for (String field : sortObject.keySet()) {
				sortObject.put(field, sortObject.getInteger(field) == 1 ? -1 : 1);
			}

			return sortObject;
		}

		@Override
		protected String getComparator(int sortOrder) {

			// use gte/lte to include the object at the cursor/keyset so that
			// we can include it in the result to check whether there is a next object.
			// It needs to be filtered out later on.
			return sortOrder == 1 ? "$gte" : "$lte";
		}

		@Override
		public <T> void postPostProcessResults(List<T> result) {
			// flip direction of the result list as we need to accomodate for the flipped sort order for proper offset
			// querying.
			Collections.reverse(result);
		}
	}

}
