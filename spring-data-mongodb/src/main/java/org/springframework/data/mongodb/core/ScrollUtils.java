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
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.ScrollPosition.Direction;
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
		Direction direction = keyset.getDirection();
		KeysetScrollDirector director = KeysetScrollDirector.of(direction);

		List<T> resultsToUse = director.postPostProcessResults(result, query.getLimit());

		IntFunction<ScrollPosition> positionFunction = value -> {

			T last = resultsToUse.get(value);
			Entity<T> entity = operations.forEntity(last);

			Map<String, Object> keys = entity.extractKeys(sortObject, sourceType);
			return ScrollPosition.of(keys, direction);
		};

		return Window.from(resultsToUse, positionFunction, hasMoreElements(result, query.getLimit()));
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

		private static final KeysetScrollDirector FORWARD = new KeysetScrollDirector();
		private static final KeysetScrollDirector REVERSE = new ReverseKeysetScrollDirector();

		/**
		 * Factory method to obtain the right {@link KeysetScrollDirector}.
		 *
		 * @param direction
		 * @return
		 */
		public static KeysetScrollDirector of(ScrollPosition.Direction direction) {
			return direction == Direction.FORWARD ? FORWARD : REVERSE;
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

		protected String getComparator(int sortOrder) {
			return sortOrder == 1 ? "$gt" : "$lt";
		}

		protected <T> List<T> postPostProcessResults(List<T> list, int limit) {
			return getFirst(limit, list);
		}

	}

	/**
	 * Reverse scrolling director variant applying {@link KeysetScrollPosition.Direction#BACKWARD}. In reverse scrolling,
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
		public <T> List<T> postPostProcessResults(List<T> list, int limit) {

			// flip direction of the result list as we need to accomodate for the flipped sort order for proper offset
			// querying.
			Collections.reverse(list);

			return getLast(limit, list);
		}

	}

	/**
	 * Return the first {@code count} items from the list.
	 *
	 * @param count
	 * @param list
	 * @return
	 * @param <T>
	 */
	static <T> List<T> getFirst(int count, List<T> list) {

		if (count > 0 && list.size() > count) {
			return list.subList(0, count);
		}

		return list;
	}

	/**
	 * Return the last {@code count} items from the list.
	 *
	 * @param count
	 * @param list
	 * @return
	 * @param <T>
	 */
	static <T> List<T> getLast(int count, List<T> list) {

		if (count > 0 && list.size() > count) {
			return list.subList(list.size() - count, list.size());
		}

		return list;
	}
}
