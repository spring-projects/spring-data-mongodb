/*
 * Copyright 2023-present the original author or authors.
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.bson.BsonNull;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.ScrollPosition.Direction;
import org.springframework.data.domain.Window;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.lang.CheckReturnValue;
import org.springframework.util.Assert;

/**
 * Utilities to run scroll queries and create {@link Window} results.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Jens Schauder
 * @author Priyanshu Gagiya
 * @since 4.1
 */
class ScrollUtils {

	/**
	 * Create the actual query to run keyset-based pagination. Affects projection, sorting, and the criteria.
	 *
	 * @param query
	 * @param idPropertyName
	 * @param sortMapper maps sort field names to their stored names
	 * @return
	 */
	static KeysetScrollQuery createKeysetPaginationQuery(Query query, String idPropertyName,
			Function<Document, Document> sortMapper) {

		KeysetScrollPosition keyset = query.getKeyset();

		Assert.notNull(keyset, "Query.keyset must not be null");

		KeysetScrollDirector director = KeysetScrollDirector.of(keyset.getDirection());
		Document sortObject = director.getSortObject(idPropertyName, query);
		Document fieldsObject = director.getFieldsObject(query.getFieldsObject(), sortObject);
		Map<String, String> keyFields = new LinkedHashMap<>();
		for (String key : sortObject.keySet()) {
			keyFields.put(key, sortMapper.apply(new Document(key, 1)).keySet().iterator().next());
		}
		Document queryObject = director.createQuery(keyset, query.getQueryObject(), sortObject, keyFields::get);

		return new KeysetScrollQuery(queryObject, fieldsObject, sortObject, keyFields);
	}

	static <T> Window<T> createWindow(Query query, List<KeysetScrollResult<T>> result) {

		KeysetScrollPosition keyset = query.getKeyset();
		Assert.notNull(keyset, "Query.keyset must not be null");

		Direction direction = keyset.getDirection();
		List<KeysetScrollResult<T>> resultsToUse = KeysetScrollDirector.of(direction).postPostProcessResults(result,
				query.getLimit());
		IntFunction<ScrollPosition> positionFunction = index -> {
			return ScrollPosition.of(resultsToUse.get(index).keys(), direction);
		};

		return Window.from(resultsToUse.stream().map(KeysetScrollResult::entity).toList(), positionFunction,
				hasMoreElements(result, query.getLimit()));
	}

	record KeysetScrollResult<T>(T entity, Map<String, Object> keys) {
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

	record KeysetScrollQuery(Document query, Document fields, Document sort, Map<String, String> keyFields) {

		Map<String, Object> captureKeys(Map<String, Object> document, CodecRegistry codecRegistry) {

			Document keys = new Document();
			keyFields.forEach((key, field) -> {
				Object value = BsonUtils.resolveValue(document, field);
				if (value == null || value instanceof BsonNull) {
					throw new IllegalStateException("Cannot extract value for key %s because its value is null".formatted(key));
				}
				keys.put(key, value);
			});
			Codec<Document> codec = codecRegistry.get(Document.class);
			// Snapshot mutable BSON values before conversion or lifecycle callbacks can change them.
			return new RawBsonDocument(keys, codec).decode(codec);
		}
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

		@CheckReturnValue
		public Document createQuery(KeysetScrollPosition keyset, Document queryObject, Document sortObject,
				Function<String, String> fieldMapper) {

			Map<String, Object> keysetValues = keyset.getKeys();
			List<String> sortKeys = new ArrayList<>(sortObject.keySet());

			// first query doesn't come with a keyset
			if (keysetValues.isEmpty()) {
				return queryObject;
			}

			if (!keysetValues.keySet().containsAll(sortKeys)) {
				throw new IllegalStateException("KeysetScrollPosition does not contain all keyset values");
			}

			List<MongoExpression> or = getKeysetCriteria(sortObject, sortKeys, keysetValues, fieldMapper);
			if (or.isEmpty()) {
				return queryObject;
			}

			if (queryObject.containsKey("$or")) {
				return new Document("$and", List.of(queryObject, new Document("$or", or)));
			}

			Document filterQuery = new Document(queryObject);
			filterQuery.put("$or", or);
			return filterQuery;
		}

		private List<MongoExpression> getKeysetCriteria(Document sortObject, List<String> sortKeys,
				Map<String, Object> keysetValues, Function<String, String> fieldMapper) {

			List<MongoExpression> or = new ArrayList<>();

			for (int i = 0; i < sortKeys.size(); i++) {

				Document sortConstraint = new Document();
				for (int j = 0; j <= i; j++) {

					String sortSegment = sortKeys.get(j);
					String field = fieldMapper.apply(sortSegment);
					Object value = keysetValues.get(sortSegment);
					if (value == null || value instanceof BsonNull) {
						throw new IllegalStateException(
								"Cannot resume from KeysetScrollPosition. Offending key: '%s' is 'null'".formatted(sortSegment));
					}

					if (j == i) {
						sortConstraint.put("$expr", new Document(getComparator(sortObject.getInteger(sortSegment)),
								List.of("$" + field, new Document("$literal", value))));
					} else {
						sortConstraint.put(field, new Document("$eq", value));
					}
				}
				// Field names are mapped; values must retain their stored BSON types.
				or.add(() -> {
					return sortConstraint;
				});
			}

			return or;
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
