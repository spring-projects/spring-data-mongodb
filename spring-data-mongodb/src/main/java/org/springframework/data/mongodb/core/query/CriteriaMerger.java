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
package org.springframework.data.mongodb.core.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;

/**
 * Merges criteria {@link Document documents} into a single one. Keys contained in multiple documents are combined using
 * {@literal $and} while all other keys remain top-level.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
class CriteriaMerger {

	private static final String AND_CONJUNCTION = "$and";

	/**
	 * @param thisDocument must not be {@literal null}
	 * @param thatDocument must not be {@literal null}
	 * @return {@literal true} if the given documents contain overlapping keys or already contain {@literal $and}.
	 */
	static boolean requiresMerge(Document thisDocument, Document thatDocument) {

		if (containsConjunction(thisDocument) || containsConjunction(thatDocument)) {
			return true;
		}

		for (String key : thatDocument.keySet()) {
			if (thisDocument.containsKey(key)) {
				return true;
			}
		}

		return false;
	}

	static Document merge(Document thisDocument, Document thatDocument) {
		return merge(List.of(thisDocument, thatDocument));
	}

	static Document merge(List<Document> criteriaDocuments) {

		Document merged = new Document();

		for (Document criteriaDocument : criteriaDocuments) {
			for (Map.Entry<String, Object> entry : criteriaDocument.entrySet()) {
				if (merged.containsKey(entry.getKey())) {
					return mergeCollidingKeysWithAnd(criteriaDocuments);
				}
				merged.put(entry.getKey(), entry.getValue());
			}
		}

		if (merged.containsKey(AND_CONJUNCTION)) {
			assertSingleTextAndGeoNear(merged);
		}

		return merged;
	}

	private static Document mergeCollidingKeysWithAnd(List<Document> criteriaDocuments) {

		Set<String> keys = new HashSet<>();
		Set<String> collidingKeys = new HashSet<>();

		for (Document criteriaDocument : criteriaDocuments) {
			for (String key : criteriaDocument.keySet()) {
				if (!keys.add(key)) {
					collidingKeys.add(key);
				}
			}
		}

		Document merged = new Document();
		List<Object> and = new ArrayList<>();

		for (Document criteriaDocument : criteriaDocuments) {
			for (Map.Entry<String, Object> entry : criteriaDocument.entrySet()) {

				String key = entry.getKey();
				Object value = entry.getValue();

				if (isConjunction(entry.getKey())) {
					merged.putIfAbsent(key, and);
					if (value instanceof Collection<?> clauses && !clauses.isEmpty()) {
						and.addAll(clauses);
					} else {
						and.add(new Document(key, value));
					}
				} else if (collidingKeys.contains(key)) {
					merged.putIfAbsent(AND_CONJUNCTION, and);
					and.add(new Document(key, value));
				} else {
					merged.put(key, value);
				}
			}
		}

		assertSingleTextAndGeoNear(merged);
		return merged;
	}

	/**
	 * Asserts a single {@literal $text} and a single {@literal $near}/{@literal $nearSphere} per key across the top level
	 * and the clauses of {@literal $and}.
	 */
	private static void assertSingleTextAndGeoNear(Document merged) {

		Set<String> keys = new HashSet<>();

		for (Map.Entry<String, Object> entry : merged.entrySet()) {
			if (isConjunction(entry.getKey()) && entry.getValue() instanceof Collection<?> clauses) {
				for (Object clause : clauses) {
					if (clause instanceof Document document) {
						document.forEach((key, value) -> assertSingleTextAndGeoNear(key, value, keys));
					}
				}
			} else {
				assertSingleTextAndGeoNear(entry.getKey(), entry.getValue(), keys);
			}
		}
	}

	private static void assertSingleTextAndGeoNear(String key, Object value, Set<String> keys) {

		if ("$text".equals(key)) {
			if (!keys.add(key)) {
				throw new InvalidMongoDbApiUsageException("A query must not contain more than one '$text' expression");
			}
		} else if (value instanceof Document document
				&& (document.containsKey("$near") || document.containsKey("$nearSphere")) && !keys.add(key)) {
			throw new InvalidMongoDbApiUsageException(
					"A query must not contain more than one '$near' or '$nearSphere' expression for '%s'".formatted(key));
		}
	}

	private static boolean containsConjunction(Document source) {
		return source.containsKey(AND_CONJUNCTION);
	}

	private static boolean isConjunction(String key) {
		return AND_CONJUNCTION.equals(key);
	}
}
