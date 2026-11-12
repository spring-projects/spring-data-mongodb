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
package org.springframework.data.mongodb.core.convert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

/**
 * Internal helper index of {@link Document documents} by their {@literal _id}. Array identifiers are compared by content.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
class DocumentIdIndex {

	private static final int NONE = -1;

	private final List<Document> documents;
	private final Map<Object, Integer> heads;
	private final int[] next;
	private final boolean hasArrayKeys;

	private DocumentIdIndex(List<Document> documents, Map<Object, Integer> heads, int[] next, boolean hasArrayKeys) {

		this.documents = documents;
		this.heads = heads;
		this.next = next;
		this.hasArrayKeys = hasArrayKeys;
	}

	static DocumentIdIndex of(List<Document> documents) {

		List<Document> source = documents instanceof RandomAccess ? documents : new ArrayList<>(documents);
		Map<Object, Integer> heads = CollectionUtils.newHashMap(source.size());
		int[] next = new int[source.size()];
		boolean hasArrayKeys = false;

		// walk backwards so the head of each chain is the first occurrence
		for (int position = source.size() - 1; position >= 0; position--) {

			Document document = source.get(position);
			if (!document.containsKey(BasicMongoPersistentProperty.ID_FIELD_NAME)) {
				continue;
			}

			Object id = document.get(BasicMongoPersistentProperty.ID_FIELD_NAME);
			if (ObjectUtils.isArray(id)) {
				hasArrayKeys = true;
			}

			Integer previous = heads.put(key(id), position);
			next[position] = previous != null ? previous : NONE;
		}

		return new DocumentIdIndex(source, heads, next, hasArrayKeys);
	}

	/**
	 * Select the first indexed document for each of the given identifiers. Identifiers without a matching document are
	 * skipped, repeated identifiers repeat the document.
	 *
	 * @param ids identifiers in the desired result order.
	 * @return the matching documents in order of the given identifiers.
	 */
	List<Document> select(List<?> ids) {

		List<Document> result = new ArrayList<>(ids.size());

		for (Object id : ids) {

			Document document = get(id);
			if (document != null) {
				result.add(document);
			}
		}
		return result;
	}

	/**
	 * Get the first indexed document for the given identifier.
	 *
	 * @param id the identifier to look up.
	 * @return the first matching document or {@literal null} if none found.
	 */
	@Nullable
	Document get(@Nullable Object id) {

		Integer head = heads.get(lookupKey(id));
		return head != null ? documents.get(head) : null;
	}

	/**
	 * Rank the indexed documents by the position of the first reference pointing to their {@literal _id}. Documents
	 * sharing an identifier share a rank. Documents not referenced rank after all others.
	 *
	 * @param references documents holding the referenced {@literal _id}.
	 * @return rank per indexed document position.
	 */
	int[] rank(List<Document> references) {

		int unmatched = references.size();
		int[] ranks = new int[next.length];
		Arrays.fill(ranks, unmatched);

		int remaining = heads.size();
		int rank = 0;
		for (Document reference : references) {

			if (remaining == 0) {
				break;
			}

			Integer head = heads.get(lookupKey(reference.get(BasicMongoPersistentProperty.ID_FIELD_NAME)));
			if (head != null && ranks[head] == unmatched) {

				for (int i = head; i != NONE; i = next[i]) {
					ranks[i] = rank;
				}
				remaining--;
			}
			rank++;
		}
		return ranks;
	}

	private @Nullable Object lookupKey(@Nullable Object id) {
		return hasArrayKeys ? key(id) : id;
	}

	private static @Nullable Object key(@Nullable Object id) {
		return ObjectUtils.isArray(id) ? new ArrayKey(id) : id;
	}

	private record ArrayKey(Object array) {

		@Override
		public boolean equals(@Nullable Object other) {
			return other instanceof ArrayKey key && ObjectUtils.nullSafeEquals(array, key.array);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(array);
		}
	}
}
