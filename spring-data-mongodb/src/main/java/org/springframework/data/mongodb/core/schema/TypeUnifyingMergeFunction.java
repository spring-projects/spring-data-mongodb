/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.bson.Document;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema.ConflictResolutionFunction;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema.ConflictResolutionFunction.Path;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema.ConflictResolutionFunction.Resolution;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Merge function considering BSON type hints. Conflicts are resolved through a {@link ConflictResolutionFunction}.
 *
 * @author Christoph Strobl
 * @since 3.4
 */
class TypeUnifyingMergeFunction implements BiFunction<Map<String, Object>, Map<String, Object>, Document> {

	private final ConflictResolutionFunction conflictResolutionFunction;

	public TypeUnifyingMergeFunction(ConflictResolutionFunction conflictResolutionFunction) {
		this.conflictResolutionFunction = conflictResolutionFunction;
	}

	@Override
	public Document apply(Map<String, Object> left, Map<String, Object> right) {
		return merge(SimplePath.root(), left, right);
	}

	@SuppressWarnings("unchecked")
	Document merge(SimplePath path, Map<String, Object> left, Map<String, Object> right) {

		Document target = new Document(left);

		for (String key : right.keySet()) {

			SimplePath currentPath = path.append(key);
			if (isTypeKey(key)) {

				Object unifiedExistingType = getUnifiedExistingType(key, target);

				if (unifiedExistingType != null) {
					if (!ObjectUtils.nullSafeEquals(unifiedExistingType, right.get(key))) {
						resolveConflict(currentPath, left, right, target);
					}
					continue;
				}
			}

			if (!target.containsKey(key)) {
				target.put(key, right.get(key));
				continue;
			}

			Object existingEntry = target.get(key);
			Object newEntry = right.get(key);
			if (existingEntry instanceof Map && newEntry instanceof Map) {
				target.put(key, merge(currentPath, (Map<String, Object>) existingEntry, (Map<String, Object>) newEntry));
			} else if (!ObjectUtils.nullSafeEquals(existingEntry, newEntry)) {
				resolveConflict(currentPath, left, right, target);
			}
		}

		return target;
	}

	private void resolveConflict(Path path, Map<String, Object> left, Map<String, Object> right, Document target) {
		applyConflictResolution(path, target, conflictResolutionFunction.resolveConflict(path, left, right));
	}

	private void applyConflictResolution(Path path, Document target, Resolution resolution) {

		if (Resolution.SKIP.equals(resolution) || resolution.getValue() == null) {
			target.remove(path.currentElement());
			return;
		}

		if (isTypeKey(resolution.getKey())) {
			target.put(getTypeKeyToUse(resolution.getKey(), target), resolution.getValue());
		} else {
			target.put(resolution.getKey(), resolution.getValue());
		}
	}

	private static boolean isTypeKey(String key) {
		return "bsonType".equals(key) || "type".equals(key);
	}

	private static String getTypeKeyToUse(String key, Document source) {

		if ("bsonType".equals(key) && source.containsKey("type")) {
			return "type";
		}
		if ("type".equals(key) && source.containsKey("bsonType")) {
			return "bsonType";
		}
		return key;
	}

	@Nullable
	private static Object getUnifiedExistingType(String key, Document source) {
		return source.get(getTypeKeyToUse(key, source));
	}

	/**
	 * Trivial {@link List} based {@link Path} implementation.
	 *
	 * @author Christoph Strobl
	 * @since 3.4
	 */
	static class SimplePath implements Path {

		private final List<String> path;

		SimplePath(List<String> path) {
			this.path = path;
		}

		static SimplePath root() {
			return new SimplePath(Collections.emptyList());
		}

		static SimplePath of(List<String> path, String next) {

			List<String> fullPath = new ArrayList<>(path.size() + 1);
			fullPath.addAll(path);
			fullPath.add(next);
			return new SimplePath(fullPath);
		}

		public SimplePath append(String next) {
			return of(this.path, next);
		}

		@Override
		public String currentElement() {
			return CollectionUtils.lastElement(path);
		}

		@Override
		public String dotPath() {
			return StringUtils.collectionToDelimitedString(path, ".");
		}

		@Override
		public String toString() {
			return dotPath();
		}
	}
}
