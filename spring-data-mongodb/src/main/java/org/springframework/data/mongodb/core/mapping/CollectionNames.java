/*
 * Copyright 2026 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.data.mongodb.MongoCollectionUtils;
import org.springframework.util.ObjectUtils;

/**
 * Holder for collection name implementations.
 *
 * @author Mark Paluch
 * @since 5.1
 */
class CollectionNames {

	public record StaticCollectionName(String collectionName) implements CollectionName {

		@Override
		public String getCollectionName() {
			return collectionName;
		}

		@Override
		public String getCollectionName(Function<Class<?>, @Nullable MongoPersistentEntity<?>> entityLookup) {
			return getCollectionName();
		}

		@Override
		public Class<?> getEntityClass() {
			return Object.class;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof StaticCollectionName that)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(collectionName, that.collectionName);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(collectionName);
		}

		@Override
		public String toString() {
			return collectionName;
		}

	}

	public record DerivedCollectionName(Class<?> entityClass) implements CollectionName {

		@Override
		public String getCollectionName() {
			return MergedAnnotations.from(entityClass).get(Document.class).getValue("collection", String.class)
					.orElseGet(() -> MongoCollectionUtils.getPreferredCollectionName(entityClass));
		}

		@Override
		public String getCollectionName(Function<Class<?>, @Nullable MongoPersistentEntity<?>> entityLookup) {

			MongoPersistentEntity<?> entity = entityLookup.apply(getEntityClass());
			return entity != null ? entity.getCollection() : getCollectionName();
		}

		@Override
		public Class<?> getEntityClass() {
			return entityClass;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof DerivedCollectionName that)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(entityClass, that.entityClass);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(entityClass);
		}

		@Override
		public String toString() {
			return String.format("%s (derived from %s)", getCollectionName(), entityClass.getName());
		}

	}
}
