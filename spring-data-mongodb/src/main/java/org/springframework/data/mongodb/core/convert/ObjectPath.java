/*
 * Copyright 2014-2019 the original author or authors.
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
import java.util.List;

import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * A path of objects nested into each other. The type allows access to all parent objects currently in creation even
 * when resolving more nested objects. This allows to avoid re-resolving object instances that are logically equivalent
 * to already resolved ones.
 * <p>
 * An immutable ordered set of target objects for {@link org.bson.Document} to {@link Object} conversions. Object paths
 * can be extended via {@link #push(Object, MongoPersistentEntity, Object)}.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.6
 */
class ObjectPath {

	static final ObjectPath ROOT = new ObjectPath();

	private final @Nullable ObjectPath parent;
	private final @Nullable Object object;
	private final @Nullable Object idValue;
	private final String collection;

	private ObjectPath() {

		this.parent = null;
		this.object = null;
		this.idValue = null;
		this.collection = "";
	}

	/**
	 * Creates a new {@link ObjectPath} from the given parent {@link ObjectPath} and adding the provided path values.
	 *
	 * @param parent must not be {@literal null}.
	 * @param collection
	 * @param idValue
	 * @param collection
	 */
	private ObjectPath(ObjectPath parent, Object object, @Nullable Object idValue, String collection) {

		this.parent = parent;
		this.object = object;
		this.idValue = idValue;
		this.collection = collection;
	}

	/**
	 * Returns a copy of the {@link ObjectPath} with the given {@link Object} as current object.
	 *
	 * @param object must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @param id must not be {@literal null}.
	 * @return new instance of {@link ObjectPath}.
	 */
	ObjectPath push(Object object, MongoPersistentEntity<?> entity, @Nullable Object id) {

		Assert.notNull(object, "Object must not be null!");
		Assert.notNull(entity, "MongoPersistentEntity must not be null!");

		return new ObjectPath(this, object, id, entity.getCollection());
	}

	/**
	 * Returns the object with the given id and stored in the given collection if it's contained in the
	 * {@link ObjectPath}.
	 *
	 * @param id must not be {@literal null}.
	 * @param collection must not be {@literal null} or empty.
	 * @return
	 * @deprecated use {@link #getPathItem(Object, String, Class)}.
	 */
	@Nullable
	@Deprecated
	Object getPathItem(Object id, String collection) {

		Assert.notNull(id, "Id must not be null!");
		Assert.hasText(collection, "Collection name must not be null!");

		for (ObjectPath current = this; current != null; current = current.parent) {

			Object object = current.getObject();

			if (object == null || current.getIdValue() == null) {
				continue;
			}

			if (collection.equals(current.getCollection()) && id.equals(current.getIdValue())) {
				return object;
			}
		}

		return null;
	}

	/**
	 * Get the object with given {@literal id}, stored in the {@literal collection} that is assignable to the given
	 * {@literal type} or {@literal null} if no match found.
	 *
	 * @param id must not be {@literal null}.
	 * @param collection must not be {@literal null} or empty.
	 * @param type must not be {@literal null}.
	 * @return {@literal null} when no match found.
	 * @since 2.0
	 */
	@Nullable
	<T> T getPathItem(Object id, String collection, Class<T> type) {

		Assert.notNull(id, "Id must not be null!");
		Assert.hasText(collection, "Collection name must not be null!");
		Assert.notNull(type, "Type must not be null!");

		for (ObjectPath current = this; current != null; current = current.parent) {

			Object object = current.getObject();

			if (object == null || current.getIdValue() == null) {
				continue;
			}

			if (collection.equals(current.getCollection()) && id.equals(current.getIdValue())
					&& ClassUtils.isAssignable(type, object.getClass())) {
				return type.cast(object);
			}
		}

		return null;
	}

	/**
	 * Returns the current object of the {@link ObjectPath} or {@literal null} if the path is empty.
	 *
	 * @return
	 */
	@Nullable
	Object getCurrentObject() {
		return getObject();
	}

	@Nullable
	private Object getObject() {
		return object;
	}

	@Nullable
	private Object getIdValue() {
		return idValue;
	}

	private String getCollection() {
		return collection;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		if (parent == null) {
			return "[empty]";
		}

		List<String> strings = new ArrayList<>();

		for (ObjectPath current = this; current != null; current = current.parent) {
			strings.add(ObjectUtils.nullSafeToString(current.getObject()));
		}

		return StringUtils.collectionToDelimitedString(strings, " -> ");
	}
}
