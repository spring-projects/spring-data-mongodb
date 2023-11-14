/*
 * Copyright 2012-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import org.springframework.data.domain.Sort.Direction;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Value object for an index field.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public final class IndexField {

	enum Type {
		GEO, TEXT, DEFAULT,

		/**
		 * @since 2.2
		 */
		HASH,

		/**
		 * @since 3.3
		 */
		WILDCARD
	}

	private final String key;
	private final @Nullable Direction direction;
	private final Type type;
	private final Float weight;

	private IndexField(String key, @Nullable Direction direction, @Nullable Type type) {
		this(key, direction, type, Float.NaN);
	}

	private IndexField(String key, @Nullable Direction direction, @Nullable Type type, @Nullable Float weight) {

		Assert.hasText(key, "Key must not be null or empty");

		if (Type.GEO.equals(type) || Type.TEXT.equals(type)) {
			Assert.isNull(direction, "Geo/Text indexes must not have a direction");
		} else {
			if (!(Type.HASH.equals(type) || Type.WILDCARD.equals(type))) {
				Assert.notNull(direction, "Default indexes require a direction");
			}
		}

		this.key = key;
		this.direction = direction;
		this.type = type == null ? Type.DEFAULT : type;
		this.weight = weight == null ? Float.NaN : weight;
	}

	public static IndexField create(String key, Direction order) {

		Assert.notNull(order, "Direction must not be null");

		return new IndexField(key, order, Type.DEFAULT);
	}

	/**
	 * Creates a {@literal hashed} {@link IndexField} for the given key.
	 *
	 * @param key must not be {@literal null} or empty.
	 * @return new instance of {@link IndexField}.
	 * @since 2.2
	 */
	static IndexField hashed(String key) {
		return new IndexField(key, null, Type.HASH);
	}

	/**
	 * Creates a {@literal wildcard} {@link IndexField} for the given key. The {@code key} must follow the
	 * {@code fieldName.$**} notation.
	 *
	 * @param key must not be {@literal null} or empty.
	 * @return new instance of {@link IndexField}.
	 * @since 3.3
	 */
	static IndexField wildcard(String key) {
		return new IndexField(key, null, Type.WILDCARD);
	}

	/**
	 * Creates a geo {@link IndexField} for the given key.
	 *
	 * @param key must not be {@literal null} or empty.
	 * @return new instance of {@link IndexField}.
	 */
	public static IndexField geo(String key) {
		return new IndexField(key, null, Type.GEO);
	}

	/**
	 * Creates a text {@link IndexField} for the given key.
	 *
	 * @since 1.6
	 */
	public static IndexField text(String key, Float weight) {
		return new IndexField(key, null, Type.TEXT, weight);
	}

	/**
	 * @return the key
	 */
	public String getKey() {
		return key;
	}

	/**
	 * Returns the direction of the {@link IndexField} or {@literal null} in case we have a geo index field.
	 *
	 * @return the direction
	 */
	@Nullable
	public Direction getDirection() {
		return direction;
	}

	/**
	 * Returns whether the {@link IndexField} is a geo index field.
	 *
	 * @return true if type is {@link Type#GEO}.
	 */
	public boolean isGeo() {
		return Type.GEO.equals(type);
	}

	/**
	 * Returns whether the {@link IndexField} is a text index field.
	 *
	 * @return true if type is {@link Type#TEXT}
	 * @since 1.6
	 */
	public boolean isText() {
		return Type.TEXT.equals(type);
	}

	/**
	 * Returns whether the {@link IndexField} is a {@literal hashed}.
	 *
	 * @return {@literal true} if {@link IndexField} is hashed.
	 * @since 2.2
	 */
	public boolean isHashed() {
		return Type.HASH.equals(type);
	}

	/**
	 * Returns whether the {@link IndexField} is contains a {@literal wildcard} expression.
	 *
	 * @return {@literal true} if {@link IndexField} contains a wildcard {@literal $**}.
	 * @since 3.3
	 */
	public boolean isWildcard() {
		return Type.WILDCARD.equals(type);
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof IndexField other)) {
			return false;
		}

		return this.key.equals(other.key) && ObjectUtils.nullSafeEquals(this.direction, other.direction)
				&& this.type == other.type;
	}

	@Override
	public int hashCode() {

		int result = 17;
		result += 31 * ObjectUtils.nullSafeHashCode(key);
		result += 31 * ObjectUtils.nullSafeHashCode(direction);
		result += 31 * ObjectUtils.nullSafeHashCode(type);
		result += 31 * ObjectUtils.nullSafeHashCode(weight);
		return result;
	}

	@Override
	public String toString() {
		return String.format("IndexField [ key: %s, direction: %s, type: %s, weight: %s]", key, direction, type, weight);
	}

}
