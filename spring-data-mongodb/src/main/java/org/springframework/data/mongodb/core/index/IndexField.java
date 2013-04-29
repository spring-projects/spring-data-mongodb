/*
 * Copyright 2012-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.index;

import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.query.Order;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Value object for an index field.
 * 
 * @author Oliver Gierke
 */
@SuppressWarnings("deprecation")
public final class IndexField {

	private final String key;
	private final Direction direction;
	private final boolean isGeo;

	private IndexField(String key, Direction direction, boolean isGeo) {

		Assert.hasText(key);
		Assert.isTrue(direction != null ^ isGeo);

		this.key = key;
		this.direction = direction;
		this.isGeo = isGeo;
	}

	/**
	 * Creates a default {@link IndexField} with the given key and {@link Order}.
	 * 
	 * @deprecated use {@link #create(String, Direction)}.
	 * @param key must not be {@literal null} or emtpy.
	 * @param direction must not be {@literal null}.
	 * @return
	 */
	@Deprecated
	public static IndexField create(String key, Order order) {
		Assert.notNull(order);
		return new IndexField(key, order.toDirection(), false);
	}

	public static IndexField create(String key, Direction order) {
		Assert.notNull(order);
		return new IndexField(key, order, false);
	}

	/**
	 * Creates a geo {@link IndexField} for the given key.
	 * 
	 * @param key must not be {@literal null} or empty.
	 * @return
	 */
	public static IndexField geo(String key) {
		return new IndexField(key, null, true);
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
	 * @deprecated use {@link #getDirection()} instead.
	 * @return the direction
	 */
	@Deprecated
	public Order getOrder() {
		return Direction.ASC.equals(direction) ? Order.ASCENDING : Order.DESCENDING;
	}

	/**
	 * Returns the direction of the {@link IndexField} or {@literal null} in case we have a geo index field.
	 * 
	 * @return the direction
	 */
	public Direction getDirection() {
		return direction;
	}

	/**
	 * Returns whether the {@link IndexField} is a geo index field.
	 * 
	 * @return the isGeo
	 */
	public boolean isGeo() {
		return isGeo;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof IndexField)) {
			return false;
		}

		IndexField that = (IndexField) obj;

		return this.key.equals(that.key) && ObjectUtils.nullSafeEquals(this.direction, that.direction)
				&& this.isGeo == that.isGeo;
	}

	/* 
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = 17;
		result += 31 * ObjectUtils.nullSafeHashCode(key);
		result += 31 * ObjectUtils.nullSafeHashCode(direction);
		result += 31 * ObjectUtils.nullSafeHashCode(isGeo);
		return result;
	}

	/* 
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("IndexField [ key: %s, direction: %s, isGeo: %s]", key, direction, isGeo);
	}
}
