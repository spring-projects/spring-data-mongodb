/*
 * Copyright 2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *			http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.geo;

import org.springframework.util.Assert;

/**
 * Calue object capturing some arbitrary object plus a distance.
 * 
 * @author Oliver Gierke
 */
public class GeoResult<T> {

	private final T content;
	private final Distance distance;

	/**
	 * Creates a new {@link GeoResult} for the given content and distance.
	 * 
	 * @param content must not be {@literal null}.
	 * @param distance must not be {@literal null}.
	 */
	public GeoResult(T content, Distance distance) {
		Assert.notNull(content);
		Assert.notNull(distance);
		this.content = content;
		this.distance = distance;
	}

	/**
	 * Returns the actual content object.
	 * 
	 * @return the content
	 */
	public T getContent() {
		return content;
	}

	/**
	 * Returns the distance the actual content object has from the origin.
	 * 
	 * @return the distance
	 */
	public Distance getDistance() {
		return distance;
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

		if (obj == null || !getClass().equals(obj.getClass())) {
			return false;
		}

		GeoResult<?> that = (GeoResult<?>) obj;

		return this.content.equals(that.content) && this.distance.equals(that.distance);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = 17;
		result += 31 * distance.hashCode();
		result += 31 * content.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("GeoResult [content: %s, distance: %s, ]", content.toString(), distance.toString());
	}
}