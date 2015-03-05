/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb.core.geo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.data.geo.Point;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * {@link GeoJsonMultiPoint} is defined as list of {@link Point}s.
 * 
 * @author Christoph Strobl
 * @since 1.7
 * @see http://geojson.org/geojson-spec.html#multipoint
 */
public class GeoJsonMultiPoint implements GeoJson<Iterable<Point>> {

	private static final String TYPE = "MultiPoint";

	private final List<Point> points;

	/**
	 * Creates a new {@link GeoJsonMultiPoint} for the given {@link Point}s.
	 * 
	 * @param points points must not be {@literal null} and have at least 2 entries.
	 */
	public GeoJsonMultiPoint(List<Point> points) {

		Assert.notNull(points, "Points must not be null.");
		Assert.isTrue(points.size() >= 2, "Minimum of 2 Points required.");

		this.points = new ArrayList<Point>(points);
	}

	/**
	 * Creates a new {@link GeoJsonMultiPoint} for the given {@link Point}s.
	 * 
	 * @param first must not be {@literal null}.
	 * @param second must not be {@literal null}.
	 * @param others must not be {@literal null}.
	 */
	public GeoJsonMultiPoint(Point first, Point second, Point... others) {

		Assert.notNull(first, "First point must not be null!");
		Assert.notNull(second, "Second point must not be null!");
		Assert.notNull(others, "Additional points must not be null!");

		this.points = new ArrayList<Point>();
		this.points.add(first);
		this.points.add(second);
		this.points.addAll(Arrays.asList(others));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.geo.GeoJson#getType()
	 */
	@Override
	public String getType() {
		return TYPE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.geo.GeoJson#getCoordinates()
	 */
	@Override
	public List<Point> getCoordinates() {
		return Collections.unmodifiableList(this.points);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(this.points);
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

		if (!(obj instanceof GeoJsonMultiPoint)) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(this.points, ((GeoJsonMultiPoint) obj).points);
	}
}
