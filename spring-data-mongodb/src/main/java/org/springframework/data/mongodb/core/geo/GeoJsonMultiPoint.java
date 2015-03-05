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

import static java.util.Collections.*;
import static org.springframework.util.Assert.*;
import static org.springframework.util.ObjectUtils.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.data.geo.Point;

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
	 * @param points points must not be {@literal null} and have at least 2 entries.
	 */
	public GeoJsonMultiPoint(List<Point> points) {

		notNull(points, "Points must not be null.");
		isTrue(points.size() >= 2, "Minimum of 2 Points required.");

		this.points = new ArrayList<Point>(points);
	}

	public GeoJsonMultiPoint(Point p0, Point p1, Point... others) {

		this.points = new ArrayList<Point>();
		this.points.add(p0);
		this.points.add(p1);
		if (!isEmpty(others)) {
			this.points.addAll(Arrays.asList(others));
		}
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
		return unmodifiableList(this.points);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return nullSafeHashCode(this.points);
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
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof GeoJsonMultiPoint)) {
			return false;
		}
		return nullSafeEquals(this.points, ((GeoJsonMultiPoint) obj).points);
	}

}
