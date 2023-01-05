/*
 * Copyright 2015-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.geo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.data.geo.Point;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * {@link GeoJsonMultiPoint} is defined as list of {@link Point}s.
 *
 * @author Christoph Strobl
 * @author Ivan Volzhev
 * @since 1.7
 * @see <a href="https://geojson.org/geojson-spec.html#multipoint">https://geojson.org/geojson-spec.html#multipoint</a>
 */
public class GeoJsonMultiPoint implements GeoJson<Iterable<Point>> {

	private static final String TYPE = "MultiPoint";

	private final List<Point> points;

	/**
	 * Creates a new {@link GeoJsonMultiPoint} for the given {@link Point}.
	 *
	 * @param point must not be {@literal null}.
	 * @since 3.2.5
	 */
	public GeoJsonMultiPoint(Point point) {

		Assert.notNull(point, "Point must not be null");

		this.points = new ArrayList<>();
		this.points.add(point);
	}

	/**
	 * Creates a new {@link GeoJsonMultiPoint} for the given {@link Point}s.
	 *
	 * @param points points must not be {@literal null} and not empty
	 */
	public GeoJsonMultiPoint(List<Point> points) {

		Assert.notNull(points, "Points must not be null");
		Assert.notEmpty(points, "Points must contain at least one point");

		this.points = new ArrayList<>(points);
	}

	/**
	 * Creates a new {@link GeoJsonMultiPoint} for the given {@link Point}s.
	 *
	 * @param first must not be {@literal null}.
	 * @param second must not be {@literal null}.
	 * @param others must not be {@literal null}.
	 */
	public GeoJsonMultiPoint(Point first, Point second, Point... others) {

		Assert.notNull(first, "First point must not be null");
		Assert.notNull(second, "Second point must not be null");
		Assert.notNull(others, "Additional points must not be null");

		this.points = new ArrayList<>();
		this.points.add(first);
		this.points.add(second);
		this.points.addAll(Arrays.asList(others));
	}

	@Override
	public String getType() {
		return TYPE;
	}

	@Override
	public List<Point> getCoordinates() {
		return Collections.unmodifiableList(this.points);
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(this.points);
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof GeoJsonMultiPoint other)) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(this.points, other.points);
	}
}
