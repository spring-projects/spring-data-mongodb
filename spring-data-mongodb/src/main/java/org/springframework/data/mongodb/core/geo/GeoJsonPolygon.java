/*
 * Copyright 2015-2022 the original author or authors.
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
import java.util.Iterator;
import java.util.List;

import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * {@link GeoJson} representation of {@link Polygon}. Unlike {@link Polygon} the {@link GeoJsonPolygon} requires a
 * closed border. Which means that the first and last {@link Point} have to have same coordinate pairs.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 * @see <a href="https://geojson.org/geojson-spec.html#polygon">https://geojson.org/geojson-spec.html#polygon</a>
 */
public class GeoJsonPolygon extends Polygon implements GeoJson<List<GeoJsonLineString>> {

	private static final long serialVersionUID = 3936163018187247185L;
	private static final String TYPE = "Polygon";

	private List<GeoJsonLineString> coordinates = new ArrayList<GeoJsonLineString>();

	/**
	 * Creates new {@link GeoJsonPolygon} from the given {@link Point}s.
	 *
	 * @param first must not be {@literal null}.
	 * @param second must not be {@literal null}.
	 * @param third must not be {@literal null}.
	 * @param fourth must not be {@literal null}.
	 * @param others can be empty.
	 */
	public GeoJsonPolygon(Point first, Point second, Point third, Point fourth, Point... others) {
		this(asList(first, second, third, fourth, others));
	}

	/**
	 * Creates new {@link GeoJsonPolygon} from the given {@link Point}s.
	 *
	 * @param points must not be {@literal null}.
	 */
	public GeoJsonPolygon(List<Point> points) {

		super(points);
		this.coordinates.add(new GeoJsonLineString(points));
	}

	/**
	 * Creates a new {@link GeoJsonPolygon} with an inner ring defined be the given {@link Point}s.
	 *
	 * @param first must not be {@literal null}.
	 * @param second must not be {@literal null}.
	 * @param third must not be {@literal null}.
	 * @param fourth must not be {@literal null}.
	 * @param others can be empty.
	 * @return new {@link GeoJsonPolygon}.
	 * @since 1.10
	 */
	public GeoJsonPolygon withInnerRing(Point first, Point second, Point third, Point fourth, Point... others) {
		return withInnerRing(asList(first, second, third, fourth, others));
	}

	/**
	 * Creates a new {@link GeoJsonPolygon} with an inner ring defined be the given {@link List} of {@link Point}s.
	 *
	 * @param points must not be {@literal null}.
	 * @return new {@link GeoJsonPolygon}.
	 */
	public GeoJsonPolygon withInnerRing(List<Point> points) {
		return withInnerRing(new GeoJsonLineString(points));
	}

	/**
	 * Creates a new {@link GeoJsonPolygon} with an inner ring defined be the given {@link GeoJsonLineString}.
	 *
	 * @param lineString must not be {@literal null}.
	 * @return new {@link GeoJsonPolygon}.
	 * @since 1.10
	 */
	public GeoJsonPolygon withInnerRing(GeoJsonLineString lineString) {

		Assert.notNull(lineString, "LineString must not be null");

		Iterator<GeoJsonLineString> it = this.coordinates.iterator();
		GeoJsonPolygon polygon = new GeoJsonPolygon(it.next().getCoordinates());

		while (it.hasNext()) {
			polygon.coordinates.add(it.next());
		}

		polygon.coordinates.add(lineString);
		return polygon;
	}

	@Override
	public String getType() {
		return TYPE;
	}

	@Override
	public List<GeoJsonLineString> getCoordinates() {
		return Collections.unmodifiableList(this.coordinates);
	}

	private static List<Point> asList(Point first, Point second, Point third, Point fourth, Point... others) {

		ArrayList<Point> result = new ArrayList<Point>(3 + others.length);

		result.add(first);
		result.add(second);
		result.add(third);
		result.add(fourth);
		result.addAll(Arrays.asList(others));

		return result;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		GeoJsonPolygon that = (GeoJsonPolygon) o;

		return ObjectUtils.nullSafeEquals(this.coordinates, that.coordinates);
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + ObjectUtils.nullSafeHashCode(coordinates);
		return result;
	}
}
