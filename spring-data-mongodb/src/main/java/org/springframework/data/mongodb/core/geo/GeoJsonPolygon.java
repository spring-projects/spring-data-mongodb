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
import org.springframework.data.geo.Polygon;
import org.springframework.util.ObjectUtils;

/**
 * {@link GeoJson} representation of {@link Polygon}. Unlike {@link Polygon} the {@link GeoJsonPolygon} requires a
 * closed border. Which means that the first and last {@link Point} have to have same coordinate pairs.
 * 
 * @author Christoph Strobl
 * @since 1.7
 * @see http://geojson.org/geojson-spec.html#polygon
 */
public class GeoJsonPolygon extends Polygon implements GeoJson<List<GeoJsonLineString>> {

	private static final long serialVersionUID = 3936163018187247185L;
	private static final String TYPE = "Polygon";
	private List<GeoJsonLineString> coordinates = new ArrayList<GeoJsonLineString>();

	/**
	 * Creates new {@link GeoJsonPolygon}.
	 * 
	 * @param p0 must not be {@literal null}.
	 * @param p1 must not be {@literal null}.
	 * @param p2 must not be {@literal null}.
	 * @param p3 must not be {@literal null}.
	 * @param others can be {@literal null}.
	 */
	public GeoJsonPolygon(Point p0, Point p1, Point p2, Point p3, final Point... others) {

		this(new ArrayList<Point>(Arrays.asList(p0, p1, p2, p3)) {
			private static final long serialVersionUID = 3143657022446395361L;

			{
				if (!ObjectUtils.isEmpty(others)) {
					for (Point p : others) {
						add(p);
					}
				}
			}
		});
	}

	/**
	 * Creates new {@link GeoJsonPolygon}.
	 * 
	 * @param points must not be {@literal null}.
	 */
	public GeoJsonPolygon(List<Point> points) {

		super(points);
		this.coordinates.add(new GeoJsonLineString(points));
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
	public List<GeoJsonLineString> getCoordinates() {
		return Collections.unmodifiableList(this.coordinates);
	}
}
