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

import java.util.List;

import org.springframework.data.geo.Point;

/**
 * {@link GeoJsonLineString} is defined as list of at least 2 {@link Point}s.
 * 
 * @author Christoph Strobl
 * @since 1.7
 * @see http://geojson.org/geojson-spec.html#linestring
 */
public class GeoJsonLineString extends GeoJsonMultiPoint {

	private static final String TYPE = "LineString";

	/**
	 * Creates a new {@link GeoJsonLineString} for the given {@link Point}s.
	 * 
	 * @param points must not be {@literal null} and have at least 2 entries.
	 */
	public GeoJsonLineString(List<Point> points) {
		super(points);
	}

	/**
	 * Creates a new {@link GeoJsonLineString} for the given {@link Point}s.
	 * 
	 * @param first must not be {@literal null}
	 * @param second must not be {@literal null}
	 * @param others can be {@literal null}
	 */
	public GeoJsonLineString(Point first, Point second, Point... others) {
		super(first, second, others);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.geo.GeoJsonMultiPoint#getType()
	 */
	@Override
	public String getType() {
		return TYPE;
	}
}
