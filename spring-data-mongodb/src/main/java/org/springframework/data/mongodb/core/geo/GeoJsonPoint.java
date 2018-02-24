/*
 * Copyright 2015-2018 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.springframework.data.geo.Point;

/**
 * {@link GeoJson} representation of {@link Point}.
 *
 * @author Christoph Strobl
 * @since 1.7
 * @see <a href="http://geojson.org/geojson-spec.html#point">http://geojson.org/geojson-spec.html#point</a>
 */
public class GeoJsonPoint extends Point implements GeoJson<List<Double>> {

	private static final long serialVersionUID = -8026303425147474002L;

	private static final String TYPE = "Point";

	/**
	 * Creates {@link GeoJsonPoint} for given coordinates.
	 *
	 * @param x
	 * @param y
	 */
	public GeoJsonPoint(double x, double y) {
		super(x, y);
	}

	/**
	 * Creates {@link GeoJsonPoint} for given {@link Point}.
	 *
	 * @param point must not be {@literal null}.
	 */
	public GeoJsonPoint(Point point) {
		super(point);
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
	public List<Double> getCoordinates() {
		return Arrays.asList(Double.valueOf(getX()), Double.valueOf(getY()));
	}
}
