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
import java.util.Collections;
import java.util.List;

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * {@link GeoJsonMultiPolygon} is defined as a list of {@link GeoJsonPolygon}s.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class GeoJsonMultiPolygon implements GeoJson<Iterable<GeoJsonPolygon>> {

	private static final String TYPE = "MultiPolygon";

	private List<GeoJsonPolygon> coordinates = new ArrayList<GeoJsonPolygon>();

	/**
	 * Creates a new {@link GeoJsonMultiPolygon} for the given {@link GeoJsonPolygon}s.
	 * 
	 * @param polygons must not be {@literal null}.
	 */
	public GeoJsonMultiPolygon(List<GeoJsonPolygon> polygons) {

		Assert.notNull(polygons, "Polygons for MultiPolygon must not be null!");

		this.coordinates.addAll(polygons);
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
	public List<GeoJsonPolygon> getCoordinates() {
		return Collections.unmodifiableList(this.coordinates);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(this.coordinates);
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

		if (!(obj instanceof GeoJsonMultiPolygon)) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(this.coordinates, ((GeoJsonMultiPolygon) obj).coordinates);
	}
}
