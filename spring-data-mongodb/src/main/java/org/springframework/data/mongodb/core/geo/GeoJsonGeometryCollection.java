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
import java.util.List;

/**
 * Defines a {@link GeoJsonGeometryCollection} that consists of a {@link List} of {@link GeoJson} objects.
 * 
 * @author Christoph Strobl
 * @since 1.7
 * @see http://geojson.org/geojson-spec.html#geometry-collection
 */
public class GeoJsonGeometryCollection implements GeoJson<Iterable<GeoJson<?>>> {

	private static final String TYPE = "GeometryCollection";
	private final List<GeoJson<?>> geometries = new ArrayList<GeoJson<?>>();

	/**
	 * @param geometries
	 */
	public GeoJsonGeometryCollection(List<GeoJson<?>> geometries) {

		notNull(geometries);
		this.geometries.addAll(geometries);
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
	public Iterable<GeoJson<?>> getCoordinates() {
		return unmodifiableList(this.geometries);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return nullSafeHashCode(this.geometries);
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
		if (!(obj instanceof GeoJsonGeometryCollection)) {
			return false;
		}
		GeoJsonGeometryCollection other = (GeoJsonGeometryCollection) obj;
		return nullSafeEquals(this.geometries, other.geometries);
	}

}
