/*
 * Copyright 2015-2017 the original author or authors.
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

/**
 * Interface definition for structures defined in GeoJSON ({@link http://geojson.org/}) format.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public interface GeoJson<T extends Iterable<?>> {

	/**
	 * String value representing the type of the {@link GeoJson} object.
	 * 
	 * @return will never be {@literal null}.
	 * @see <a href="http://geojson.org/geojson-spec.html#geojson-objects">http://geojson.org/geojson-spec.html#geojson-objects</a>
	 */
	String getType();

	/**
	 * The value of the coordinates member is always an {@link Iterable}. The structure for the elements within is
	 * determined by {@link #getType()} of geometry.
	 * 
	 * @return will never be {@literal null}.
	 * @see <a href="http://geojson.org/geojson-spec.html#geometry-objects">http://geojson.org/geojson-spec.html#geometry-objects</a>
	 */
	T getCoordinates();
}
