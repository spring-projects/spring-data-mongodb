/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

/**
 * Geoposatial index type.
 *
 * @author Laurent Canet
 * @author Oliver Gierke
 * @since 1.4
 */
public enum GeoSpatialIndexType {

	/**
	 * Simple 2-Dimensional index for legacy-format points.
	 */
	GEO_2D,

	/**
	 * 2D Index for GeoJSON-formatted data over a sphere. Only available in Mongo 2.4.
	 */
	GEO_2DSPHERE,

	/**
	 * An haystack index for grouping results over small results.
	 */
	GEO_HAYSTACK
}
