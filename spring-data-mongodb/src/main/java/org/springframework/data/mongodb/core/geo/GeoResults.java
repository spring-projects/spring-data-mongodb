/*
 * Copyright 2011-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *			http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.geo;

import java.util.List;

import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Metric;

/**
 * Value object to capture {@link GeoResult}s as well as the average distance they have.
 * 
 * @deprecated As of release 1.5, replaced by {@link org.springframework.data.geo.GeoResults}. This class is scheduled
 *             to be removed in the next major release.
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@Deprecated
public class GeoResults<T> extends org.springframework.data.geo.GeoResults<T> {

	/**
	 * Creates a new {@link GeoResults} instance manually calculating the average distance from the distance values of the
	 * given {@link GeoResult}s.
	 * 
	 * @param results must not be {@literal null}.
	 */
	public GeoResults(List<? extends GeoResult<T>> results) {
		super(results);
	}

	public GeoResults(List<? extends GeoResult<T>> results, Metric metric) {
		super(results, metric);
	}

	/**
	 * Creates a new {@link GeoResults} instance from the given {@link GeoResult}s and average distance.
	 * 
	 * @param results must not be {@literal null}.
	 * @param averageDistance
	 */
	@PersistenceConstructor
	public GeoResults(List<? extends GeoResult<T>> results, Distance averageDistance) {
		super(results, averageDistance);
	}
}
