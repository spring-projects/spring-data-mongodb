/*
 * Copyright 2011 the original author or authors.
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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Value object to capture {@link GeoResult}s as well as the average distance they have.
 * 
 * @author Oliver Gierke
 */
public class GeoResults<T> implements Iterable<GeoResult<T>> {

	private final List<GeoResult<T>> results;
	private final Distance averageDistance;

	/**
	 * Creates a new {@link GeoResults} instance manually calculating the average distance from the distance values of the
	 * given {@link GeoResult}s.
	 * 
	 * @param results must not be {@literal null}.
	 */
	public GeoResults(List<GeoResult<T>> results) {
		this(results, (Metric) null);
	}
	
	public GeoResults(List<GeoResult<T>> results, Metric metric) {
		this(results, calculateAverageDistance(results, metric));
	}

	/**
	 * Creates a new {@link GeoResults} instance from the given {@link GeoResult}s and average distance.
	 * 
	 * @param results must not be {@literal null}.
	 * @param averageDistance
	 */
	@PersistenceConstructor
	public GeoResults(List<GeoResult<T>> results, Distance averageDistance) {
		Assert.notNull(results);
		this.results = results;
		this.averageDistance = averageDistance;
	}

	/**
	 * @return the averageDistance
	 */
	public Distance getAverageDistance() {
		return averageDistance;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<GeoResult<T>> iterator() {
		return results.iterator();
	}

	/**
	 * Returns the actual
	 * 
	 * @return
	 */
	public List<GeoResult<T>> getContent() {
		return Collections.unmodifiableList(results);
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

		if (obj == null || !getClass().equals(obj.getClass())) {
			return false;
		}

		GeoResults<?> that = (GeoResults<?>) obj;

		return this.results.equals(that.results) && this.averageDistance == that.averageDistance;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = 17;
		result += 31 * results.hashCode();
		result += 31 * averageDistance.hashCode();
		return result;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("GeoResults: [averageDistance: %s, results: %s]", averageDistance.toString(),
				StringUtils.collectionToCommaDelimitedString(results));
	}

	private static Distance calculateAverageDistance(List<? extends GeoResult<?>> results, Metric metric) {

		if (results.isEmpty()) {
			return new Distance(0, null);
		}

		double averageDistance = 0;

		for (GeoResult<?> result : results) {
			averageDistance += result.getDistance().getValue();
		}

		return new Distance(averageDistance / results.size(), metric);
	}
}
