/*
 * Copyright 2011 the original author or authors.
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
import java.util.Iterator;
import java.util.List;

import org.springframework.util.Assert;

/**
 * Simple value object to represent a {@link Polygon}.
 * 
 * @author Oliver Gierke
 */
public class Polygon implements Shape, Iterable<Point> {

	private final List<Point> points;

	/**
	 * Creates a new {@link Polygon} for the given Points.
	 * 
	 * @param x
	 * @param y
	 * @param z
	 * @param others
	 */
	public Polygon(Point x, Point y, Point z, Point... others) {

		Assert.notNull(x);
		Assert.notNull(y);
		Assert.notNull(z);
		Assert.notNull(others);

		this.points = new ArrayList<Point>(3 + others.length);
		this.points.addAll(Arrays.asList(x, y, z));
		this.points.addAll(Arrays.asList(others));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.geo.Shape#asList()
	 */
	public List<List<Double>> asList() {

		List<List<Double>> result = new ArrayList<List<Double>>();

		for (Point point : points) {
			result.add(point.asList());
		}

		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.geo.Shape#getCommand()
	 */
	public String getCommand() {
		return "$polygon";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<Point> iterator() {
		return this.points.iterator();
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

		Polygon that = (Polygon) obj;

		return this.points.equals(that.points);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return points.hashCode();
	}
}
