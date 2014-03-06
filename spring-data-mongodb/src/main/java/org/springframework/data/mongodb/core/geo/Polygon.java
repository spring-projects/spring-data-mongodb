/*
 * Copyright 2011-2014 the original author or authors.
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
import java.util.List;

import org.springframework.data.geo.Point;

/**
 * Simple value object to represent a {@link Polygon}.
 * 
 * @deprecated As of release 1.5, replaced by {@link org.springframework.data.geo.Point}. This class is scheduled to be
 *             removed in the next major release.
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@Deprecated
public class Polygon extends org.springframework.data.geo.Polygon implements Shape {

	/**
	 * Creates a new {@link Polygon} for the given Points.
	 * 
	 * @param x
	 * @param y
	 * @param z
	 * @param others
	 */
	public <P extends Point> Polygon(P x, P y, P z, P... others) {
		super(x, y, z, others);
	}

	/**
	 * Creates a new {@link Polygon} for the given Points.
	 * 
	 * @param points
	 */
	public <P extends Point> Polygon(List<P> points) {
		super(points);
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
	 * @see org.springframework.data.mongodb.core.geo.Shape#asList()
	 */
	@Override
	public List<? extends Object> asList() {

		List<Point> points = getPoints();
		List<List<Double>> tuples = new ArrayList<List<Double>>(points.size());

		for (Point point : points) {
			tuples.add(Arrays.asList(point.getX(), point.getY()));
		}

		return tuples;
	}
}
