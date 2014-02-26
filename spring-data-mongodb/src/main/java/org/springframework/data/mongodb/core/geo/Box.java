/*
 * Copyright 2010-2014 the original author or authors.
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
 * Represents a geospatial box value
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@Deprecated
public class Box extends org.springframework.data.geo.Box implements Shape {

	public Box(Point lowerLeft, Point upperRight) {
		super(lowerLeft, upperRight);
	}

	public Box(double[] lowerLeft, double[] upperRight) {
		super(lowerLeft, upperRight);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.geo.Shape#asList()
	 */
	public List<? extends Object> asList() {

		List<List<Double>> list = new ArrayList<List<Double>>();

		list.add(Arrays.asList(getFirst().getX(), getFirst().getY()));
		list.add(Arrays.asList(getSecond().getX(), getSecond().getY()));

		return list;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.geo.Shape#getCommand()
	 */
	public String getCommand() {
		return "$box";
	}
}
