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

import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.geo.Point;

/**
 * Represents a geospatial circle value
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@Deprecated
public class Circle extends org.springframework.data.geo.Circle implements Shape {

	/**
	 * Creates a new {@link Circle} from the given {@link Point} and radius.
	 * 
	 * @param center must not be {@literal null}.
	 * @param radius must be greater or equal to zero.
	 */
	@PersistenceConstructor
	public Circle(Point center, double radius) {
		super(center, radius);
	}

	/**
	 * Creates a new {@link Circle} from the given coordinates and radius.
	 * 
	 * @param centerX
	 * @param centerY
	 * @param radius must be greater or equal to zero.
	 */
	public Circle(double centerX, double centerY, double radius) {
		this(new Point(centerX, centerY), radius);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.geo.Shape#asList()
	 */
	public List<Object> asList() {

		List<Object> result = new ArrayList<Object>();
		result.add(Arrays.asList(getCenter().getX(), getCenter().getY()));
		result.add(getRadius());

		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.geo.Shape#getCommand()
	 */
	public String getCommand() {
		return "$center";
	}
}
