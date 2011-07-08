/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.mongodb.geo;

import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.util.Assert;

/**
 * Represents a geospatial circle value
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class Circle {

	private Point center;
	private double radius;

	@PersistenceConstructor
	public Circle(Point center, double radius) {
		Assert.notNull(center);
		Assert.isTrue(radius >= 0, "Radius must not be negative!");
		this.center = center;
		this.radius = radius;
	}

	public Circle(double centerX, double centerY, double radius) {
		this(new Point(centerX, centerY), radius);
	}

	public Point getCenter() {
		return center;
	}

	public double getRadius() {
		return radius;
	}

	@Override
	public String toString() {
		return String.format("Circle [center=%s, radius=%d]", center, radius);
	}
}
