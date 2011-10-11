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
package org.springframework.data.mongodb.core.geo;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.util.Assert;

/**
 * Represents a geospatial circle value
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class Circle implements Shape {

	private final Point center;
	private final double radius;

	/**
	 * Creates a new {@link Circle} from the given {@link Point} and radius.
	 * 
	 * @param center must not be {@literal null}.
	 * @param radius must be greater or equal to zero.
	 */
	@PersistenceConstructor
	public Circle(Point center, double radius) {
		
		Assert.notNull(center);
		Assert.isTrue(radius >= 0, "Radius must not be negative!");
		
		this.center = center;
		this.radius = radius;
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

	/**
	 * Returns the center of the {@link Circle}.
	 * 
	 * @return will never be {@literal null}.
	 */
	public Point getCenter() {
		return center;
	}

	/**
	 * Returns the radius of the {@link Circle}.
	 * 
	 * @return
	 */
	public double getRadius() {
		return radius;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.geo.Shape#asList()
	 */
	public List<Object> asList() {
		List<Object> result = new ArrayList<Object>();
		result.add(getCenter().asList());
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

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("Circle [center=%s, radius=%f]", center, radius);
	}
	
	/* (non-Javadoc)
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
		
		Circle that = (Circle) obj;
		
		return this.center.equals(that.center) && this.radius == that.radius;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = 17;
		result += 31 * center.hashCode();
		result += 31 * radius;
		return result;
	}
}
