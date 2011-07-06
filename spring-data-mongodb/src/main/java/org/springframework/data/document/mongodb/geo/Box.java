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
package org.springframework.data.document.mongodb.geo;

import org.springframework.data.document.mongodb.mapping.Field;
import org.springframework.util.Assert;

/**
 * Represents a geospatial box value
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class Box {

	@Field(order = 10)
	private final Point first;
	@Field(order = 20)
	private final Point second;

	public Box(Point lowerLeft, Point upperRight) {
		Assert.notNull(lowerLeft);
		Assert.notNull(upperRight);
		this.first = lowerLeft;
		this.second = upperRight;
	}

	public Box(double[] lowerLeft, double[] upperRight) {
		Assert.isTrue(lowerLeft.length == 2, "Point array has to have 2 elements!");
		Assert.isTrue(upperRight.length == 2, "Point array has to have 2 elements!");
		this.first = new Point(lowerLeft[0], lowerLeft[1]);
		this.second = new Point(upperRight[0], upperRight[1]);
	}

	public Point getLowerLeft() {
		return first;
	}

	public Point getUpperRight() {
		return second;
	}

	@Override
	public String toString() {
		return String.format("Box [%s, %s]", first, second);
	}

	@Override
	public int hashCode() {

		int result = 31;
		result += 17 * first.hashCode();
		result += 17 * second.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Box that = (Box) obj;
		return this.first.equals(that.first) && this.second.equals(that.second);
	}
}
