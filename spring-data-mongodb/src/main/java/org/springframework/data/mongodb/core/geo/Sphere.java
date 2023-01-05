/*
 * Copyright 2014-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.geo;

import java.util.Arrays;
import java.util.List;

import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Shape;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Represents a geospatial sphere value.
 *
 * @author Thomas Darimont
 * @author Mark Paluch
 * @since 1.5
 */
public class Sphere implements Shape {

	public static final String COMMAND = "$centerSphere";
	private final Point center;
	private final Distance radius;

	/**
	 * Creates a Sphere around the given center {@link Point} with the given radius.
	 *
	 * @param center must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 */
	@PersistenceCreator
	public Sphere(Point center, Distance radius) {

		Assert.notNull(center, "Center point must not be null");
		Assert.notNull(radius, "Radius must not be null");
		Assert.isTrue(radius.getValue() >= 0, "Radius must not be negative");

		this.center = center;
		this.radius = radius;
	}

	/**
	 * Creates a Sphere around the given center {@link Point} with the given radius.
	 *
	 * @param center must not be {@literal null}.
	 * @param radius
	 */
	public Sphere(Point center, double radius) {
		this(center, new Distance(radius));
	}

	/**
	 * Creates a Sphere from the given {@link Circle}.
	 *
	 * @param circle must not be {@literal null}.
	 */
	public Sphere(Circle circle) {
		this(circle.getCenter(), circle.getRadius());
	}

	/**
	 * Returns the center of the {@link Circle}.
	 *
	 * @return will never be {@literal null}.
	 */
	public Point getCenter() {
		return new Point(this.center);
	}

	/**
	 * Returns the radius of the {@link Circle}.
	 *
	 * @return never {@literal null}.
	 */
	public Distance getRadius() {
		return radius;
	}

	@Override
	public String toString() {
		return String.format("Sphere [center=%s, radius=%s]", center, radius);
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Sphere other)) {
			return false;
		}

		return this.center.equals(other.center) && this.radius.equals(other.radius);
	}

	@Override
	public int hashCode() {
		int result = 17;
		result += 31 * center.hashCode();
		result += 31 * radius.hashCode();
		return result;
	}

	/**
	 * Returns the {@link Shape} as a list of usually {@link Double} or {@link List}s of {@link Double}s. Wildcard bound
	 * to allow implementations to return a more concrete element type.
	 *
	 * @return never {@literal null}.
	 */
	public List<? extends Object> asList() {
		return Arrays.asList(Arrays.asList(center.getX(), center.getY()), this.radius.getValue());
	}

	/**
	 * Returns the command to be used to create the {@literal $within} criterion.
	 *
	 * @return never {@literal null}.
	 */
	public String getCommand() {
		return COMMAND;
	}
}
