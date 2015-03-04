/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import static org.springframework.util.Assert.*;
import static org.springframework.util.ObjectUtils.*;

import org.springframework.data.geo.Point;

/**
 * {@link NearCommand} used to define geo spatial criterion parameter for constructing {@literal $near} and
 * {@literal $nearSphere} commands.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class NearCommand {

	private final Point coordinates;
	private double minDistance;
	private double maxDistance;

	/**
	 * Creates new {@link NearCommand}.
	 * 
	 * @param point must not be {@literal null}.
	 */
	public NearCommand(Point point) {

		notNull(point, "Point for NearCommand must not be null!");
		this.coordinates = point;
	}

	/**
	 * Creates new {@link NearCommand}.
	 * 
	 * @param must not be {@literal null}.
	 * @param maxDistance
	 */
	public NearCommand(Point o, double maxDistance) {

		this(o);
		this.maxDistance = maxDistance;
	}

	/**
	 * Get center coordinates
	 * 
	 * @return
	 */
	public Point getCoordinates() {
		return coordinates;
	}

	/**
	 * Get maxDistance
	 * 
	 * @return
	 */
	public double getMaxDistance() {
		return maxDistance;
	}

	/**
	 * @param maxDistance
	 */
	public void setMaxDistance(double maxDistance) {
		this.maxDistance = maxDistance;
	}

	/**
	 * @param minDistance
	 */
	public void setMinDistance(double minDistance) {
		this.minDistance = minDistance;
	}

	@Override
	public int hashCode() {

		int result = nullSafeHashCode(coordinates);
		result += nullSafeHashCode(maxDistance);
		result += nullSafeHashCode(minDistance);
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
		if (!(obj instanceof NearCommand)) {
			return false;
		}
		NearCommand that = (NearCommand) obj;
		return nullSafeEquals(this.coordinates, that.coordinates) && nullSafeEquals(this.maxDistance, that.maxDistance)
				&& nullSafeEquals(this.minDistance, that.minDistance);
	}

}
