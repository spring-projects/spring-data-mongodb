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
package org.springframework.data.mongodb.core.convert;

import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Thomas Darimont
 */
public enum GeoCommandUtils {

	INSTANCE;

	/**
	 * Wraps the given {@link Shape} in an appropriate MongoDB command.
	 * 
	 * @param shape must not be {@literal null}.
	 * @return
	 */
	public DBObject wrapInCommand(Shape shape) {

		Assert.notNull(shape, "Shape must not be null!");

		return new BasicDBObject(getCommand(shape), shape);
	}

	/**
	 * Returns the MongoDB command for the given {@link Shape}.
	 * 
	 * @param shape must not be {@literal null}.
	 * @return
	 */
	private String getCommand(Shape shape) {

		Assert.notNull(shape, "Shape must not be null!");

		if (shape instanceof Box) {
			return org.springframework.data.mongodb.core.geo.Box.COMMAND;
		} else if (shape instanceof Circle || shape instanceof org.springframework.data.mongodb.core.geo.Circle) {
			return org.springframework.data.mongodb.core.geo.Circle.COMMAND;
		} else if (shape instanceof Polygon) {
			return org.springframework.data.mongodb.core.geo.Polygon.COMMAND;
		} else if (shape instanceof Sphere) {
			return org.springframework.data.mongodb.core.geo.Sphere.COMMAND;
		}

		throw new IllegalArgumentException("Unknown shape: " + shape);
	}
}
