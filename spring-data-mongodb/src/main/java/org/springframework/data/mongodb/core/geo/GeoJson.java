/*
 * Copyright 2015 the original author or authors.
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

import org.springframework.data.geo.Box;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * {@link GeoJson} provides a wrapper around geometric structures like {@link Point},...
 * 
 * @author Christoph Strobl
 * @param <T>
 * @since 1.7
 */
public class GeoJson<T> {

	private final T geometry;

	/**
	 * Creates new {@link GeoJson}.
	 * 
	 * @param geometry
	 */
	public GeoJson(T geometry) {

		Assert.notNull(geometry, "Geometry to construct GeoJson must not be null!");
		this.geometry = geometry;
	}

	/**
	 * Get the underlying geometry object.
	 * 
	 * @return
	 */
	public T getGeometry() {
		return geometry;
	}

	/**
	 * Creates {@link GeoJson} for {@link Point}.
	 * 
	 * @param point must not be {@literal null}.
	 * @return
	 */
	public static GeoJson<Point> point(Point point) {
		return new GeoJson<Point>(point);
	}

	/**
	 * Creates {@link GeoJson} for {@link Point} with given {@code x,y} coordinates.
	 * 
	 * @param x
	 * @param y
	 * @return
	 */
	public static GeoJson<Point> point(double x, double y) {
		return new GeoJson<Point>(new Point(x, y));
	}

	/**
	 * Creates {@link GeoJson} for given {@link Box}.
	 * 
	 * @param box must not be {@literal null}.
	 * @return
	 */
	public static GeoJson<Polygon> polygon(Box box) {

		Point p0 = box.getFirst();
		Point p2 = box.getSecond();

		Point p1 = new Point(p0.getX(), p2.getY());
		Point p3 = new Point(p2.getX(), p0.getY());

		return polygon(p0, p1, p2, p3, p0);
	}

	/**
	 * Creates {@link GeoJson} for given {@link Polygon}.
	 * 
	 * @param polygon must not be {@literal null}.
	 * @return
	 */
	public static GeoJson<Polygon> polygon(Polygon polygon) {
		return new GeoJson<Polygon>(polygon);
	}

	/**
	 * Creates {@link GeoJson} for {@link Polygon} defined by given {@link Point}s.
	 * 
	 * @param p0 must not be {@literal null}.
	 * @param p1 must not be {@literal null}.
	 * @param p2 must not be {@literal null}.
	 * @param p3 must not be {@literal null}.
	 * @param others
	 * @return
	 */
	public static GeoJson<Polygon> polygon(Point p0, Point p1, Point p2, Point p3, Point... others) {

		List<Point> points = new ArrayList<Point>(Arrays.asList(p0, p1, p2, p3));
		if (!ObjectUtils.isEmpty(others)) {
			points.addAll(Arrays.asList(others));
		}
		return new GeoJson<Polygon>(new Polygon(points));
	}

	/**
	 * Creates {@link GeoJson} for {@link Polygon} defined by given {@link Point}s.
	 * 
	 * @param p0 must not be {@literal null}.
	 * @param p1 must not be {@literal null}.
	 * @param p2 must not be {@literal null}.
	 * @return
	 */
	public static GeoJson<Polygon> triangle(Point p0, Point p1, Point p2) {
		return polygon(p0, p1, p2, p0);
	}

	/**
	 * Creates {@link GeoJson} for {@link Polygon} defined by given {@link Point}s.
	 * 
	 * @param p0 must not be {@literal null}.
	 * @param p1 must not be {@literal null}.
	 * @param p2 must not be {@literal null}.
	 * @param p3 must not be {@literal null}.
	 * @param others
	 * @return
	 */
	public static GeoJson<Polygon> rectangle(Point p0, Point p1, Point p2, Point p3) {
		return polygon(p0, p1, p2, p3, p0);
	}

}
