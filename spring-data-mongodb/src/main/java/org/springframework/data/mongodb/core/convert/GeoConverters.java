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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Wrapper class to contain useful geo structure converters for the usage with Mongo.
 * 
 * @author Thomas Darimont
 * @since 1.5
 */
abstract class GeoConverters {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private GeoConverters() {}

	/**
	 * Converts a {@link List} of {@link Double}s into a {@link Point}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	public static enum ListToPointConverter implements Converter<List<Double>, Point> {

		INSTANCE;

		@SuppressWarnings("deprecation")
		public Point convert(List<Double> source) {

			Assert.notEmpty(source, "Source must not be empty!");
			Assert.isTrue(source.size() == 2, "Source must contain 2 elements");

			return source == null ? null : new org.springframework.data.mongodb.core.geo.Point(source.get(0), source.get(1));
		}
	}

	/**
	 * Converts a {@link Point} into a {@link List} of {@link Double}s.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	public static enum PointToListConverter implements Converter<Point, List<Double>> {

		INSTANCE;

		@Override
		public List<Double> convert(Point source) {
			return source == null ? null : Arrays.asList(source.getX(), source.getY());
		}
	}

	/**
	 * Converts a {@link Box} into a {@link DBObject}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@WritingConverter
	public static enum BoxToDbObjectConverter implements Converter<Box, DBObject> {

		INSTANCE;

		@Override
		@SuppressWarnings({ "unchecked", "deprecation" })
		public DBObject convert(Box source) {

			if (source == null) {
				return null;
			}

			return new BasicDBObject(org.springframework.data.mongodb.core.geo.Box.COMMAND, Arrays.asList(
					toList(source.getFirst()), toList(source.getSecond())));
		}
	}

	/**
	 * Converts a {@link DBObject} into a {@link org.springframework.data.mongodb.core.geo.Box}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	public static enum DbObjectToBoxConverter implements Converter<DBObject, Box> {

		INSTANCE;

		@SuppressWarnings("deprecation")
		@Override
		public Box convert(DBObject source) {

			if (source == null) {
				return null;
			}

			List<?> list = (List<?>) source.get(org.springframework.data.mongodb.core.geo.Box.COMMAND);

			return new org.springframework.data.mongodb.core.geo.Box(toPoint(list.get(0)), toPoint(list.get(1)));
		}
	}

	/**
	 * Converts a {@link Circle} into a {@link DBObject}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	public static enum CircleToDbObjectConverter implements Converter<Circle, DBObject> {

		INSTANCE;

		@SuppressWarnings("deprecation")
		@Override
		public DBObject convert(Circle source) {

			if (source == null) {
				return null;
			}

			return new BasicDBObject(org.springframework.data.mongodb.core.geo.Circle.COMMAND, Arrays.asList(
					toList(source.getCenter()), source.getRadius().getNormalizedValue()));
		}
	}

	/**
	 * Converts a {@link DBObject} into a {@link org.springframework.data.mongodb.core.geo.Circle}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	public static enum DbObjectToCircleConverter implements Converter<DBObject, Circle> {

		INSTANCE;

		@SuppressWarnings("deprecation")
		@Override
		public Circle convert(DBObject source) {

			if (source == null) {
				return null;
			}

			List<?> list = (List<?>) source.get(org.springframework.data.mongodb.core.geo.Circle.COMMAND);

			return new Circle(toPoint(list.get(0)), (Double) list.get(1));
		}
	}

	/**
	 * Converts a {@link Circle} into a {@link DBObject}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@SuppressWarnings("deprecation")
	public static enum LegacyCircleToDbObjectConverter implements
			Converter<org.springframework.data.mongodb.core.geo.Circle, DBObject> {

		INSTANCE;

		@Override
		public DBObject convert(org.springframework.data.mongodb.core.geo.Circle source) {

			if (source == null) {
				return null;
			}

			return new BasicDBObject(org.springframework.data.mongodb.core.geo.Circle.COMMAND, Arrays.asList(
					toList(source.getCenter()), source.getRadius()));
		}
	}

	/**
	 * Converts a {@link DBObject} into a {@link org.springframework.data.mongodb.core.geo.Circle}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	@SuppressWarnings("deprecation")
	public static enum DbObjectToLegacyCircleConverter implements
			Converter<DBObject, org.springframework.data.mongodb.core.geo.Circle> {

		INSTANCE;

		@Override
		public org.springframework.data.mongodb.core.geo.Circle convert(DBObject source) {

			if (source == null) {
				return null;
			}

			List<?> list = (List<?>) source.get(org.springframework.data.mongodb.core.geo.Circle.COMMAND);

			return new org.springframework.data.mongodb.core.geo.Circle(toPoint(list.get(0)), (Double) list.get(1));
		}
	}

	/**
	 * Converts a {@link Sphere} into a {@link DBObject}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	public static enum SphereToDbObjectConverter implements Converter<Sphere, DBObject> {

		INSTANCE;

		@Override
		public DBObject convert(Sphere source) {

			if (source == null) {
				return null;
			}

			return new BasicDBObject(Sphere.COMMAND, Arrays.asList(toList(source.getCenter()), source.getRadius()
					.getNormalizedValue()));
		}
	}

	/**
	 * Converts a {@link DBObject} into a {@link Sphere}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	public static enum DbObjectToSphereConverter implements Converter<DBObject, Sphere> {

		INSTANCE;

		@Override
		public Sphere convert(DBObject source) {

			if (source == null) {
				return null;
			}

			List<?> list = (List<?>) source.get(Sphere.COMMAND);

			return new Sphere(toPoint(list.get(0)), (Double) list.get(1));
		}
	}

	/**
	 * Converts a {@link Polygon} into a {@link DBObject}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	public static enum PolygonToDbObjectConverter implements Converter<Polygon, DBObject> {

		INSTANCE;

		@Override
		public DBObject convert(Polygon source) {

			if (source == null) {
				return null;
			}

			List<Point> points = source.getPoints();
			List<List<Double>> pointTuples = new ArrayList<List<Double>>(points.size());
			for (Point point : points) {
				pointTuples.add(toList(point));
			}

			return new BasicDBObject("$polygon", pointTuples);
		}
	}

	/**
	 * Converts a {@link DBObject} into a {@link org.springframework.data.mongodb.core.geo.Polygon}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	public static enum DbObjectToPolygonConverter implements Converter<DBObject, Polygon> {

		INSTANCE;

		@SuppressWarnings("deprecation")
		@Override
		public Polygon convert(DBObject source) {

			if (source == null) {
				return null;
			}

			List<?> list = (List<?>) source.get("$polygon");

			List<Point> points = new ArrayList<Point>(list.size());
			for (Object element : list) {

				Assert.notNull(element, "point elements of polygon must not be null!");

				points.add(toPoint(element));
			}

			return new org.springframework.data.mongodb.core.geo.Polygon(points);
		}
	}

	/**
	 * Converts the given item into a {@link Point}.
	 * 
	 * @param item
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static Point toPoint(Object item) {

		Assert.isInstanceOf(List.class, item);

		return ListToPointConverter.INSTANCE.convert((List<Double>) item);
	}

	/**
	 * Converts the given {@link Point} into a {@link List} representation with X,Y coordinates as {@link Double}s.
	 * 
	 * @param point
	 * @return
	 */
	private static List<Double> toList(Point point) {
		return PointToListConverter.INSTANCE.convert(point);
	}
}
