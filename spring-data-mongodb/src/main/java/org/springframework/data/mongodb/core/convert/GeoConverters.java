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
import java.util.Collection;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.data.mongodb.core.query.GeoCommand;
import org.springframework.util.Assert;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Wrapper class to contain useful geo structure converters for the usage with Mongo.
 * 
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @since 1.5
 */
abstract class GeoConverters {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private GeoConverters() {}

	/**
	 * Returns the geo converters to be registered.
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Collection<? extends Object> getConvertersToRegister() {
		return Arrays.asList( //
				BoxToDbObjectConverter.INSTANCE //
				, PolygonToDbObjectConverter.INSTANCE //
				, CircleToDbObjectConverter.INSTANCE //
				, LegacyCircleToDbObjectConverter.INSTANCE //
				, SphereToDbObjectConverter.INSTANCE //
				, DbObjectToBoxConverter.INSTANCE //
				, DbObjectToPolygonConverter.INSTANCE //
				, DbObjectToCircleConverter.INSTANCE //
				, DbObjectToLegacyCircleConverter.INSTANCE //
				, DbObjectToSphereConverter.INSTANCE //
				, DbObjectToPointConverter.INSTANCE //
				, PointToDbObjectConverter.INSTANCE //
				, GeoCommandToDbObjectConverter.INSTANCE);
	}

	/**
	 * Converts a {@link List} of {@link Double}s into a {@link Point}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	public static enum DbObjectToPointConverter implements Converter<DBObject, Point> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		@SuppressWarnings("deprecation")
		public Point convert(DBObject source) {

			Assert.isTrue(source.keySet().size() == 2, "Source must contain 2 elements");

			return source == null ? null : new org.springframework.data.mongodb.core.geo.Point((Double) source.get("x"),
					(Double) source.get("y"));
		}
	}

	/**
	 * Converts a {@link Point} into a {@link List} of {@link Double}s.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	public static enum PointToDbObjectConverter implements Converter<Point, DBObject> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public DBObject convert(Point source) {
			return source == null ? null : new BasicDBObject("x", source.getX()).append("y", source.getY());
		}
	}

	/**
	 * Converts a {@link Box} into a {@link BasicDBList}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@WritingConverter
	public static enum BoxToDbObjectConverter implements Converter<Box, DBObject> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public DBObject convert(Box source) {

			if (source == null) {
				return null;
			}

			BasicDBObject result = new BasicDBObject();
			result.put("first", PointToDbObjectConverter.INSTANCE.convert(source.getFirst()));
			result.put("second", PointToDbObjectConverter.INSTANCE.convert(source.getSecond()));
			return result;
		}
	}

	/**
	 * Converts a {@link BasicDBList} into a {@link org.springframework.data.mongodb.core.geo.Box}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	public static enum DbObjectToBoxConverter implements Converter<DBObject, Box> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		@SuppressWarnings("deprecation")
		public Box convert(DBObject source) {

			if (source == null) {
				return null;
			}

			Point first = DbObjectToPointConverter.INSTANCE.convert((DBObject) source.get("first"));
			Point second = DbObjectToPointConverter.INSTANCE.convert((DBObject) source.get("second"));

			return new org.springframework.data.mongodb.core.geo.Box(first, second);
		}
	}

	/**
	 * Converts a {@link Circle} into a {@link BasicDBList}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	public static enum CircleToDbObjectConverter implements Converter<Circle, DBObject> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public DBObject convert(Circle source) {

			if (source == null) {
				return null;
			}

			DBObject result = new BasicDBObject();
			result.put("center", PointToDbObjectConverter.INSTANCE.convert(source.getCenter()));
			result.put("radius", source.getRadius().getNormalizedValue());
			result.put("metric", source.getRadius().getMetric().toString());
			return result;
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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Circle convert(DBObject source) {

			if (source == null) {
				return null;
			}

			DBObject center = (DBObject) source.get("center");
			Double radius = (Double) source.get("radius");

			Distance distance = new Distance(radius);

			if (source.containsField("metric")) {

				String metricString = (String) source.get("metric");
				Assert.notNull(metricString, "Metric must not be null!");

				distance = distance.in(Metrics.valueOf(metricString));
			}

			Assert.notNull(center, "Center must not be null!");
			Assert.notNull(radius, "Radius must not be null!");

			return new Circle(DbObjectToPointConverter.INSTANCE.convert(center), distance);
		}
	}

	/**
	 * Converts a {@link Circle} into a {@link BasicDBList}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@SuppressWarnings("deprecation")
	public static enum LegacyCircleToDbObjectConverter implements
			Converter<org.springframework.data.mongodb.core.geo.Circle, DBObject> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public DBObject convert(org.springframework.data.mongodb.core.geo.Circle source) {

			if (source == null) {
				return null;
			}

			DBObject result = new BasicDBObject();
			result.put("center", PointToDbObjectConverter.INSTANCE.convert(source.getCenter()));
			result.put("radius", source.getRadius());
			return result;
		}
	}

	/**
	 * Converts a {@link BasicDBList} into a {@link org.springframework.data.mongodb.core.geo.Circle}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	@SuppressWarnings("deprecation")
	public static enum DbObjectToLegacyCircleConverter implements
			Converter<DBObject, org.springframework.data.mongodb.core.geo.Circle> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public org.springframework.data.mongodb.core.geo.Circle convert(DBObject source) {

			if (source == null) {
				return null;
			}

			DBObject centerSource = (DBObject) source.get("center");
			Double radius = (Double) source.get("radius");

			Assert.notNull(centerSource, "Center must not be null!");
			Assert.notNull(radius, "Radius must not be null!");

			Point center = DbObjectToPointConverter.INSTANCE.convert(centerSource);
			return new org.springframework.data.mongodb.core.geo.Circle(center, radius);
		}
	}

	/**
	 * Converts a {@link Sphere} into a {@link BasicDBList}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	public static enum SphereToDbObjectConverter implements Converter<Sphere, DBObject> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public DBObject convert(Sphere source) {

			if (source == null) {
				return null;
			}

			DBObject result = new BasicDBObject();
			result.put("center", PointToDbObjectConverter.INSTANCE.convert(source.getCenter()));
			result.put("radius", source.getRadius().getNormalizedValue());
			result.put("metric", source.getRadius().getMetric().toString());
			return result;
		}
	}

	/**
	 * Converts a {@link BasicDBList} into a {@link Sphere}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	public static enum DbObjectToSphereConverter implements Converter<DBObject, Sphere> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Sphere convert(DBObject source) {

			if (source == null) {
				return null;
			}

			DBObject center = (DBObject) source.get("center");
			Double radius = (Double) source.get("radius");

			Distance distance = new Distance(radius);

			if (source.containsField("metric")) {

				String metricString = (String) source.get("metric");
				Assert.notNull(metricString, "Metric must not be null!");

				distance = distance.in(Metrics.valueOf(metricString));
			}

			Assert.notNull(center, "Center must not be null!");
			Assert.notNull(radius, "Radius must not be null!");

			return new Sphere(DbObjectToPointConverter.INSTANCE.convert(center), distance);
		}
	}

	/**
	 * Converts a {@link Polygon} into a {@link BasicDBList}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	public static enum PolygonToDbObjectConverter implements Converter<Polygon, DBObject> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public DBObject convert(Polygon source) {

			if (source == null) {
				return null;
			}

			List<Point> points = source.getPoints();
			List<DBObject> pointTuples = new ArrayList<DBObject>(points.size());

			for (Point point : points) {
				pointTuples.add(PointToDbObjectConverter.INSTANCE.convert(point));
			}

			DBObject result = new BasicDBObject();
			result.put("points", pointTuples);
			return result;
		}
	}

	/**
	 * Converts a {@link BasicDBList} into a {@link org.springframework.data.mongodb.core.geo.Polygon}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	public static enum DbObjectToPolygonConverter implements Converter<DBObject, Polygon> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		@SuppressWarnings({ "deprecation", "unchecked" })
		public Polygon convert(DBObject source) {

			if (source == null) {
				return null;
			}

			List<DBObject> points = (List<DBObject>) source.get("points");
			List<Point> newPoints = new ArrayList<Point>(points.size());

			for (DBObject element : points) {

				Assert.notNull(element, "Point elements of polygon must not be null!");
				newPoints.add(DbObjectToPointConverter.INSTANCE.convert(element));
			}

			return new org.springframework.data.mongodb.core.geo.Polygon(newPoints);
		}
	}

	/**
	 * Converts a {@link Sphere} into a {@link BasicDBList}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	public static enum GeoCommandToDbObjectConverter implements Converter<GeoCommand, DBObject> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		@SuppressWarnings("deprecation")
		public DBObject convert(GeoCommand source) {

			if (source == null) {
				return null;
			}

			BasicDBList argument = new BasicDBList();

			Shape shape = source.getShape();

			if (shape instanceof Box) {

				argument.add(toList(((Box) shape).getFirst()));
				argument.add(toList(((Box) shape).getSecond()));

			} else if (shape instanceof Circle) {

				argument.add(toList(((Circle) shape).getCenter()));
				argument.add(((Circle) shape).getRadius().getNormalizedValue());

			} else if (shape instanceof org.springframework.data.mongodb.core.geo.Circle) {

				argument.add(toList(((org.springframework.data.mongodb.core.geo.Circle) shape).getCenter()));
				argument.add(((org.springframework.data.mongodb.core.geo.Circle) shape).getRadius());

			} else if (shape instanceof Polygon) {

				for (Point point : ((Polygon) shape).getPoints()) {
					argument.add(toList(point));
				}

			} else if (shape instanceof Sphere) {

				argument.add(toList(((Sphere) shape).getCenter()));
				argument.add(((Sphere) shape).getRadius().getNormalizedValue());
			}

			return new BasicDBObject(source.getCommand(), argument);
		}
	}

	static List<Double> toList(Point point) {
		return Arrays.asList(point.getX(), point.getY());
	}
}
