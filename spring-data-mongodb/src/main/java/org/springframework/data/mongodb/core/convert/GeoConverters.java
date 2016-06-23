/*
 * Copyright 2014-2016 the original author or authors.
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
import org.springframework.data.mongodb.core.geo.GeoJson;
import org.springframework.data.mongodb.core.geo.GeoJsonGeometryCollection;
import org.springframework.data.mongodb.core.geo.GeoJsonLineString;
import org.springframework.data.mongodb.core.geo.GeoJsonMultiLineString;
import org.springframework.data.mongodb.core.geo.GeoJsonMultiPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonMultiPolygon;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.data.mongodb.core.query.GeoCommand;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Wrapper class to contain useful geo structure converters for the usage with Mongo.
 * 
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
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
				, SphereToDbObjectConverter.INSTANCE //
				, DbObjectToBoxConverter.INSTANCE //
				, DbObjectToPolygonConverter.INSTANCE //
				, DbObjectToCircleConverter.INSTANCE //
				, DbObjectToSphereConverter.INSTANCE //
				, DbObjectToPointConverter.INSTANCE //
				, PointToDbObjectConverter.INSTANCE //
				, GeoCommandToDbObjectConverter.INSTANCE //
				, GeoJsonToDbObjectConverter.INSTANCE //
				, GeoJsonPointToDbObjectConverter.INSTANCE //
				, GeoJsonPolygonToDbObjectConverter.INSTANCE //
				, DbObjectToGeoJsonPointConverter.INSTANCE //
				, DbObjectToGeoJsonPolygonConverter.INSTANCE //
				, DbObjectToGeoJsonLineStringConverter.INSTANCE //
				, DbObjectToGeoJsonMultiLineStringConverter.INSTANCE //
				, DbObjectToGeoJsonMultiPointConverter.INSTANCE //
				, DbObjectToGeoJsonMultiPolygonConverter.INSTANCE //
				, DbObjectToGeoJsonGeometryCollectionConverter.INSTANCE);
	}

	/**
	 * Converts a {@link List} of {@link Double}s into a {@link Point}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	static enum DbObjectToPointConverter implements Converter<DBObject, Point> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Point convert(DBObject source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(source.keySet().size() == 2, "Source must contain 2 elements");

			if (source.containsField("type")) {
				return DbObjectToGeoJsonPointConverter.INSTANCE.convert(source);
			}

			return new Point((Double) source.get("x"), (Double) source.get("y"));
		}
	}

	/**
	 * Converts a {@link Point} into a {@link List} of {@link Double}s.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	static enum PointToDbObjectConverter implements Converter<Point, DBObject> {

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
	static enum BoxToDbObjectConverter implements Converter<Box, DBObject> {

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
	 * Converts a {@link BasicDBList} into a {@link Box}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	static enum DbObjectToBoxConverter implements Converter<DBObject, Box> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Box convert(DBObject source) {

			if (source == null) {
				return null;
			}

			Point first = DbObjectToPointConverter.INSTANCE.convert((DBObject) source.get("first"));
			Point second = DbObjectToPointConverter.INSTANCE.convert((DBObject) source.get("second"));

			return new Box(first, second);
		}
	}

	/**
	 * Converts a {@link Circle} into a {@link BasicDBList}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	static enum CircleToDbObjectConverter implements Converter<Circle, DBObject> {

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
	 * Converts a {@link DBObject} into a {@link Circle}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	static enum DbObjectToCircleConverter implements Converter<DBObject, Circle> {

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
	 * Converts a {@link Sphere} into a {@link BasicDBList}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	static enum SphereToDbObjectConverter implements Converter<Sphere, DBObject> {

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
	static enum DbObjectToSphereConverter implements Converter<DBObject, Sphere> {

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
	static enum PolygonToDbObjectConverter implements Converter<Polygon, DBObject> {

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
	 * Converts a {@link BasicDBList} into a {@link Polygon}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	static enum DbObjectToPolygonConverter implements Converter<DBObject, Polygon> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		@SuppressWarnings({ "unchecked" })
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

			return new Polygon(newPoints);
		}
	}

	/**
	 * Converts a {@link Sphere} into a {@link BasicDBList}.
	 * 
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	static enum GeoCommandToDbObjectConverter implements Converter<GeoCommand, DBObject> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		@SuppressWarnings("rawtypes")
		public DBObject convert(GeoCommand source) {

			if (source == null) {
				return null;
			}

			BasicDBList argument = new BasicDBList();

			Shape shape = source.getShape();

			if (shape instanceof GeoJson) {
				return GeoJsonToDbObjectConverter.INSTANCE.convert((GeoJson) shape);
			}

			if (shape instanceof Box) {

				argument.add(toList(((Box) shape).getFirst()));
				argument.add(toList(((Box) shape).getSecond()));

			} else if (shape instanceof Circle) {

				argument.add(toList(((Circle) shape).getCenter()));
				argument.add(((Circle) shape).getRadius().getNormalizedValue());

			} else if (shape instanceof Circle) {

				argument.add(toList(((Circle) shape).getCenter()));
				argument.add(((Circle) shape).getRadius());

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

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@SuppressWarnings("rawtypes")
	static enum GeoJsonToDbObjectConverter implements Converter<GeoJson, DBObject> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public DBObject convert(GeoJson source) {

			if (source == null) {
				return null;
			}

			DBObject dbo = new BasicDBObject("type", source.getType());

			if (source instanceof GeoJsonGeometryCollection) {

				BasicDBList dbl = new BasicDBList();

				for (GeoJson geometry : ((GeoJsonGeometryCollection) source).getCoordinates()) {
					dbl.add(convert(geometry));
				}

				dbo.put("geometries", dbl);

			} else {
				dbo.put("coordinates", convertIfNecessarry(source.getCoordinates()));
			}

			return dbo;
		}

		private Object convertIfNecessarry(Object candidate) {

			if (candidate instanceof GeoJson) {
				return convertIfNecessarry(((GeoJson) candidate).getCoordinates());
			}

			if (candidate instanceof Iterable) {

				BasicDBList dbl = new BasicDBList();

				for (Object element : (Iterable) candidate) {
					dbl.add(convertIfNecessarry(element));
				}

				return dbl;
			}

			if (candidate instanceof Point) {
				return toList((Point) candidate);
			}

			return candidate;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum GeoJsonPointToDbObjectConverter implements Converter<GeoJsonPoint, DBObject> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public DBObject convert(GeoJsonPoint source) {
			return GeoJsonToDbObjectConverter.INSTANCE.convert(source);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum GeoJsonPolygonToDbObjectConverter implements Converter<GeoJsonPolygon, DBObject> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public DBObject convert(GeoJsonPolygon source) {
			return GeoJsonToDbObjectConverter.INSTANCE.convert(source);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DbObjectToGeoJsonPointConverter implements Converter<DBObject, GeoJsonPoint> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public GeoJsonPoint convert(DBObject source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "Point"),
					String.format("Cannot convert type '%s' to Point.", source.get("type")));

			List<Number> dbl = (List<Number>) source.get("coordinates");
			return new GeoJsonPoint(dbl.get(0).doubleValue(), dbl.get(1).doubleValue());
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DbObjectToGeoJsonPolygonConverter implements Converter<DBObject, GeoJsonPolygon> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJsonPolygon convert(DBObject source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "Polygon"),
					String.format("Cannot convert type '%s' to Polygon.", source.get("type")));

			return toGeoJsonPolygon((BasicDBList) source.get("coordinates"));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DbObjectToGeoJsonMultiPolygonConverter implements Converter<DBObject, GeoJsonMultiPolygon> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJsonMultiPolygon convert(DBObject source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "MultiPolygon"),
					String.format("Cannot convert type '%s' to MultiPolygon.", source.get("type")));

			BasicDBList dbl = (BasicDBList) source.get("coordinates");
			List<GeoJsonPolygon> polygones = new ArrayList<GeoJsonPolygon>();

			for (Object polygon : dbl) {
				polygones.add(toGeoJsonPolygon((BasicDBList) polygon));
			}

			return new GeoJsonMultiPolygon(polygones);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DbObjectToGeoJsonLineStringConverter implements Converter<DBObject, GeoJsonLineString> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJsonLineString convert(DBObject source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "LineString"),
					String.format("Cannot convert type '%s' to LineString.", source.get("type")));

			BasicDBList cords = (BasicDBList) source.get("coordinates");

			return new GeoJsonLineString(toListOfPoint(cords));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DbObjectToGeoJsonMultiPointConverter implements Converter<DBObject, GeoJsonMultiPoint> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJsonMultiPoint convert(DBObject source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "MultiPoint"),
					String.format("Cannot convert type '%s' to MultiPoint.", source.get("type")));

			BasicDBList cords = (BasicDBList) source.get("coordinates");

			return new GeoJsonMultiPoint(toListOfPoint(cords));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DbObjectToGeoJsonMultiLineStringConverter implements Converter<DBObject, GeoJsonMultiLineString> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJsonMultiLineString convert(DBObject source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "MultiLineString"),
					String.format("Cannot convert type '%s' to MultiLineString.", source.get("type")));

			List<GeoJsonLineString> lines = new ArrayList<GeoJsonLineString>();
			BasicDBList cords = (BasicDBList) source.get("coordinates");

			for (Object line : cords) {
				lines.add(new GeoJsonLineString(toListOfPoint((BasicDBList) line)));
			}
			return new GeoJsonMultiLineString(lines);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DbObjectToGeoJsonGeometryCollectionConverter implements Converter<DBObject, GeoJsonGeometryCollection> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@SuppressWarnings("rawtypes")
		@Override
		public GeoJsonGeometryCollection convert(DBObject source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "GeometryCollection"),
					String.format("Cannot convert type '%s' to GeometryCollection.", source.get("type")));

			List<GeoJson<?>> geometries = new ArrayList<GeoJson<?>>();
			for (Object o : (List) source.get("geometries")) {
				geometries.add(convertGeometries((DBObject) o));
			}
			return new GeoJsonGeometryCollection(geometries);

		}

		private static GeoJson<?> convertGeometries(DBObject source) {

			Object type = source.get("type");
			if (ObjectUtils.nullSafeEquals(type, "Point")) {
				return DbObjectToGeoJsonPointConverter.INSTANCE.convert(source);
			}

			if (ObjectUtils.nullSafeEquals(type, "MultiPoint")) {
				return DbObjectToGeoJsonMultiPointConverter.INSTANCE.convert(source);
			}

			if (ObjectUtils.nullSafeEquals(type, "LineString")) {
				return DbObjectToGeoJsonLineStringConverter.INSTANCE.convert(source);
			}

			if (ObjectUtils.nullSafeEquals(type, "MultiLineString")) {
				return DbObjectToGeoJsonMultiLineStringConverter.INSTANCE.convert(source);
			}

			if (ObjectUtils.nullSafeEquals(type, "Polygon")) {
				return DbObjectToGeoJsonPolygonConverter.INSTANCE.convert(source);
			}
			if (ObjectUtils.nullSafeEquals(type, "MultiPolygon")) {
				return DbObjectToGeoJsonMultiPolygonConverter.INSTANCE.convert(source);
			}

			throw new IllegalArgumentException(String.format("Cannot convert unknown GeoJson type %s", type));
		}
	}

	static List<Double> toList(Point point) {
		return Arrays.asList(point.getX(), point.getY());
	}

	/**
	 * Converts a coordinate pairs nested in in {@link BasicDBList} into {@link GeoJsonPoint}s.
	 * 
	 * @param listOfCoordinatePairs
	 * @return
	 * @since 1.7
	 */
	@SuppressWarnings("unchecked")
	static List<Point> toListOfPoint(BasicDBList listOfCoordinatePairs) {

		List<Point> points = new ArrayList<Point>();

		for (Object point : listOfCoordinatePairs) {

			Assert.isInstanceOf(List.class, point);

			List<Number> coordinatesList = (List<Number>) point;

			points.add(new GeoJsonPoint(coordinatesList.get(0).doubleValue(), coordinatesList.get(1).doubleValue()));
		}
		return points;
	}

	/**
	 * Converts a coordinate pairs nested in in {@link BasicDBList} into {@link GeoJsonPolygon}.
	 * 
	 * @param dbList
	 * @return
	 * @since 1.7
	 */
	static GeoJsonPolygon toGeoJsonPolygon(BasicDBList dbList) {
		return new GeoJsonPolygon(toListOfPoint((BasicDBList) dbList.get(0)));
	}
}
