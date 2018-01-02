/*
 * Copyright 2014-2018 the original author or authors.
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

import org.bson.Document;
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
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;

import com.mongodb.BasicDBList;

/**
 * Wrapper class to contain useful geo structure converters for the usage with Mongo.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thiago Diniz da Silveira
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
				BoxToDocumentConverter.INSTANCE //
				, PolygonToDocumentConverter.INSTANCE //
				, CircleToDocumentConverter.INSTANCE //
				, SphereToDocumentConverter.INSTANCE //
				, DocumentToBoxConverter.INSTANCE //
				, DocumentToPolygonConverter.INSTANCE //
				, DocumentToCircleConverter.INSTANCE //
				, DocumentToSphereConverter.INSTANCE //
				, DocumentToPointConverter.INSTANCE //
				, PointToDocumentConverter.INSTANCE //
				, GeoCommandToDocumentConverter.INSTANCE //
				, GeoJsonToDocumentConverter.INSTANCE //
				, GeoJsonPointToDocumentConverter.INSTANCE //
				, GeoJsonPolygonToDocumentConverter.INSTANCE //
				, DocumentToGeoJsonPointConverter.INSTANCE //
				, DocumentToGeoJsonPolygonConverter.INSTANCE //
				, DocumentToGeoJsonLineStringConverter.INSTANCE //
				, DocumentToGeoJsonMultiLineStringConverter.INSTANCE //
				, DocumentToGeoJsonMultiPointConverter.INSTANCE //
				, DocumentToGeoJsonMultiPolygonConverter.INSTANCE //
				, DocumentToGeoJsonGeometryCollectionConverter.INSTANCE);
	}

	/**
	 * Converts a {@link List} of {@link Double}s into a {@link Point}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	static enum DocumentToPointConverter implements Converter<Document, Point> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Point convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(source.keySet().size() == 2, "Source must contain 2 elements");

			if (source.containsKey("type")) {
				return DocumentToGeoJsonPointConverter.INSTANCE.convert(source);
			}

			return new Point(toPrimitiveDoubleValue(source.get("x")), toPrimitiveDoubleValue(source.get("y")));
		}
	}

	/**
	 * Converts a {@link Point} into a {@link List} of {@link Double}s.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	static enum PointToDocumentConverter implements Converter<Point, Document> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Document convert(Point source) {
			return source == null ? null : new Document("x", source.getX()).append("y", source.getY());
		}
	}

	/**
	 * Converts a {@link Box} into a {@link BasicDBList}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@WritingConverter
	static enum BoxToDocumentConverter implements Converter<Box, Document> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Document convert(Box source) {

			if (source == null) {
				return null;
			}

			Document result = new Document();
			result.put("first", PointToDocumentConverter.INSTANCE.convert(source.getFirst()));
			result.put("second", PointToDocumentConverter.INSTANCE.convert(source.getSecond()));
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
	static enum DocumentToBoxConverter implements Converter<Document, Box> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Box convert(Document source) {

			if (source == null) {
				return null;
			}

			Point first = DocumentToPointConverter.INSTANCE.convert((Document) source.get("first"));
			Point second = DocumentToPointConverter.INSTANCE.convert((Document) source.get("second"));

			return new Box(first, second);
		}
	}

	/**
	 * Converts a {@link Circle} into a {@link BasicDBList}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	static enum CircleToDocumentConverter implements Converter<Circle, Document> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Document convert(Circle source) {

			if (source == null) {
				return null;
			}

			Document result = new Document();
			result.put("center", PointToDocumentConverter.INSTANCE.convert(source.getCenter()));
			result.put("radius", source.getRadius().getNormalizedValue());
			result.put("metric", source.getRadius().getMetric().toString());
			return result;
		}
	}

	/**
	 * Converts a {@link Document} into a {@link Circle}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	static enum DocumentToCircleConverter implements Converter<Document, Circle> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Circle convert(Document source) {

			if (source == null) {
				return null;
			}

			Document center = (Document) source.get("center");
			Number radius = (Number) source.get("radius");

			Assert.notNull(center, "Center must not be null!");
			Assert.notNull(radius, "Radius must not be null!");

			Distance distance = new Distance(toPrimitiveDoubleValue(radius));

			if (source.containsKey("metric")) {

				String metricString = (String) source.get("metric");
				Assert.notNull(metricString, "Metric must not be null!");

				distance = distance.in(Metrics.valueOf(metricString));
			}

			return new Circle(DocumentToPointConverter.INSTANCE.convert(center), distance);
		}
	}

	/**
	 * Converts a {@link Sphere} into a {@link BasicDBList}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	static enum SphereToDocumentConverter implements Converter<Sphere, Document> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Document convert(Sphere source) {

			if (source == null) {
				return null;
			}

			Document result = new Document();
			result.put("center", PointToDocumentConverter.INSTANCE.convert(source.getCenter()));
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
	static enum DocumentToSphereConverter implements Converter<Document, Sphere> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Sphere convert(Document source) {

			if (source == null) {
				return null;
			}

			Document center = (Document) source.get("center");
			Number radius = (Number) source.get("radius");

			Assert.notNull(center, "Center must not be null!");
			Assert.notNull(radius, "Radius must not be null!");

			Distance distance = new Distance(toPrimitiveDoubleValue(radius));

			if (source.containsKey("metric")) {

				String metricString = (String) source.get("metric");
				Assert.notNull(metricString, "Metric must not be null!");

				distance = distance.in(Metrics.valueOf(metricString));
			}

			return new Sphere(DocumentToPointConverter.INSTANCE.convert(center), distance);
		}
	}

	/**
	 * Converts a {@link Polygon} into a {@link BasicDBList}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	static enum PolygonToDocumentConverter implements Converter<Polygon, Document> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Document convert(Polygon source) {

			if (source == null) {
				return null;
			}

			List<Point> points = source.getPoints();
			List<Document> pointTuples = new ArrayList<Document>(points.size());

			for (Point point : points) {
				pointTuples.add(PointToDocumentConverter.INSTANCE.convert(point));
			}

			Document result = new Document();
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
	static enum DocumentToPolygonConverter implements Converter<Document, Polygon> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		@SuppressWarnings({ "unchecked" })
		public Polygon convert(Document source) {

			if (source == null) {
				return null;
			}

			List<Document> points = (List<Document>) source.get("points");
			List<Point> newPoints = new ArrayList<Point>(points.size());

			for (Document element : points) {

				Assert.notNull(element, "Point elements of polygon must not be null!");
				newPoints.add(DocumentToPointConverter.INSTANCE.convert(element));
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
	static enum GeoCommandToDocumentConverter implements Converter<GeoCommand, Document> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		@SuppressWarnings("rawtypes")
		public Document convert(GeoCommand source) {

			if (source == null) {
				return null;
			}

			List argument = new ArrayList();

			Shape shape = source.getShape();

			if (shape instanceof GeoJson) {
				return GeoJsonToDocumentConverter.INSTANCE.convert((GeoJson) shape);
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

			return new Document(source.getCommand(), argument);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@SuppressWarnings("rawtypes")
	static enum GeoJsonToDocumentConverter implements Converter<GeoJson, Document> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Document convert(GeoJson source) {

			if (source == null) {
				return null;
			}

			Document dbo = new Document("type", source.getType());

			if (source instanceof GeoJsonGeometryCollection) {

				List dbl = new ArrayList();

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

				List dbl = new ArrayList();

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
	static enum GeoJsonPointToDocumentConverter implements Converter<GeoJsonPoint, Document> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Document convert(GeoJsonPoint source) {
			return GeoJsonToDocumentConverter.INSTANCE.convert(source);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum GeoJsonPolygonToDocumentConverter implements Converter<GeoJsonPolygon, Document> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Document convert(GeoJsonPolygon source) {
			return GeoJsonToDocumentConverter.INSTANCE.convert(source);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DocumentToGeoJsonPointConverter implements Converter<Document, GeoJsonPoint> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public GeoJsonPoint convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "Point"),
					String.format("Cannot convert type '%s' to Point.", source.get("type")));

			List<Number> dbl = (List<Number>) source.get("coordinates");
			return new GeoJsonPoint(toPrimitiveDoubleValue(dbl.get(0)), toPrimitiveDoubleValue(dbl.get(1)));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DocumentToGeoJsonPolygonConverter implements Converter<Document, GeoJsonPolygon> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJsonPolygon convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "Polygon"),
					String.format("Cannot convert type '%s' to Polygon.", source.get("type")));

			return toGeoJsonPolygon((List) source.get("coordinates"));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DocumentToGeoJsonMultiPolygonConverter implements Converter<Document, GeoJsonMultiPolygon> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJsonMultiPolygon convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "MultiPolygon"),
					String.format("Cannot convert type '%s' to MultiPolygon.", source.get("type")));

			List dbl = (List) source.get("coordinates");
			List<GeoJsonPolygon> polygones = new ArrayList<GeoJsonPolygon>();

			for (Object polygon : dbl) {
				polygones.add(toGeoJsonPolygon((List) polygon));
			}

			return new GeoJsonMultiPolygon(polygones);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DocumentToGeoJsonLineStringConverter implements Converter<Document, GeoJsonLineString> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJsonLineString convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "LineString"),
					String.format("Cannot convert type '%s' to LineString.", source.get("type")));

			List cords = (List) source.get("coordinates");

			return new GeoJsonLineString(toListOfPoint(cords));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DocumentToGeoJsonMultiPointConverter implements Converter<Document, GeoJsonMultiPoint> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJsonMultiPoint convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "MultiPoint"),
					String.format("Cannot convert type '%s' to MultiPoint.", source.get("type")));

			List cords = (List) source.get("coordinates");

			return new GeoJsonMultiPoint(toListOfPoint(cords));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DocumentToGeoJsonMultiLineStringConverter implements Converter<Document, GeoJsonMultiLineString> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJsonMultiLineString convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "MultiLineString"),
					String.format("Cannot convert type '%s' to MultiLineString.", source.get("type")));

			List<GeoJsonLineString> lines = new ArrayList<GeoJsonLineString>();
			List cords = (List) source.get("coordinates");

			for (Object line : cords) {
				lines.add(new GeoJsonLineString(toListOfPoint((List) line)));
			}
			return new GeoJsonMultiLineString(lines);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static enum DocumentToGeoJsonGeometryCollectionConverter implements Converter<Document, GeoJsonGeometryCollection> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@SuppressWarnings("rawtypes")
		@Override
		public GeoJsonGeometryCollection convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "GeometryCollection"),
					String.format("Cannot convert type '%s' to GeometryCollection.", source.get("type")));

			List<GeoJson<?>> geometries = new ArrayList<GeoJson<?>>();
			for (Object o : (List) source.get("geometries")) {
				geometries.add(convertGeometries((Document) o));
			}
			return new GeoJsonGeometryCollection(geometries);

		}

		private static GeoJson<?> convertGeometries(Document source) {

			Object type = source.get("type");
			if (ObjectUtils.nullSafeEquals(type, "Point")) {
				return DocumentToGeoJsonPointConverter.INSTANCE.convert(source);
			}

			if (ObjectUtils.nullSafeEquals(type, "MultiPoint")) {
				return DocumentToGeoJsonMultiPointConverter.INSTANCE.convert(source);
			}

			if (ObjectUtils.nullSafeEquals(type, "LineString")) {
				return DocumentToGeoJsonLineStringConverter.INSTANCE.convert(source);
			}

			if (ObjectUtils.nullSafeEquals(type, "MultiLineString")) {
				return DocumentToGeoJsonMultiLineStringConverter.INSTANCE.convert(source);
			}

			if (ObjectUtils.nullSafeEquals(type, "Polygon")) {
				return DocumentToGeoJsonPolygonConverter.INSTANCE.convert(source);
			}
			if (ObjectUtils.nullSafeEquals(type, "MultiPolygon")) {
				return DocumentToGeoJsonMultiPolygonConverter.INSTANCE.convert(source);
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
	static List<Point> toListOfPoint(List listOfCoordinatePairs) {

		List<Point> points = new ArrayList<Point>();

		for (Object point : listOfCoordinatePairs) {

			Assert.isInstanceOf(List.class, point);

			List<Number> coordinatesList = (List<Number>) point;

			points.add(new GeoJsonPoint(toPrimitiveDoubleValue(coordinatesList.get(0)),
					toPrimitiveDoubleValue(coordinatesList.get(1))));
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
	static GeoJsonPolygon toGeoJsonPolygon(List dbList) {
		return new GeoJsonPolygon(toListOfPoint((List) dbList.get(0)));
	}

	private static double toPrimitiveDoubleValue(Object value) {

		Assert.isInstanceOf(Number.class, value, "Argument must be a Number.");
		return NumberUtils.convertNumberToTargetClass((Number) value, Double.class).doubleValue();
	}
}
