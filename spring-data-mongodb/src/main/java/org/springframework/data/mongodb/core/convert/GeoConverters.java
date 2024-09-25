/*
 * Copyright 2014-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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

import com.mongodb.Function;

/**
 * Wrapper class to contain useful geo structure converters for the usage with Mongo.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thiago Diniz da Silveira
 * @since 1.5
 */
@SuppressWarnings("ConstantConditions")
abstract class GeoConverters {

	private final static Map<String, Function<Document, GeoJson<?>>> converters;

	static {

		Collator caseInsensitive = Collator.getInstance();
		caseInsensitive.setStrength(Collator.PRIMARY);

		Map<String, Function<Document, GeoJson<?>>> geoConverters = new TreeMap<>(caseInsensitive);
		geoConverters.put("point", DocumentToGeoJsonPointConverter.INSTANCE::convert);
		geoConverters.put("multipoint", DocumentToGeoJsonMultiPointConverter.INSTANCE::convert);
		geoConverters.put("linestring", DocumentToGeoJsonLineStringConverter.INSTANCE::convert);
		geoConverters.put("multilinestring", DocumentToGeoJsonMultiLineStringConverter.INSTANCE::convert);
		geoConverters.put("polygon", DocumentToGeoJsonPolygonConverter.INSTANCE::convert);
		geoConverters.put("multipolygon", DocumentToGeoJsonMultiPolygonConverter.INSTANCE::convert);
		geoConverters.put("geometrycollection", DocumentToGeoJsonGeometryCollectionConverter.INSTANCE::convert);

		converters = geoConverters;
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private GeoConverters() {}

	/**
	 * Returns the geo converters to be registered.
	 *
	 * @return never {@literal null}.
	 */
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
				, DocumentToGeoJsonGeometryCollectionConverter.INSTANCE //
				, DocumentToGeoJsonConverter.INSTANCE);
	}

	/**
	 * Converts a {@link List} of {@link Double}s into a {@link Point}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	enum DocumentToPointConverter implements Converter<Document, Point> {

		INSTANCE;

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
	enum PointToDocumentConverter implements Converter<Point, Document> {

		INSTANCE;

		@Override
		public Document convert(Point source) {
			return source == null ? null : new Document("x", source.getX()).append("y", source.getY());
		}
	}

	/**
	 * Converts a {@link Box} into a {@link Document}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@WritingConverter
	enum BoxToDocumentConverter implements Converter<Box, Document> {

		INSTANCE;

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
	 * Converts a {@link Document} into a {@link Box}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	enum DocumentToBoxConverter implements Converter<Document, Box> {

		INSTANCE;

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
	 * Converts a {@link Circle} into a {@link Document}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	enum CircleToDocumentConverter implements Converter<Circle, Document> {

		INSTANCE;

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
	enum DocumentToCircleConverter implements Converter<Document, Circle> {

		INSTANCE;

		@Override
		public Circle convert(Document source) {

			if (source == null) {
				return null;
			}

			Document center = (Document) source.get("center");
			Number radius = (Number) source.get("radius");

			Assert.notNull(center, "Center must not be null");
			Assert.notNull(radius, "Radius must not be null");

			Distance distance = new Distance(toPrimitiveDoubleValue(radius));

			if (source.containsKey("metric")) {

				String metricString = (String) source.get("metric");
				Assert.notNull(metricString, "Metric must not be null");

				distance = distance.in(Metrics.valueOf(metricString));
			}

			return new Circle(DocumentToPointConverter.INSTANCE.convert(center), distance);
		}
	}

	/**
	 * Converts a {@link Sphere} into a {@link Document}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	enum SphereToDocumentConverter implements Converter<Sphere, Document> {

		INSTANCE;

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
	 * Converts a {@link Document} into a {@link Sphere}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	enum DocumentToSphereConverter implements Converter<Document, Sphere> {

		INSTANCE;

		@Override
		public Sphere convert(Document source) {

			if (source == null) {
				return null;
			}

			Document center = (Document) source.get("center");
			Number radius = (Number) source.get("radius");

			Assert.notNull(center, "Center must not be null");
			Assert.notNull(radius, "Radius must not be null");

			Distance distance = new Distance(toPrimitiveDoubleValue(radius));

			if (source.containsKey("metric")) {

				String metricString = (String) source.get("metric");
				Assert.notNull(metricString, "Metric must not be null");

				distance = distance.in(Metrics.valueOf(metricString));
			}

			return new Sphere(DocumentToPointConverter.INSTANCE.convert(center), distance);
		}
	}

	/**
	 * Converts a {@link Polygon} into a {@link Document}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	enum PolygonToDocumentConverter implements Converter<Polygon, Document> {

		INSTANCE;

		@Override
		public Document convert(Polygon source) {

			if (source == null) {
				return null;
			}

			List<Point> points = source.getPoints();
			List<Document> pointTuples = new ArrayList<>(points.size());

			for (Point point : points) {
				pointTuples.add(PointToDocumentConverter.INSTANCE.convert(point));
			}

			Document result = new Document();
			result.put("points", pointTuples);
			return result;
		}
	}

	/**
	 * Converts a {@link Document} into a {@link Polygon}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	@ReadingConverter
	enum DocumentToPolygonConverter implements Converter<Document, Polygon> {

		INSTANCE;

		@Override
		@SuppressWarnings({ "unchecked" })
		public Polygon convert(Document source) {

			if (source == null) {
				return null;
			}

			List<Document> points = (List<Document>) source.get("points");
			List<Point> newPoints = new ArrayList<>(points.size());

			for (Document element : points) {

				Assert.notNull(element, "Point elements of polygon must not be null");
				newPoints.add(DocumentToPointConverter.INSTANCE.convert(element));
			}

			return new Polygon(newPoints);
		}
	}

	/**
	 * Converts a {@link Sphere} into a {@link Document}.
	 *
	 * @author Thomas Darimont
	 * @since 1.5
	 */
	enum GeoCommandToDocumentConverter implements Converter<GeoCommand, Document> {

		INSTANCE;

		@Override
		@SuppressWarnings("rawtypes")
		public Document convert(GeoCommand source) {

			if (source == null) {
				return null;
			}

			List<Object> argument = new ArrayList<>(2);

			Shape shape = source.getShape();

			if (shape instanceof GeoJson geoJson) {
				return GeoJsonToDocumentConverter.INSTANCE.convert(geoJson);
			}

			if (shape instanceof Box box) {

				argument.add(toList(box.getFirst()));
				argument.add(toList(box.getSecond()));

			} else if (shape instanceof Circle circle) {

				argument.add(toList(circle.getCenter()));
				argument.add(circle.getRadius().getNormalizedValue());

			} else if (shape instanceof Polygon polygon) {

				List<Point> points = polygon.getPoints();
				argument = new ArrayList<>(points.size());
				for (Point point : points) {
					argument.add(toList(point));
				}

			} else if (shape instanceof Sphere sphere) {

				argument.add(toList(sphere.getCenter()));
				argument.add(sphere.getRadius().getNormalizedValue());
			}

			return new Document(source.getCommand(), argument);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum GeoJsonToDocumentConverter implements Converter<GeoJson<?>, Document> {

		INSTANCE;

		@Override
		public Document convert(GeoJson<?> source) {

			if (source == null) {
				return null;
			}

			Document dbo = new Document("type", source.getType());

			if (source instanceof GeoJsonGeometryCollection collection) {

				List<Object> dbl = new ArrayList<>();

				for (GeoJson<?> geometry : collection.getCoordinates()) {
					dbl.add(convert(geometry));
				}

				dbo.put("geometries", dbl);

			} else {
				dbo.put("coordinates", convertIfNecessary(source.getCoordinates()));
			}

			return dbo;
		}

		private Object convertIfNecessary(Object candidate) {

			if (candidate instanceof GeoJson geoJson) {
				return convertIfNecessary(geoJson.getCoordinates());
			}

			if (candidate instanceof Iterable<?> iterable) {

				List<Object> dbl = new ArrayList<>();

				for (Object element : iterable) {
					dbl.add(convertIfNecessary(element));
				}

				return dbl;
			}

			if (candidate instanceof Point point) {
				return toList(point);
			}

			return candidate;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum GeoJsonPointToDocumentConverter implements Converter<GeoJsonPoint, Document> {

		INSTANCE;

		@Override
		public Document convert(GeoJsonPoint source) {
			return GeoJsonToDocumentConverter.INSTANCE.convert(source);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum GeoJsonPolygonToDocumentConverter implements Converter<GeoJsonPolygon, Document> {

		INSTANCE;

		@Override
		public Document convert(GeoJsonPolygon source) {
			return GeoJsonToDocumentConverter.INSTANCE.convert(source);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum DocumentToGeoJsonPointConverter implements Converter<Document, GeoJsonPoint> {

		INSTANCE;

		@Override
		@SuppressWarnings("unchecked")
		public GeoJsonPoint convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "Point"),
					String.format("Cannot convert type '%s' to Point", source.get("type")));

			List<Number> dbl = (List<Number>) source.get("coordinates");
			return new GeoJsonPoint(toPrimitiveDoubleValue(dbl.get(0)), toPrimitiveDoubleValue(dbl.get(1)));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum DocumentToGeoJsonPolygonConverter implements Converter<Document, GeoJsonPolygon> {

		INSTANCE;

		@Override
		public GeoJsonPolygon convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "Polygon"),
					String.format("Cannot convert type '%s' to Polygon", source.get("type")));

			return toGeoJsonPolygon((List<?>) source.get("coordinates"));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum DocumentToGeoJsonMultiPolygonConverter implements Converter<Document, GeoJsonMultiPolygon> {

		INSTANCE;

		@Override
		public GeoJsonMultiPolygon convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "MultiPolygon"),
					String.format("Cannot convert type '%s' to MultiPolygon", source.get("type")));

			List<?> dbl = (List<?>) source.get("coordinates");
			List<GeoJsonPolygon> polygones = new ArrayList<>();

			for (Object polygon : dbl) {
				polygones.add(toGeoJsonPolygon((List<?>) polygon));
			}

			return new GeoJsonMultiPolygon(polygones);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum DocumentToGeoJsonLineStringConverter implements Converter<Document, GeoJsonLineString> {

		INSTANCE;

		@Override
		public GeoJsonLineString convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "LineString"),
					String.format("Cannot convert type '%s' to LineString", source.get("type")));

			List<?> cords = (List<?>) source.get("coordinates");

			return new GeoJsonLineString(toListOfPoint(cords));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum DocumentToGeoJsonMultiPointConverter implements Converter<Document, GeoJsonMultiPoint> {

		INSTANCE;

		@Override
		public GeoJsonMultiPoint convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "MultiPoint"),
					String.format("Cannot convert type '%s' to MultiPoint", source.get("type")));

			List<?> cords = (List<?>) source.get("coordinates");

			return new GeoJsonMultiPoint(toListOfPoint(cords));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum DocumentToGeoJsonMultiLineStringConverter implements Converter<Document, GeoJsonMultiLineString> {

		INSTANCE;

		@Override
		public GeoJsonMultiLineString convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "MultiLineString"),
					String.format("Cannot convert type '%s' to MultiLineString", source.get("type")));

			List<GeoJsonLineString> lines = new ArrayList<>();
			List<?> cords = (List<?>) source.get("coordinates");

			for (Object line : cords) {
				lines.add(new GeoJsonLineString(toListOfPoint((List<?>) line)));
			}
			return new GeoJsonMultiLineString(lines);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum DocumentToGeoJsonGeometryCollectionConverter implements Converter<Document, GeoJsonGeometryCollection> {

		INSTANCE;

		@SuppressWarnings("rawtypes")
		@Override
		public GeoJsonGeometryCollection convert(Document source) {

			if (source == null) {
				return null;
			}

			Assert.isTrue(ObjectUtils.nullSafeEquals(source.get("type"), "GeometryCollection"),
					String.format("Cannot convert type '%s' to GeometryCollection", source.get("type")));

			List<GeoJson<?>> geometries = new ArrayList<>();
			for (Object o : (List) source.get("geometries")) {
				geometries.add(toGenericGeoJson((Document) o));
			}

			return new GeoJsonGeometryCollection(geometries);
		}
	}

	static List<Double> toList(Point point) {
		return Arrays.asList(point.getX(), point.getY());
	}

	/**
	 * Converts a coordinate pairs nested in {@link List} into {@link GeoJsonPoint}s.
	 *
	 * @param listOfCoordinatePairs must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.7
	 */
	@SuppressWarnings("unchecked")
	static List<Point> toListOfPoint(List<?> listOfCoordinatePairs) {

		List<Point> points = new ArrayList<>(listOfCoordinatePairs.size());

		for (Object point : listOfCoordinatePairs) {

			Assert.isInstanceOf(List.class, point);

			List<Number> coordinatesList = (List<Number>) point;

			points.add(new GeoJsonPoint(toPrimitiveDoubleValue(coordinatesList.get(0)),
					toPrimitiveDoubleValue(coordinatesList.get(1))));
		}
		return points;
	}

	/**
	 * Converts a coordinate pairs nested in {@link List} into {@link GeoJsonPolygon}.
	 *
	 * @param dbList must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.7
	 */
	static GeoJsonPolygon toGeoJsonPolygon(List<?> dbList) {

		GeoJsonPolygon polygon = new GeoJsonPolygon(toListOfPoint((List<?>) dbList.get(0)));
		return dbList.size() > 1 ? polygon.withInnerRing(toListOfPoint((List<?>) dbList.get(1))) : polygon;
	}

	/**
	 * Converter implementation transforming a {@link Document} into a concrete {@link GeoJson} based on the embedded
	 * {@literal type} information.
	 *
	 * @since 2.1
	 * @author Christoph Strobl
	 */
	@ReadingConverter
	enum DocumentToGeoJsonConverter implements Converter<Document, GeoJson<?>> {
		INSTANCE;

		@Override
		public GeoJson<?> convert(Document source) {
			return toGenericGeoJson(source);
		}
	}

	private static GeoJson<?> toGenericGeoJson(Document source) {

		String type = source.get("type", String.class);

		if (type != null) {

			Function<Document, GeoJson<?>> converter = converters.get(type);

			if (converter != null) {
				return converter.apply(source);
			}
		}

		throw new IllegalArgumentException(String.format("No converter found capable of converting GeoJson type %s", type));
	}

	private static double toPrimitiveDoubleValue(Object value) {

		Assert.isInstanceOf(Number.class, value, "Argument must be a Number");
		return NumberUtils.convertNumberToTargetClass((Number) value, Double.class);
	}
}
