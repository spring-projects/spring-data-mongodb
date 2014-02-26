/*
 * Copyright 2011-2014 the original author or authors.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;
import org.springframework.data.geo.Sphere;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Wrapper class to contain useful converters for the usage with Mongo.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
abstract class MongoConverters {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private MongoConverters() {}

	/**
	 * Simple singleton to convert {@link ObjectId}s to their {@link String} representation.
	 * 
	 * @author Oliver Gierke
	 */
	public static enum ObjectIdToStringConverter implements Converter<ObjectId, String> {
		INSTANCE;

		public String convert(ObjectId id) {
			return id == null ? null : id.toString();
		}
	}

	/**
	 * Simple singleton to convert {@link String}s to their {@link ObjectId} representation.
	 * 
	 * @author Oliver Gierke
	 */
	public static enum StringToObjectIdConverter implements Converter<String, ObjectId> {
		INSTANCE;

		public ObjectId convert(String source) {
			return StringUtils.hasText(source) ? new ObjectId(source) : null;
		}
	}

	/**
	 * Simple singleton to convert {@link ObjectId}s to their {@link java.math.BigInteger} representation.
	 * 
	 * @author Oliver Gierke
	 */
	public static enum ObjectIdToBigIntegerConverter implements Converter<ObjectId, BigInteger> {
		INSTANCE;

		public BigInteger convert(ObjectId source) {
			return source == null ? null : new BigInteger(source.toString(), 16);
		}
	}

	/**
	 * Simple singleton to convert {@link BigInteger}s to their {@link ObjectId} representation.
	 * 
	 * @author Oliver Gierke
	 */
	public static enum BigIntegerToObjectIdConverter implements Converter<BigInteger, ObjectId> {
		INSTANCE;

		public ObjectId convert(BigInteger source) {
			return source == null ? null : new ObjectId(source.toString(16));
		}
	}

	public static enum BigDecimalToStringConverter implements Converter<BigDecimal, String> {
		INSTANCE;

		public String convert(BigDecimal source) {
			return source == null ? null : source.toString();
		}
	}

	public static enum StringToBigDecimalConverter implements Converter<String, BigDecimal> {
		INSTANCE;

		public BigDecimal convert(String source) {
			return StringUtils.hasText(source) ? new BigDecimal(source) : null;
		}
	}

	public static enum BigIntegerToStringConverter implements Converter<BigInteger, String> {
		INSTANCE;

		public String convert(BigInteger source) {
			return source == null ? null : source.toString();
		}
	}

	public static enum StringToBigIntegerConverter implements Converter<String, BigInteger> {
		INSTANCE;

		public BigInteger convert(String source) {
			return StringUtils.hasText(source) ? new BigInteger(source) : null;
		}
	}

	public static enum URLToStringConverter implements Converter<URL, String> {
		INSTANCE;

		public String convert(URL source) {
			return source == null ? null : source.toString();
		}
	}

	public static enum StringToURLConverter implements Converter<String, URL> {
		INSTANCE;

		private static final TypeDescriptor SOURCE = TypeDescriptor.valueOf(String.class);
		private static final TypeDescriptor TARGET = TypeDescriptor.valueOf(URL.class);

		public URL convert(String source) {

			try {
				return source == null ? null : new URL(source);
			} catch (MalformedURLException e) {
				throw new ConversionFailedException(SOURCE, TARGET, source, e);
			}
		}
	}

	@ReadingConverter
	public static enum DBObjectToStringConverter implements Converter<DBObject, String> {

		INSTANCE;

		@Override
		public String convert(DBObject source) {
			return source == null ? null : source.toString();
		}
	}

	/**
	 * @author Thomas Darimont
	 */
	@WritingConverter
	public static enum ShapeToDbObjectConverter implements Converter<org.springframework.data.geo.Shape, DBObject> {

		INSTANCE;

		public DBObject convert(org.springframework.data.geo.Shape source) {
			return source == null ? null : MongoShapeConverter.toDboObject(source);
		}
	}

	/**
	 * @author Thomas Darimont
	 */
	@ReadingConverter
	public static enum DboObjectToShapeConverter implements Converter<DBObject, org.springframework.data.geo.Shape> {

		INSTANCE;

		public org.springframework.data.geo.Shape convert(DBObject source) {

			return source == null ? null : MongoShapeConverter.fromDboObject(source);
		}
	}

	@ReadingConverter
	public static enum ListToPointConverter implements Converter<List<Double>, Point> {

		INSTANCE;

		public Point convert(List<Double> source) {

			Assert.notEmpty(source, "Source must not be empty!");
			Assert.isTrue(source.size() == 2, "Source must contain 2 elements");

			return source == null ? null : new Point(source.get(0), source.get(1));
		}
	}

	@WritingConverter
	public static enum PointToListConverter implements Converter<org.springframework.data.geo.Point, List<Double>> {

		INSTANCE;

		@Override
		public List<Double> convert(org.springframework.data.geo.Point source) {
			return Arrays.asList(source.getX(), source.getY());
		}
	}

	/**
	 * Provides lookup and conversion functionality for common MongoDB {@link Shape}s.
	 * 
	 * @author Thomas Darimont
	 */
	public static enum MongoShapeConverter {

		BOX("$box", Box.class) {

			@Override
			public Box read(DBObject o) {

				List<?> list = (List<?>) o.get(getKeyword());

				Point lowerLeft = ListToPointConverter.INSTANCE.convert((List<Double>) list.get(0));
				Point upperRight = ListToPointConverter.INSTANCE.convert((List<Double>) list.get(1));

				return new Box(lowerLeft, upperRight);
			}

			/* 
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.convert.MongoConverters.MongoShapeConverter#write(org.springframework.data.geo.Shape)
			 */
			@Override
			public DBObject write(Shape shape) {

				if (!(shape instanceof Box)) {
					return null;
				}

				Box box = (Box) shape;

				List<Double> lowerLeft = PointToListConverter.INSTANCE.convert(box.getLowerLeft());
				List<Double> upperRight = PointToListConverter.INSTANCE.convert(box.getUpperRight());

				return new BasicDBObject(getKeyword(), Arrays.asList(lowerLeft, upperRight));
			}
		},

		CIRCLE("$center", Circle.class) {

			@Override
			public Circle read(DBObject o) {

				List<?> list = (List<?>) o.get(getKeyword());

				Point center = ListToPointConverter.INSTANCE.convert((List<Double>) list.get(0));
				Double radius = (Double) list.get(1);

				return new Circle(center, radius);
			}

			/* 
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.convert.MongoConverters.MongoShapeConverter#write(org.springframework.data.geo.Shape)
			 */
			@Override
			public DBObject write(Shape shape) {

				if (!(shape instanceof Circle)) {
					return null;
				}

				Circle circle = (Circle) shape;

				List<Double> center = PointToListConverter.INSTANCE.convert(circle.getCenter());
				Double radius = circle.getRadius();

				return new BasicDBObject(getKeyword(), Arrays.asList(center, radius));
			}
		},

		SPHERE("$centerSphere", Sphere.class) {

			@Override
			public Sphere read(DBObject o) {

				List<?> list = (List<?>) o.get(getKeyword());

				Point center = ListToPointConverter.INSTANCE.convert((List<Double>) list.get(0));
				Double radius = (Double) list.get(1);

				return new Sphere(center, radius);
			}

			@Override
			public DBObject write(Shape shape) {

				if (!(shape instanceof Sphere)) {
					return null;
				}

				Sphere sphere = (Sphere) shape;

				List<Double> center = PointToListConverter.INSTANCE.convert(sphere.getCenter());
				Double radius = sphere.getRadius();

				return new BasicDBObject(getKeyword(), Arrays.asList(center, radius));
			}
		},

		POLYGON("$polygon", Polygon.class) {

			@Override
			public Polygon read(DBObject o) {

				List<?> list = (List<?>) o.get(getKeyword());

				List<Point> points = new ArrayList<Point>(list.size());
				for (Object element : list) {

					Assert.notNull(element, "point elements of polygon must not be null!");

					points.add(ListToPointConverter.INSTANCE.convert((List<Double>) element));
				}

				return new Polygon(points);
			}

			@Override
			public DBObject write(Shape shape) {

				if (!(shape instanceof Polygon)) {
					return null;
				}

				Polygon polygon = (Polygon) shape;

				List<Point> points = polygon.getPoints();
				List<List<Double>> pointTuples = new ArrayList<List<Double>>(points.size());
				for (Point point : points) {
					pointTuples.add(point.asList());
				}

				return new BasicDBObject(getKeyword(), pointTuples);
			}
		};

		private final String keyword;
		private final Class<? extends Shape> shapeType;

		private MongoShapeConverter(String keyword, Class<? extends Shape> shapeType) {
			this.keyword = keyword;
			this.shapeType = shapeType;
		}

		public abstract <S extends Shape> S read(DBObject o);

		public abstract DBObject write(Shape shape);

		/**
		 * Returns a {@link Shape} from the given
		 * 
		 * @param source
		 * @return
		 */
		public static <S extends Shape> S fromDboObject(DBObject source) {

			if (source == null) {
				return null;
			}

			Iterator<String> keys = source.keySet().iterator();
			if (!keys.hasNext()) {
				return null;
			}

			String keyword = keys.next();
			Assert.isTrue(!keys.hasNext(), "Expected only one keyword but got: " + source.keySet().size());

			MongoShapeConverter mongoShapeConverter = from(keyword);

			return mongoShapeConverter == null ? null : (S) mongoShapeConverter.read(source);
		}

		public static DBObject toDboObject(Shape source) {

			if (source == null) {
				return null;
			}

			MongoShapeConverter mongoShapeConverter = lookupConverterForShapeType(source);

			return mongoShapeConverter == null ? null : mongoShapeConverter.write(source);
		}

		public String getKeyword() {
			return keyword;
		}

		public Class<? extends Shape> getShapeType() {
			return shapeType;
		}

		/**
		 * Returns a {@link MongoShape} that corresponds to the given keyword or {@literal null} if none could be found.
		 * 
		 * @param keyword must not be {@literal null}!
		 * @return
		 */
		public static MongoShapeConverter from(String keyword) {

			Assert.notNull(keyword, "Keyword must not be null!");

			return lookupConverterForKeyword(keyword);
		}

		/**
		 * Returns the keyword that corresponds to the given {@link org.springframework.data.geo.Shape}.
		 * 
		 * @param shape
		 * @return
		 */
		public static String getKeyword(Shape shape) {

			Assert.notNull(shape, "Shape must not be null!");

			// Fast-path for legacy SD MongoDB shapes.
			if (shape instanceof org.springframework.data.mongodb.core.geo.Shape) {
				return ((org.springframework.data.mongodb.core.geo.Shape) shape).getCommand();
			}

			MongoShapeConverter converter = lookupConverterForShapeType(shape);

			if (converter == null) {
				throw new IllegalArgumentException("Shape is not supported: " + (shape == null ? null : shape.getClass()));
			}

			return converter.getKeyword();
		}

		private static MongoShapeConverter lookupConverterForShapeType(Shape shape) {

			Assert.notNull(shape, "Shape must not be null!");

			for (MongoShapeConverter mongoShapeConverter : values()) {
				if (mongoShapeConverter.getShapeType().isAssignableFrom(shape.getClass())) {
					return mongoShapeConverter;
				}
			}
			return null;
		}

		private static MongoShapeConverter lookupConverterForKeyword(String keyword) {

			Assert.notNull(keyword, "Keyword must not be null!");

			for (MongoShapeConverter mongoShapeConverter : values()) {
				if (keyword.equals(mongoShapeConverter.getKeyword())) {
					return mongoShapeConverter;
				}
			}
			return null;
		}
	}
}
