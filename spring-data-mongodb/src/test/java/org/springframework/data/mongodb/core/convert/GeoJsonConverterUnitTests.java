/*
 * Copyright 2015-2016 the original author or authors.
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

import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.convert.GeoConverters.DbObjectToGeoJsonLineStringConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DbObjectToGeoJsonMultiLineStringConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DbObjectToGeoJsonMultiPointConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DbObjectToGeoJsonMultiPolygonConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DbObjectToGeoJsonPointConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DbObjectToGeoJsonPolygonConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.GeoJsonToDbObjectConverter;
import org.springframework.data.mongodb.core.geo.GeoJson;
import org.springframework.data.mongodb.core.geo.GeoJsonGeometryCollection;
import org.springframework.data.mongodb.core.geo.GeoJsonLineString;
import org.springframework.data.mongodb.core.geo.GeoJsonMultiLineString;
import org.springframework.data.mongodb.core.geo.GeoJsonMultiPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonMultiPolygon;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.test.util.BasicDbListBuilder;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
 */
@RunWith(Suite.class)
@SuiteClasses({ GeoJsonConverterUnitTests.GeoJsonToDbObjectConverterUnitTests.class,
		GeoJsonConverterUnitTests.DbObjectToGeoJsonPointConverterUnitTests.class,
		GeoJsonConverterUnitTests.DbObjectToGeoJsonPolygonConverterUnitTests.class,
		GeoJsonConverterUnitTests.DbObjectToGeoJsonLineStringConverterUnitTests.class,
		GeoJsonConverterUnitTests.DbObjectToGeoJsonMultiPolygonConverterUnitTests.class,
		GeoJsonConverterUnitTests.DbObjectToGeoJsonMultiLineStringConverterUnitTests.class,
		GeoJsonConverterUnitTests.DbObjectToGeoJsonMultiPointConverterUnitTests.class })
public class GeoJsonConverterUnitTests {

	/*
	 * --- GeoJson
	 */
	static final GeoJsonPoint SINGLE_POINT = new GeoJsonPoint(100, 50);

	static final Point POINT_0 = new Point(0, 0);
	static final Point POINT_1 = new Point(100, 0);
	static final Point POINT_2 = new Point(100, 100);
	static final Point POINT_3 = new Point(0, 100);

	static final Point INNER_POINT_0 = new Point(10, 10);
	static final Point INNER_POINT_1 = new Point(90, 10);
	static final Point INNER_POINT_2 = new Point(90, 90);
	static final Point INNER_POINT_3 = new Point(10, 90);

	static final GeoJsonMultiPoint MULTI_POINT = new GeoJsonMultiPoint(POINT_0, POINT_2, POINT_3);
	static final GeoJsonLineString LINE_STRING = new GeoJsonLineString(POINT_0, POINT_1, POINT_2);
	@SuppressWarnings("unchecked") static final GeoJsonMultiLineString MULTI_LINE_STRING = new GeoJsonMultiLineString(
			Arrays.asList(POINT_0, POINT_1, POINT_2), Arrays.asList(POINT_3, POINT_0));
	static final GeoJsonPolygon POLYGON = new GeoJsonPolygon(POINT_0, POINT_1, POINT_2, POINT_3, POINT_0);
	static final GeoJsonPolygon POLYGON_WITH_2_RINGS = POLYGON.withInnerRing(INNER_POINT_0, INNER_POINT_1, INNER_POINT_2,
			INNER_POINT_3, INNER_POINT_0);
	static final GeoJsonMultiPolygon MULTI_POLYGON = new GeoJsonMultiPolygon(Arrays.asList(POLYGON));
	static final GeoJsonGeometryCollection GEOMETRY_COLLECTION = new GeoJsonGeometryCollection(
			Arrays.<GeoJson<?>> asList(SINGLE_POINT, POLYGON));
	/*
	 * -- GeoJson DBObjects
	 */

	// Point
	static final BasicDBList SINGE_POINT_CORDS = new BasicDbListBuilder() //
			.add(SINGLE_POINT.getX()) //
			.add(SINGLE_POINT.getY()) //
			.get(); //
	static final DBObject SINGLE_POINT_DBO = new BasicDBObjectBuilder() //
			.add("type", "Point") //
			.add("coordinates", SINGE_POINT_CORDS)//
			.get();

	// MultiPoint
	static final BasicDBList MULTI_POINT_CORDS = new BasicDbListBuilder() //
			.add(new BasicDbListBuilder().add(POINT_0.getX()).add(POINT_0.getY()).get()) //
			.add(new BasicDbListBuilder().add(POINT_2.getX()).add(POINT_2.getY()).get()) //
			.add(new BasicDbListBuilder().add(POINT_3.getX()).add(POINT_3.getY()).get()) //
			.get();
	static final DBObject MULTI_POINT_DBO = new BasicDBObjectBuilder() //
			.add("type", "MultiPoint")//
			.add("coordinates", MULTI_POINT_CORDS)//
			.get();

	// Polygon
	static final BasicDBList POLYGON_OUTER_CORDS = new BasicDbListBuilder() //
			.add(new BasicDbListBuilder().add(POINT_0.getX()).add(POINT_0.getY()).get()) //
			.add(new BasicDbListBuilder().add(POINT_1.getX()).add(POINT_1.getY()).get()) //
			.add(new BasicDbListBuilder().add(POINT_2.getX()).add(POINT_2.getY()).get()) //
			.add(new BasicDbListBuilder().add(POINT_3.getX()).add(POINT_3.getY()).get()) //
			.add(new BasicDbListBuilder().add(POINT_0.getX()).add(POINT_0.getY()).get()) //
			.get();

	static final BasicDBList POLYGON_INNER_CORDS = new BasicDbListBuilder() //
			.add(new BasicDbListBuilder().add(INNER_POINT_0.getX()).add(INNER_POINT_0.getY()).get()) //
			.add(new BasicDbListBuilder().add(INNER_POINT_1.getX()).add(INNER_POINT_1.getY()).get()) //
			.add(new BasicDbListBuilder().add(INNER_POINT_2.getX()).add(INNER_POINT_2.getY()).get()) //
			.add(new BasicDbListBuilder().add(INNER_POINT_3.getX()).add(INNER_POINT_3.getY()).get()) //
			.add(new BasicDbListBuilder().add(INNER_POINT_0.getX()).add(INNER_POINT_0.getY()).get()) //
			.get();

	static final BasicDBList POLYGON_CORDS = new BasicDbListBuilder().add(POLYGON_OUTER_CORDS).get();
	static final DBObject POLYGON_DBO = new BasicDBObjectBuilder() //
			.add("type", "Polygon") //
			.add("coordinates", POLYGON_CORDS) //
			.get();

	static final BasicDBList POLYGON_WITH_2_RINGS_CORDS = new BasicDbListBuilder().add(POLYGON_OUTER_CORDS)
			.add(POLYGON_INNER_CORDS).get();
	static final DBObject POLYGON_WITH_2_RINGS_DBO = new BasicDBObjectBuilder() //
			.add("type", "Polygon") //
			.add("coordinates", POLYGON_WITH_2_RINGS_CORDS) //
			.get();

	// LineString
	static final BasicDBList LINE_STRING_CORDS_0 = new BasicDbListBuilder() //
			.add(new BasicDbListBuilder().add(POINT_0.getX()).add(POINT_0.getY()).get()) //
			.add(new BasicDbListBuilder().add(POINT_1.getX()).add(POINT_1.getY()).get()) //
			.add(new BasicDbListBuilder().add(POINT_2.getX()).add(POINT_2.getY()).get()) //
			.get();
	static final BasicDBList LINE_STRING_CORDS_1 = new BasicDbListBuilder() //
			.add(new BasicDbListBuilder().add(POINT_3.getX()).add(POINT_3.getY()).get()) //
			.add(new BasicDbListBuilder().add(POINT_0.getX()).add(POINT_0.getY()).get()) //
			.get();
	static final DBObject LINE_STRING_DBO = new BasicDBObjectBuilder().add("type", "LineString")
			.add("coordinates", LINE_STRING_CORDS_0).get();

	// MultiLineString
	static final BasicDBList MUILT_LINE_STRING_CORDS = new BasicDbListBuilder() //
			.add(LINE_STRING_CORDS_0) //
			.add(LINE_STRING_CORDS_1) //
			.get();
	static final DBObject MULTI_LINE_STRING_DBO = new BasicDBObjectBuilder().add("type", "MultiLineString")
			.add("coordinates", MUILT_LINE_STRING_CORDS).get();

	// MultiPolygoin
	static final BasicDBList MULTI_POLYGON_CORDS = new BasicDbListBuilder().add(POLYGON_CORDS).get();
	static final DBObject MULTI_POLYGON_DBO = new BasicDBObjectBuilder().add("type", "MultiPolygon")
			.add("coordinates", MULTI_POLYGON_CORDS).get();

	// GeometryCollection
	static final BasicDBList GEOMETRY_COLLECTION_GEOMETRIES = new BasicDbListBuilder() //
			.add(SINGLE_POINT_DBO)//
			.add(POLYGON_DBO)//
			.get();
	static final DBObject GEOMETRY_COLLECTION_DBO = new BasicDBObjectBuilder().add("type", "GeometryCollection")
			.add("geometries", GEOMETRY_COLLECTION_GEOMETRIES).get();

	/**
	 * @author Christoph Strobl
	 */
	public static class DbObjectToGeoJsonPolygonConverterUnitTests {

		DbObjectToGeoJsonPolygonConverter converter = DbObjectToGeoJsonPolygonConverter.INSTANCE;
		public @Rule ExpectedException expectedException = ExpectedException.none();

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(POLYGON_DBO), equalTo(POLYGON));
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null), nullValue());
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldThrowExceptionWhenTypeDoesNotMatchPolygon() {

			expectedException.expect(IllegalArgumentException.class);
			expectedException.expectMessage("'YouDontKonwMe' to Polygon");

			converter.convert(new BasicDBObject("type", "YouDontKonwMe"));
		}

		/**
		 * @see DATAMONGO-1399
		 */
		@Test
		public void shouldConvertDboWithMultipleRingsCorrectly() {
			assertThat(converter.convert(POLYGON_WITH_2_RINGS_DBO), equalTo(POLYGON_WITH_2_RINGS));
		}

	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DbObjectToGeoJsonPointConverterUnitTests {

		DbObjectToGeoJsonPointConverter converter = DbObjectToGeoJsonPointConverter.INSTANCE;
		public @Rule ExpectedException expectedException = ExpectedException.none();

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(SINGLE_POINT_DBO), equalTo(SINGLE_POINT));
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null), nullValue());
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldThrowExceptionWhenTypeDoesNotMatchPoint() {

			expectedException.expect(IllegalArgumentException.class);
			expectedException.expectMessage("'YouDontKonwMe' to Point");

			converter.convert(new BasicDBObject("type", "YouDontKonwMe"));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DbObjectToGeoJsonLineStringConverterUnitTests {

		DbObjectToGeoJsonLineStringConverter converter = DbObjectToGeoJsonLineStringConverter.INSTANCE;
		public @Rule ExpectedException expectedException = ExpectedException.none();

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(LINE_STRING_DBO), equalTo(LINE_STRING));
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null), nullValue());
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldThrowExceptionWhenTypeDoesNotMatchPoint() {

			expectedException.expect(IllegalArgumentException.class);
			expectedException.expectMessage("'YouDontKonwMe' to LineString");

			converter.convert(new BasicDBObject("type", "YouDontKonwMe"));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DbObjectToGeoJsonMultiLineStringConverterUnitTests {

		DbObjectToGeoJsonMultiLineStringConverter converter = DbObjectToGeoJsonMultiLineStringConverter.INSTANCE;
		public @Rule ExpectedException expectedException = ExpectedException.none();

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(MULTI_LINE_STRING_DBO), equalTo(MULTI_LINE_STRING));
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null), nullValue());
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldThrowExceptionWhenTypeDoesNotMatchPoint() {

			expectedException.expect(IllegalArgumentException.class);
			expectedException.expectMessage("'YouDontKonwMe' to MultiLineString");

			converter.convert(new BasicDBObject("type", "YouDontKonwMe"));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DbObjectToGeoJsonMultiPointConverterUnitTests {

		DbObjectToGeoJsonMultiPointConverter converter = DbObjectToGeoJsonMultiPointConverter.INSTANCE;
		public @Rule ExpectedException expectedException = ExpectedException.none();

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(MULTI_POINT_DBO), equalTo(MULTI_POINT));
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null), nullValue());
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldThrowExceptionWhenTypeDoesNotMatchPoint() {

			expectedException.expect(IllegalArgumentException.class);
			expectedException.expectMessage("'YouDontKonwMe' to MultiPoint");

			converter.convert(new BasicDBObject("type", "YouDontKonwMe"));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DbObjectToGeoJsonMultiPolygonConverterUnitTests {

		DbObjectToGeoJsonMultiPolygonConverter converter = DbObjectToGeoJsonMultiPolygonConverter.INSTANCE;
		public @Rule ExpectedException expectedException = ExpectedException.none();

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(MULTI_POLYGON_DBO), equalTo(MULTI_POLYGON));
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null), nullValue());
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldThrowExceptionWhenTypeDoesNotMatchPoint() {

			expectedException.expect(IllegalArgumentException.class);
			expectedException.expectMessage("'YouDontKonwMe' to MultiPolygon");

			converter.convert(new BasicDBObject("type", "YouDontKonwMe"));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class GeoJsonToDbObjectConverterUnitTests {

		GeoJsonToDbObjectConverter converter = GeoJsonToDbObjectConverter.INSTANCE;

		/**
		 * @see DATAMONGO-1135
		 */
		public void convertShouldReturnNullWhenGivenNull() {
			assertThat(converter.convert(null), nullValue());
		}

		/**
		 * @see DATAMONGO-1135
		 */
		@Test
		public void shouldConvertGeoJsonPointCorrectly() {
			assertThat(converter.convert(SINGLE_POINT), equalTo(SINGLE_POINT_DBO));
		}

		/**
		 * @see DATAMONGO-1135
		 */
		@Test
		public void shouldConvertGeoJsonPolygonCorrectly() {
			assertThat(converter.convert(POLYGON), equalTo(POLYGON_DBO));
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldConvertGeoJsonLineStringCorrectly() {
			assertThat(converter.convert(LINE_STRING), equalTo(LINE_STRING_DBO));
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldConvertGeoJsonMultiLineStringCorrectly() {
			assertThat(converter.convert(MULTI_LINE_STRING), equalTo(MULTI_LINE_STRING_DBO));
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldConvertGeoJsonMultiPointCorrectly() {
			assertThat(converter.convert(MULTI_POINT), equalTo(MULTI_POINT_DBO));
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldConvertGeoJsonMultiPolygonCorrectly() {
			assertThat(converter.convert(MULTI_POLYGON), equalTo(MULTI_POLYGON_DBO));
		}

		/**
		 * @see DATAMONGO-1137
		 */
		@Test
		public void shouldConvertGeometryCollectionCorrectly() {
			assertThat(converter.convert(GEOMETRY_COLLECTION), equalTo(GEOMETRY_COLLECTION_DBO));
		}

		/**
		 * @see DATAMONGO-1399
		 */
		@Test
		public void shouldConvertGeoJsonPolygonWithMultipleRingsCorrectly() {
			assertThat(converter.convert(POLYGON_WITH_2_RINGS), equalTo(POLYGON_WITH_2_RINGS_DBO));
		}
	}
}
