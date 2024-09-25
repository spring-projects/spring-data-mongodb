/*
 * Copyright 2015-2024 the original author or authors.
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

import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.convert.GeoConverters.DocumentToGeoJsonLineStringConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DocumentToGeoJsonMultiLineStringConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DocumentToGeoJsonMultiPointConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DocumentToGeoJsonMultiPolygonConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DocumentToGeoJsonPointConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.DocumentToGeoJsonPolygonConverter;
import org.springframework.data.mongodb.core.convert.GeoConverters.GeoJsonToDocumentConverter;
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

/**
 * @author Christoph Strobl
 */
@RunWith(Suite.class)
@SuiteClasses({ GeoJsonConverterUnitTests.GeoJsonToDocumentConverterUnitTests.class,
		GeoJsonConverterUnitTests.DocumentToGeoJsonPointConverterUnitTests.class,
		GeoJsonConverterUnitTests.DocumentToGeoJsonPolygonConverterUnitTests.class,
		GeoJsonConverterUnitTests.DocumentToGeoJsonLineStringConverterUnitTests.class,
		GeoJsonConverterUnitTests.DocumentToGeoJsonMultiPolygonConverterUnitTests.class,
		GeoJsonConverterUnitTests.DocumentToGeoJsonMultiLineStringConverterUnitTests.class,
		GeoJsonConverterUnitTests.DocumentToGeoJsonMultiPointConverterUnitTests.class })
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
	 * -- GeoJson Documents
	 */

	// Point
	static final BasicDBList SINGE_POINT_CORDS = new BasicDbListBuilder() //
			.add(SINGLE_POINT.getX()) //
			.add(SINGLE_POINT.getY()) //
			.get(); //
	static final Document SINGLE_POINT_DOC = new Document() //
			.append("type", "Point") //
			.append("coordinates", SINGE_POINT_CORDS);//

	// MultiPoint
	static final BasicDBList MULTI_POINT_CORDS = new BasicDbListBuilder() //
			.add(new BasicDbListBuilder().add(POINT_0.getX()).add(POINT_0.getY()).get()) //
			.add(new BasicDbListBuilder().add(POINT_2.getX()).add(POINT_2.getY()).get()) //
			.add(new BasicDbListBuilder().add(POINT_3.getX()).add(POINT_3.getY()).get()) //
			.get();
	static final Document MULTI_POINT_DOC = new Document() //
			.append("type", "MultiPoint")//
			.append("coordinates", MULTI_POINT_CORDS);//

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
	static final Document POLYGON_DOC = new Document() //
			.append("type", "Polygon") //
			.append("coordinates", POLYGON_CORDS); //

	static final BasicDBList POLYGON_WITH_2_RINGS_CORDS = new BasicDbListBuilder().add(POLYGON_OUTER_CORDS)
			.add(POLYGON_INNER_CORDS).get();
	static final Document POLYGON_WITH_2_RINGS_DOC = new Document() //
			.append("type", "Polygon") //
			.append("coordinates", POLYGON_WITH_2_RINGS_CORDS);

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
	static final Document LINE_STRING_DOC = new Document().append("type", "LineString").append("coordinates",
			LINE_STRING_CORDS_0);

	// MultiLineString
	static final BasicDBList MUILT_LINE_STRING_CORDS = new BasicDbListBuilder() //
			.add(LINE_STRING_CORDS_0) //
			.add(LINE_STRING_CORDS_1) //
			.get();
	static final Document MULTI_LINE_STRING_DOC = new Document().append("type", "MultiLineString").append("coordinates",
			MUILT_LINE_STRING_CORDS);

	// MultiPolygoin
	static final BasicDBList MULTI_POLYGON_CORDS = new BasicDbListBuilder().add(POLYGON_CORDS).get();
	static final Document MULTI_POLYGON_DOC = new Document().append("type", "MultiPolygon").append("coordinates",
			MULTI_POLYGON_CORDS);

	// GeometryCollection
	static final BasicDBList GEOMETRY_COLLECTION_GEOMETRIES = new BasicDbListBuilder() //
			.add(SINGLE_POINT_DOC)//
			.add(POLYGON_DOC)//
			.get();
	static final Document GEOMETRY_COLLECTION_DOC = new Document().append("type", "GeometryCollection")
			.append("geometries", GEOMETRY_COLLECTION_GEOMETRIES);

	/**
	 * @author Christoph Strobl
	 */
	public static class DocumentToGeoJsonPolygonConverterUnitTests {

		DocumentToGeoJsonPolygonConverter converter = DocumentToGeoJsonPolygonConverter.INSTANCE;

		@Test // DATAMONGO-1137
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(POLYGON_DOC)).isEqualTo(POLYGON);
		}

		@Test // DATAMONGO-1137
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null)).isNull();
		}

		@Test // DATAMONGO-1137
		public void shouldThrowExceptionWhenTypeDoesNotMatchPolygon() {
			assertThatIllegalArgumentException().isThrownBy(() -> converter.convert(new Document("type", "YouDontKonwMe")));
		}

		@Test // DATAMONGO-1399
		public void shouldConvertDboWithMultipleRingsCorrectly() {
			assertThat(converter.convert(POLYGON_WITH_2_RINGS_DOC)).isEqualTo(POLYGON_WITH_2_RINGS);
		}

	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DocumentToGeoJsonPointConverterUnitTests {

		DocumentToGeoJsonPointConverter converter = DocumentToGeoJsonPointConverter.INSTANCE;

		@Test // DATAMONGO-1137
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(SINGLE_POINT_DOC)).isEqualTo(SINGLE_POINT);
		}

		@Test // DATAMONGO-1137
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null)).isNull();
		}

		@Test // DATAMONGO-1137
		public void shouldThrowExceptionWhenTypeDoesNotMatchPoint() {

			assertThatIllegalArgumentException().isThrownBy(() -> converter.convert(new Document("type", "YouDontKonwMe")));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DocumentToGeoJsonLineStringConverterUnitTests {

		DocumentToGeoJsonLineStringConverter converter = DocumentToGeoJsonLineStringConverter.INSTANCE;

		@Test // DATAMONGO-1137
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(LINE_STRING_DOC)).isEqualTo(LINE_STRING);
		}

		@Test // DATAMONGO-1137
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null)).isNull();
		}

		@Test // DATAMONGO-1137
		public void shouldThrowExceptionWhenTypeDoesNotMatchPoint() {
			assertThatIllegalArgumentException().isThrownBy(() -> converter.convert(new Document("type", "YouDontKonwMe")));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DocumentToGeoJsonMultiLineStringConverterUnitTests {

		DocumentToGeoJsonMultiLineStringConverter converter = DocumentToGeoJsonMultiLineStringConverter.INSTANCE;

		@Test // DATAMONGO-1137
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(MULTI_LINE_STRING_DOC)).isEqualTo(MULTI_LINE_STRING);
		}

		@Test // DATAMONGO-1137
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null)).isNull();
		}

		@Test // DATAMONGO-1137
		public void shouldThrowExceptionWhenTypeDoesNotMatchPoint() {
			assertThatIllegalArgumentException().isThrownBy(() -> converter.convert(new Document("type", "YouDontKonwMe")));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DocumentToGeoJsonMultiPointConverterUnitTests {

		DocumentToGeoJsonMultiPointConverter converter = DocumentToGeoJsonMultiPointConverter.INSTANCE;

		@Test // DATAMONGO-1137
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(MULTI_POINT_DOC)).isEqualTo(MULTI_POINT);
		}

		@Test // DATAMONGO-1137
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null)).isNull();
		}

		@Test // DATAMONGO-1137
		public void shouldThrowExceptionWhenTypeDoesNotMatchPoint() {
			assertThatIllegalArgumentException().isThrownBy(() -> converter.convert(new Document("type", "YouDontKonwMe")));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DocumentToGeoJsonMultiPolygonConverterUnitTests {

		DocumentToGeoJsonMultiPolygonConverter converter = DocumentToGeoJsonMultiPolygonConverter.INSTANCE;

		@Test // DATAMONGO-1137
		public void shouldConvertDboCorrectly() {
			assertThat(converter.convert(MULTI_POLYGON_DOC)).isEqualTo(MULTI_POLYGON);
		}

		@Test // DATAMONGO-1137
		public void shouldReturnNullWhenConvertIsGivenNull() {
			assertThat(converter.convert(null)).isNull();
		}

		@Test // DATAMONGO-1137
		public void shouldThrowExceptionWhenTypeDoesNotMatchPoint() {
			assertThatIllegalArgumentException().isThrownBy(() -> converter.convert(new Document("type", "YouDontKonwMe")));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class GeoJsonToDocumentConverterUnitTests {

		GeoJsonToDocumentConverter converter = GeoJsonToDocumentConverter.INSTANCE;

		// DATAMONGO-1135
		public void convertShouldReturnNullWhenGivenNull() {
			assertThat(converter.convert(null)).isNull();
		}

		@Test // DATAMONGO-1135
		public void shouldConvertGeoJsonPointCorrectly() {
			assertThat(converter.convert(SINGLE_POINT)).isEqualTo(SINGLE_POINT_DOC);
		}

		@Test // DATAMONGO-1135
		public void shouldConvertGeoJsonPolygonCorrectly() {
			assertThat(converter.convert(POLYGON)).isEqualTo(POLYGON_DOC);
		}

		@Test // DATAMONGO-1137
		public void shouldConvertGeoJsonLineStringCorrectly() {
			assertThat(converter.convert(LINE_STRING)).isEqualTo(LINE_STRING_DOC);
		}

		@Test // DATAMONGO-1137
		public void shouldConvertGeoJsonMultiLineStringCorrectly() {
			assertThat(converter.convert(MULTI_LINE_STRING)).isEqualTo(MULTI_LINE_STRING_DOC);
		}

		@Test // DATAMONGO-1137
		public void shouldConvertGeoJsonMultiPointCorrectly() {
			assertThat(converter.convert(MULTI_POINT)).isEqualTo(MULTI_POINT_DOC);
		}

		@Test // DATAMONGO-1137
		public void shouldConvertGeoJsonMultiPolygonCorrectly() {
			assertThat(converter.convert(MULTI_POLYGON)).isEqualTo(MULTI_POLYGON_DOC);
		}

		@Test // DATAMONGO-1137
		public void shouldConvertGeometryCollectionCorrectly() {
			assertThat(converter.convert(GEOMETRY_COLLECTION)).isEqualTo(GEOMETRY_COLLECTION_DOC);
		}

		@Test // DATAMONGO-1399
		public void shouldConvertGeoJsonPolygonWithMultipleRingsCorrectly() {
			assertThat(converter.convert(POLYGON_WITH_2_RINGS)).isEqualTo(POLYGON_WITH_2_RINGS_DOC);
		}
	}
}
