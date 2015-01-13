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
package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.geo.GeoJson.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.convert.GeoConverters.GeoJsonToDbObjectConverter;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
 * @since 1.7
 */
public class GeoJsonToDbObjectConverterUnitTests {

	GeoJsonToDbObjectConverter converter = GeoJsonToDbObjectConverter.INSTANCE;

	static final Point SINGLE_POINT = new Point(100, 50);

	static final Point POLYGON_0 = new Point(0, 0);
	static final Point POLYGON_1 = new Point(100, 0);
	static final Point POLYGON_2 = new Point(100, 100);
	static final Point POLYGON_3 = new Point(0, 100);

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

		DBObject point = converter.convert(point(SINGLE_POINT));

		BasicDBList values = new BasicDBList();
		values.add(new double[] { SINGLE_POINT.getX(), SINGLE_POINT.getY() });

		assertThat(point,
				isBsonObject().containing("$geometry.coordinates", Arrays.asList(SINGLE_POINT.getX(), SINGLE_POINT.getY())));
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void shouldConvertGeoJsonBoxCorrectly() {

		DBObject dbo = converter.convert(polygon(new Box(POLYGON_0, POLYGON_2)));

		validatePolygon(dbo, POLYGON_0, POLYGON_1, POLYGON_2, POLYGON_3, POLYGON_0);
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void shouldConvertGeoJsonPolygonCorrectly() {

		DBObject dbo = converter.convert(polygon(POLYGON_0, POLYGON_1, POLYGON_2, POLYGON_3, POLYGON_0));

		validatePolygon(dbo, POLYGON_0, POLYGON_1, POLYGON_2, POLYGON_3, POLYGON_0);
	}

	/**
	 * @see DATAMONGO-1135
	 */
	@Test
	public void shouldAddMissingClosingPointWhenConvertGeoJsonPolygon() {

		DBObject dbo = converter.convert(polygon(POLYGON_0, POLYGON_1, POLYGON_2, POLYGON_3));

		validatePolygon(dbo, POLYGON_0, POLYGON_1, POLYGON_2, POLYGON_3, POLYGON_0);
	}

	@SuppressWarnings("unchecked")
	void validatePolygon(DBObject dbo, Point... expectedCoordinates) {

		DBObject $geometry = DBObjectTestUtils.getAsDBObject(dbo, "$geometry");
		assertThat(DBObjectTestUtils.getTypedValue($geometry, "type", String.class), equalTo("Polygon"));

		BasicDBList coordinates = DBObjectTestUtils.getAsDBList($geometry, "coordinates");
		BasicDBList values = (BasicDBList) coordinates.get(0);

		for (int i = 0; i < values.size(); i++) {
			assertThat((List<Double>) values.get(i), hasItems(expectedCoordinates[i].getX(), expectedCoordinates[i].getY()));
		}
	}
}
