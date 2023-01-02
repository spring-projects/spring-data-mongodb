/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.geo;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.geo.Point;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for {@link GeoJsonSerializersModule}.
 *
 * @author Bjorn Harvold
 * @author Christoph Strobl
 */
class GeoJsonSerializersUnitTests {

	private ObjectMapper mapper;

	@BeforeEach
	void beforeEach() {

		mapper = new ObjectMapper();
		mapper.registerModule(new GeoJsonSerializersModule());
	}

	@Test // GH-3517
	void shouldSerializeJsonPointCorrectly() throws IOException {

		GeoJsonPoint geoJsonPoint = new GeoJsonPoint(10D, 20D);

		assertThat(mapper.writeValueAsString(geoJsonPoint)).isEqualTo("{\"type\":\"Point\",\"coordinates\":[10.0,20.0]}");
	}

	@Test // GH-3517
	void shouldSerializeGeoJsonLineStringCorrectly() throws IOException {

		GeoJsonLineString lineString = new GeoJsonLineString(
				Arrays.asList(new Point(10, 20), new Point(30, 40), new Point(50, 60)));

		assertThat(mapper.writeValueAsString(lineString))
				.isEqualTo("{\"type\":\"LineString\",\"coordinates\":[[10.0,20.0],[30.0,40.0],[50.0,60.0]]}");
	}

	@Test // GH-3517
	void shouldSerializeGeoJsonMultiPointCorrectly() throws IOException {

		GeoJsonMultiPoint multiPoint = new GeoJsonMultiPoint(
				Arrays.asList(new Point(10, 20), new Point(30, 40), new Point(50, 60)));

		assertThat(mapper.writeValueAsString(multiPoint))
				.isEqualTo("{\"type\":\"MultiPoint\",\"coordinates\":[[10.0,20.0],[30.0,40.0],[50.0,60.0]]}");
	}

	@Test // GH-3517
	void shouldSerializeJsonMultiLineStringCorrectly() throws IOException {

		GeoJsonMultiLineString multiLineString = new GeoJsonMultiLineString(
				Arrays.asList(new Point(10, 20), new Point(30, 40)), Arrays.asList(new Point(50, 60), new Point(70, 80)));

		assertThat(mapper.writeValueAsString(multiLineString)).isEqualTo(
				"{\"type\":\"MultiLineString\",\"coordinates\":[[[10.0,20.0],[30.0,40.0]],[[50.0,60.0],[70.0,80.0]]]}");
	}

	@Test // GH-3517
	void shouldSerializeGeoJsonPolygonCorrectly() throws IOException {

		List<Point> points = Arrays.asList(new Point(100, 0), new Point(101, 0), new Point(101, 1), new Point(100, 1),
				new Point(100, 0));
		GeoJsonPolygon polygon = new GeoJsonPolygon(points);

		assertThat(mapper.writeValueAsString(polygon)).isEqualTo(
				"{\"type\":\"Polygon\",\"coordinates\":[[[100.0,0.0],[101.0,0.0],[101.0,1.0],[100.0,1.0],[100.0,0.0]]]}");
	}

	@Test // GH-3517
	void shouldSerializeGeoJsonMultiPolygonCorrectly() throws IOException {

		String json = "{\"type\":\"MultiPolygon\",\"coordinates\":[" + "[" + "["
				+ "[102.0,2.0],[103.0,2.0],[103.0,3.0],[102.0,3.0],[102.0,2.0]" + "]" + "]," + "[" + "["
				+ "[100.0,0.0],[101.0,0.0],[101.0,1.0],[100.0,1.0],[100.0,0.0]" + "]" + "]," + "[" + "["
				+ "[100.2,0.2],[100.8,0.2],[100.8,0.8],[100.2,0.8],[100.2,0.2]" + "]" + "]" + "]" + "}";

		GeoJsonMultiPolygon multiPolygon = new GeoJsonMultiPolygon(Arrays.asList(
				new GeoJsonPolygon(Arrays.asList(new Point(102, 2), new Point(103, 2), new Point(103, 3), new Point(102, 3),
						new Point(102, 2))),
				new GeoJsonPolygon(Arrays.asList(new Point(100, 0), new Point(101, 0), new Point(101, 1), new Point(100, 1),
						new Point(100, 0))),
				new GeoJsonPolygon(Arrays.asList(new Point(100.2, 0.2), new Point(100.8, 0.2), new Point(100.8, 0.8),
						new Point(100.2, 0.8), new Point(100.2, 0.2)))));

		assertThat(mapper.writeValueAsString(multiPolygon)).isEqualTo(json);
	}
}
