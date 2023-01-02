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

import java.io.IOException;

import org.springframework.data.geo.Point;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * A Jackson {@link Module} to register custom {@link JsonSerializer}s for GeoJSON types.
 *
 * @author Bjorn Harvold
 * @author Christoph Strobl
 * @since 3.2
 */
class GeoJsonSerializersModule extends SimpleModule {

	private static final long serialVersionUID = 1340494654898895610L;

	GeoJsonSerializersModule() {
		registerSerializersIn(this);
	}


	static void registerSerializersIn(SimpleModule module) {

		module.addSerializer(GeoJsonPoint.class, new GeoJsonPointSerializer());
		module.addSerializer(GeoJsonMultiPoint.class, new GeoJsonMultiPointSerializer());
		module.addSerializer(GeoJsonLineString.class, new GeoJsonLineStringSerializer());
		module.addSerializer(GeoJsonMultiLineString.class, new GeoJsonMultiLineStringSerializer());
		module.addSerializer(GeoJsonPolygon.class, new GeoJsonPolygonSerializer());
		module.addSerializer(GeoJsonMultiPolygon.class, new GeoJsonMultiPolygonSerializer());
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 */
	private static abstract class GeoJsonSerializer<T extends GeoJson<? extends Iterable>> extends JsonSerializer<T> {

		@Override
		public void serialize(T shape, JsonGenerator jsonGenerator, SerializerProvider serializers) throws IOException {

			jsonGenerator.writeStartObject();
			jsonGenerator.writeStringField("type", shape.getType());
			jsonGenerator.writeArrayFieldStart("coordinates");

			doSerialize(shape, jsonGenerator);

			jsonGenerator.writeEndArray();
			jsonGenerator.writeEndObject();
		}

		/**
		 * Perform the actual serialization given the {@literal shape} as {@link GeoJson}.
		 *
		 * @param shape
		 * @param jsonGenerator
		 * @return
		 */
		protected abstract void doSerialize(T shape, JsonGenerator jsonGenerator) throws IOException;

		/**
		 * Write a {@link Point} as array. <br />
		 * {@code [10.0, 20.0]}
		 *
		 * @param point
		 * @param jsonGenerator
		 * @throws IOException
		 */
		protected void writePoint(Point point, JsonGenerator jsonGenerator) throws IOException {

			jsonGenerator.writeStartArray();
			writeRawCoordinates(point, jsonGenerator);
			jsonGenerator.writeEndArray();
		}

		/**
		 * Write the {@link Point} coordinates. <br />
		 * {@code 10.0, 20.0}
		 *
		 * @param point
		 * @param jsonGenerator
		 * @throws IOException
		 */
		protected void writeRawCoordinates(Point point, JsonGenerator jsonGenerator) throws IOException {

			jsonGenerator.writeNumber(point.getX());
			jsonGenerator.writeNumber(point.getY());
		}

		/**
		 * Write an {@link Iterable} of {@link Point} as array. <br />
		 * {@code [ [10.0, 20.0], [30.0, 40.0], [50.0, 60.0] ]}
		 *
		 * @param points
		 * @param jsonGenerator
		 * @throws IOException
		 */
		protected void writeLine(Iterable<Point> points, JsonGenerator jsonGenerator) throws IOException {

			jsonGenerator.writeStartArray();
			writeRawLine(points, jsonGenerator);
			jsonGenerator.writeEndArray();
		}

		/**
		 * Write an {@link Iterable} of {@link Point}. <br />
		 * {@code [10.0, 20.0], [30.0, 40.0], [50.0, 60.0]}
		 *
		 * @param points
		 * @param jsonGenerator
		 * @throws IOException
		 */
		protected void writeRawLine(Iterable<Point> points, JsonGenerator jsonGenerator) throws IOException {

			for (Point point : points) {
				writePoint(point, jsonGenerator);
			}
		}
	}

	/**
	 * {@link JsonSerializer} converting {@link GeoJsonPoint} to:
	 *
	 * <pre>
	 * <code>
	 * { "type": "Point", "coordinates": [10.0, 20.0] }
	 * </code>
	 * </pre>
	 *
	 * @author Bjorn Harvold
	 * @author Christoph Strobl
	 * @since 3.2
	 */
	static class GeoJsonPointSerializer extends GeoJsonSerializer<GeoJsonPoint> {

		@Override
		protected void doSerialize(GeoJsonPoint value, JsonGenerator jsonGenerator) throws IOException {
			writeRawCoordinates(value, jsonGenerator);
		}
	}

	/**
	 * {@link JsonSerializer} converting {@link GeoJsonLineString} to:
	 *
	 * <pre>
	 * <code>
	 * {
	 *   "type": "LineString",
	 *   "coordinates": [
	 *     [10.0, 20.0], [30.0, 40.0], [50.0, 60.0]
	 *   ]
	 * }
	 * </code>
	 * </pre>
	 *
	 * @author Bjorn Harvold
	 * @author Christoph Strobl
	 * @since 3.2
	 */
	static class GeoJsonLineStringSerializer extends GeoJsonSerializer<GeoJsonLineString> {

		@Override
		protected void doSerialize(GeoJsonLineString value, JsonGenerator jsonGenerator) throws IOException {
			writeRawLine(value.getCoordinates(), jsonGenerator);
		}
	}

	/**
	 * {@link JsonSerializer} converting {@link GeoJsonMultiPoint} to:
	 *
	 * <pre>
	 * <code>
	 * {
	 *   "type": "MultiPoint",
	 *   "coordinates": [
	 *     [10.0, 20.0], [30.0, 40.0], [50.0, 60.0]
	 *   ]
	 * }
	 * </code>
	 * </pre>
	 *
	 * @author Bjorn Harvold
	 * @author Christoph Strobl
	 * @since 3.2
	 */
	static class GeoJsonMultiPointSerializer extends GeoJsonSerializer<GeoJsonMultiPoint> {

		@Override
		protected void doSerialize(GeoJsonMultiPoint value, JsonGenerator jsonGenerator) throws IOException {
			writeRawLine(value.getCoordinates(), jsonGenerator);
		}
	}

	/**
	 * {@link JsonSerializer} converting {@link GeoJsonMultiLineString} to:
	 *
	 * <pre>
	 * <code>
	 * {
	 *   "type": "MultiLineString",
	 *   "coordinates": [
	 *     [ [10.0, 20.0], [30.0, 40.0] ],
	 *     [ [50.0, 60.0] , [70.0, 80.0] ]
	 *   ]
	 * }
	 * </code>
	 * </pre>
	 *
	 * @author Bjorn Harvold
	 * @author Christoph Strobl
	 * @since 3.2
	 */
	static class GeoJsonMultiLineStringSerializer extends GeoJsonSerializer<GeoJsonMultiLineString> {

		@Override
		protected void doSerialize(GeoJsonMultiLineString value, JsonGenerator jsonGenerator) throws IOException {

			for (GeoJsonLineString lineString : value.getCoordinates()) {
				writeLine(lineString.getCoordinates(), jsonGenerator);
			}
		}
	}

	/**
	 * {@link JsonSerializer} converting {@link GeoJsonPolygon} to:
	 *
	 * <pre>
	 * <code>
	 * {
	 *   "type": "Polygon",
	 *   "coordinates": [
	 *     [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
	 *   ]
	 * }
	 * </code>
	 * </pre>
	 *
	 * @author Bjorn Harvold
	 * @author Christoph Strobl
	 * @since 3.2
	 */
	static class GeoJsonPolygonSerializer extends GeoJsonSerializer<GeoJsonPolygon> {

		@Override
		protected void doSerialize(GeoJsonPolygon value, JsonGenerator jsonGenerator) throws IOException {

			for (GeoJsonLineString lineString : value.getCoordinates()) {
				writeLine(lineString.getCoordinates(), jsonGenerator);
			}
		}
	}

	/**
	 * {@link JsonSerializer} converting {@link GeoJsonMultiPolygon} to:
	 *
	 * <pre>
	 * <code>
	 * {
	 *   "type": "MultiPolygon",
	 *   "coordinates": [
	 *     [[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],
	 *     [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
	 *     [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]
	 *   ]
	 * }
	 * </code>
	 * </pre>
	 *
	 * @author Bjorn Harvold
	 * @author Christoph Strobl
	 * @since 3.2
	 */
	static class GeoJsonMultiPolygonSerializer extends GeoJsonSerializer<GeoJsonMultiPolygon> {

		@Override
		protected void doSerialize(GeoJsonMultiPolygon value, JsonGenerator jsonGenerator) throws IOException {

			for (GeoJsonPolygon polygon : value.getCoordinates()) {

				jsonGenerator.writeStartArray();
				for (GeoJsonLineString lineString : polygon.getCoordinates()) {
					writeLine(lineString.getCoordinates(), jsonGenerator);
				}
				jsonGenerator.writeEndArray();
			}
		}
	}
}
