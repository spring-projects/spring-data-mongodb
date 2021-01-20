/*
 * Copyright 2015-2021 the original author or authors.
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.springframework.data.geo.Point;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A Jackson {@link Module} to register custom {@link JsonSerializer}s for GeoJSON types.
 *
 * @author Bjorn Harvold
 * @since 
 */
public class GeoJsonSerializersModule extends SimpleModule {

	private static final long serialVersionUID = 1340494654898895610L;

	public GeoJsonSerializersModule() {
		addSerializer(GeoJsonPoint.class, new GeoJsonPointSerializer());
		addSerializer(GeoJsonMultiPoint.class, new GeoJsonMultiPointSerializer());
		addSerializer(GeoJsonLineString.class, new GeoJsonLineStringSerializer());
		addSerializer(GeoJsonMultiLineString.class, new GeoJsonMultiLineStringSerializer());
		addSerializer(GeoJsonPolygon.class, new GeoJsonPolygonSerializer());
		addSerializer(GeoJsonMultiPolygon.class, new GeoJsonMultiPolygonSerializer());
	}

	public static class GeoJsonPointSerializer extends JsonSerializer<GeoJsonPoint> {
		@Override
		public void serialize(GeoJsonPoint value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
			gen.writeStartObject();
			gen.writeStringField("type", value.getType());
			gen.writeObjectField("coordinates", value.getCoordinates());
			gen.writeEndObject();
		}

	}

	public static class GeoJsonLineStringSerializer extends JsonSerializer<GeoJsonLineString> {

		@Override
		public void serialize(GeoJsonLineString value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
			gen.writeStartObject();
			gen.writeStringField("type", value.getType());
			gen.writeArrayFieldStart("coordinates");
			for (Point p : value.getCoordinates()) {
				gen.writeObject(new double[]{p.getX(), p.getY()});
			}
			gen.writeEndArray();
			gen.writeEndObject();
		}
	}

	public static class GeoJsonMultiPointSerializer extends JsonSerializer<GeoJsonMultiPoint> {

		@Override
		public void serialize(GeoJsonMultiPoint value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
			gen.writeStartObject();
			gen.writeStringField("type", value.getType());
			gen.writeArrayFieldStart("coordinates");
			for (Point p : value.getCoordinates()) {
				gen.writeObject(new double[]{p.getX(), p.getY()});
			}
			gen.writeEndArray();
			gen.writeEndObject();
		}
	}

	public static class GeoJsonMultiLineStringSerializer extends JsonSerializer<GeoJsonMultiLineString> {

		@Override
		public void serialize(GeoJsonMultiLineString value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
			gen.writeStartObject();
			gen.writeStringField("type", value.getType());
			gen.writeArrayFieldStart("coordinates");
			for (GeoJsonLineString lineString : value.getCoordinates()) {
				List<double[]> arrayList = new ArrayList<>();
				for (Point p : lineString.getCoordinates()) {
					arrayList.add(new double[]{p.getX(), p.getY()});
				}
				double[][] doubles = arrayList.toArray(new double[0][0]);
				gen.writeObject(doubles);
			}
			gen.writeEndArray();
			gen.writeEndObject();
		}
	}

	public static class GeoJsonPolygonSerializer extends JsonSerializer<GeoJsonPolygon> {

		@Override
		public void serialize(GeoJsonPolygon value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
			gen.writeStartObject();
			gen.writeStringField("type", value.getType());
			gen.writeArrayFieldStart("coordinates");
			for (GeoJsonLineString ls : value.getCoordinates()) {
				gen.writeStartArray();
				for (Point p : ls.getCoordinates()) {
					gen.writeObject(new double[]{p.getX(), p.getY()});
				}
				gen.writeEndArray();
			}
			gen.writeEndArray();
			gen.writeEndObject();
		}
	}

	public static class GeoJsonMultiPolygonSerializer extends JsonSerializer<GeoJsonMultiPolygon> {

		@Override
		public void serialize(GeoJsonMultiPolygon value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
			gen.writeStartObject();
			gen.writeStringField("type", value.getType());
			gen.writeArrayFieldStart("coordinates");
			for (GeoJsonPolygon polygon : value.getCoordinates()) {

				gen.writeStartArray();

				gen.writeStartArray();
				for (GeoJsonLineString lineString : polygon.getCoordinates()) {

					for (Point p : lineString.getCoordinates()) {
						gen.writeObject(new double[]{p.getX(), p.getY()});
					}

				}

				gen.writeEndArray();
				gen.writeEndArray();
			}
			gen.writeEndArray();
			gen.writeEndObject();
		}
	}
}
