package org.springframework.data.mongodb.core.geo;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.Version;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.module.SimpleDeserializers;
import tools.jackson.databind.module.SimpleSerializers;
import tools.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.data.geo.Point;
import org.springframework.util.Assert;

public class GeoJsonJackson3Module {

	private static Version version = new Version(3, 2, 0, null, "org.springframework.data",
			"spring-data-mongodb-geojson");

	public static class Serializers extends JacksonModule {

		@Override
		public String getModuleName() {
			return "Spring Data MongoDB GeoJson - Serializers";
		}

		@Override
		public Version version() {

			return version;
		}

		@Override
		public void setupModule(SetupContext ctx) {

			final SimpleSerializers serializers = new SimpleSerializers();

			serializers.addSerializer(GeoJsonPoint.class, new GeoJsonPointSerializer());
			serializers.addSerializer(GeoJsonMultiPoint.class, new GeoJsonMultiPointSerializer());
			serializers.addSerializer(GeoJsonLineString.class, new GeoJsonLineStringSerializer());
			serializers.addSerializer(GeoJsonMultiLineString.class, new GeoJsonMultiLineStringSerializer());
			serializers.addSerializer(GeoJsonPolygon.class, new GeoJsonPolygonSerializer());
			serializers.addSerializer(GeoJsonMultiPolygon.class, new GeoJsonMultiPolygonSerializer());

			ctx.addSerializers(serializers);
		}
	}

	public static class Deserializers extends JacksonModule {

		@Override
		public String getModuleName() {
			return "Spring Data MongoDB GeoJson - Deserializers";
		}

		@Override
		public Version version() {
			return version;
		}

		@Override
		public void setupModule(SetupContext ctx) {

			final SimpleDeserializers deserializers = new SimpleDeserializers();

			deserializers.addDeserializer(GeoJsonPoint.class, new GeoJsonPointDeserializer());
			deserializers.addDeserializer(GeoJsonMultiPoint.class, new GeoJsonMultiPointDeserializer());
			deserializers.addDeserializer(GeoJsonLineString.class, new GeoJsonLineStringDeserializer());
			deserializers.addDeserializer(GeoJsonMultiLineString.class, new GeoJsonMultiLineStringDeserializer());
			deserializers.addDeserializer(GeoJsonPolygon.class, new GeoJsonPolygonDeserializer());
			deserializers.addDeserializer(GeoJsonMultiPolygon.class, new GeoJsonMultiPolygonDeserializer());

			ctx.addDeserializers(deserializers);
		}
	}

	private abstract static class GeoJsonDeserializer<T extends GeoJson<?>> extends ValueDeserializer<T> {

		public @Nullable T deserialize(JsonParser jp, @Nullable DeserializationContext context) throws JacksonException {

			JsonNode node = jp.readValueAsTree();
			JsonNode coordinates = node.get("coordinates");
			return coordinates != null && coordinates.isArray() ? this.doDeserialize((ArrayNode) coordinates) : null;
		}

		protected abstract @Nullable T doDeserialize(ArrayNode coordinates);

		protected @Nullable GeoJsonPoint toGeoJsonPoint(@Nullable ArrayNode node) {
			return node == null ? null : new GeoJsonPoint(node.get(0).asDouble(), node.get(1).asDouble());
		}

		protected @Nullable Point toPoint(@Nullable ArrayNode node) {
			return node == null ? null : new Point(node.get(0).asDouble(), node.get(1).asDouble());
		}

		protected List<Point> toPoints(@Nullable ArrayNode node) {

			if (node == null) {
				return Collections.emptyList();
			} else {
				List<Point> points = new ArrayList<>(node.size());

				for (JsonNode coordinatePair : node) {

					if (coordinatePair.isArray()) {

						Point point = this.toPoint((ArrayNode) coordinatePair);

						Assert.notNull(point, "Point must not be null!");

						points.add(point);
					}
				}

				return points;
			}
		}

		protected GeoJsonLineString toLineString(ArrayNode node) {
			return new GeoJsonLineString(this.toPoints(node));
		}
	}

	private static class GeoJsonPointDeserializer extends GeoJsonDeserializer<GeoJsonPoint> {

		protected @Nullable GeoJsonPoint doDeserialize(ArrayNode coordinates) {
			return this.toGeoJsonPoint(coordinates);
		}
	}

	private static class GeoJsonLineStringDeserializer extends GeoJsonDeserializer<GeoJsonLineString> {

		protected GeoJsonLineString doDeserialize(ArrayNode coordinates) {
			return new GeoJsonLineString(this.toPoints(coordinates));
		}
	}

	private static class GeoJsonMultiPointDeserializer extends GeoJsonDeserializer<GeoJsonMultiPoint> {

		protected GeoJsonMultiPoint doDeserialize(ArrayNode coordinates) {
			return new GeoJsonMultiPoint(this.toPoints(coordinates));
		}
	}

	private static class GeoJsonMultiLineStringDeserializer extends GeoJsonDeserializer<GeoJsonMultiLineString> {

		protected GeoJsonMultiLineString doDeserialize(ArrayNode coordinates) {
			List<GeoJsonLineString> lines = new ArrayList<>(coordinates.size());

			for (JsonNode lineString : coordinates) {
				if (lineString.isArray()) {
					lines.add(this.toLineString((ArrayNode) lineString));
				}
			}

			return new GeoJsonMultiLineString(lines);
		}
	}

	private static class GeoJsonPolygonDeserializer extends GeoJsonDeserializer<GeoJsonPolygon> {

		protected @Nullable GeoJsonPolygon doDeserialize(ArrayNode coordinates) {

			Iterator<JsonNode> coordinateIterator = coordinates.iterator();
			if (coordinateIterator.hasNext()) {

				JsonNode ring = coordinateIterator.next();
				return new GeoJsonPolygon(this.toPoints((ArrayNode) ring));

			} else {
				return null;
			}
		}
	}

	private static class GeoJsonMultiPolygonDeserializer extends GeoJsonDeserializer<GeoJsonMultiPolygon> {

		protected GeoJsonMultiPolygon doDeserialize(ArrayNode coordinates) {
			List<GeoJsonPolygon> polygons = new ArrayList<>(coordinates.size());

			for (JsonNode polygon : coordinates) {

				for (JsonNode ring : polygon) {
					polygons.add(new GeoJsonPolygon(this.toPoints((ArrayNode) ring)));
				}
			}

			return new GeoJsonMultiPolygon(polygons);
		}
	}

	private abstract static class GeoJsonSerializer<T extends GeoJson<? extends Iterable<?>>> extends ValueSerializer<T> {

		@Override
		public void serialize(T shape, JsonGenerator jsonGenerator, SerializationContext context) {

			jsonGenerator.writeStartObject();
			jsonGenerator.writeStringProperty("type", shape.getType());
			jsonGenerator.writeArrayPropertyStart("coordinates");
			this.doSerialize(shape, jsonGenerator);
			jsonGenerator.writeEndArray();
			jsonGenerator.writeEndObject();
		}

		protected abstract void doSerialize(T shape, JsonGenerator jsonGenerator);

		protected void writePoint(Point point, JsonGenerator jsonGenerator) {

			jsonGenerator.writeStartArray();
			this.writeRawCoordinates(point, jsonGenerator);
			jsonGenerator.writeEndArray();
		}

		protected void writeRawCoordinates(Point point, JsonGenerator jsonGenerator) {

			jsonGenerator.writeNumber(point.getX());
			jsonGenerator.writeNumber(point.getY());
		}

		protected void writeLine(Iterable<Point> points, JsonGenerator jsonGenerator) {

			jsonGenerator.writeStartArray();
			this.writeRawLine(points, jsonGenerator);
			jsonGenerator.writeEndArray();
		}

		protected void writeRawLine(Iterable<Point> points, JsonGenerator jsonGenerator) {
			for (Point point : points) {
				this.writePoint(point, jsonGenerator);
			}

		}
	}

	static class GeoJsonPointSerializer extends GeoJsonSerializer<GeoJsonPoint> {
		protected void doSerialize(GeoJsonPoint value, JsonGenerator jsonGenerator) {
			this.writeRawCoordinates(value, jsonGenerator);
		}
	}

	static class GeoJsonLineStringSerializer extends GeoJsonSerializer<GeoJsonLineString> {
		protected void doSerialize(GeoJsonLineString value, JsonGenerator jsonGenerator) {
			this.writeRawLine(value.getCoordinates(), jsonGenerator);
		}
	}

	static class GeoJsonMultiPointSerializer extends GeoJsonSerializer<GeoJsonMultiPoint> {
		protected void doSerialize(GeoJsonMultiPoint value, JsonGenerator jsonGenerator) {
			this.writeRawLine(value.getCoordinates(), jsonGenerator);
		}
	}

	static class GeoJsonMultiLineStringSerializer extends GeoJsonSerializer<GeoJsonMultiLineString> {
		protected void doSerialize(GeoJsonMultiLineString value, JsonGenerator jsonGenerator) {
			for (GeoJsonLineString lineString : value.getCoordinates()) {
				this.writeLine(lineString.getCoordinates(), jsonGenerator);
			}

		}
	}

	static class GeoJsonPolygonSerializer extends GeoJsonSerializer<GeoJsonPolygon> {
		protected void doSerialize(GeoJsonPolygon value, JsonGenerator jsonGenerator) throws JacksonException {
			for (GeoJsonLineString lineString : value.getCoordinates()) {
				this.writeLine(lineString.getCoordinates(), jsonGenerator);
			}

		}
	}

	static class GeoJsonMultiPolygonSerializer extends GeoJsonSerializer<GeoJsonMultiPolygon> {
		protected void doSerialize(GeoJsonMultiPolygon value, JsonGenerator jsonGenerator) throws JacksonException {
			for (GeoJsonPolygon polygon : value.getCoordinates()) {
				jsonGenerator.writeStartArray();

				for (GeoJsonLineString lineString : polygon.getCoordinates()) {
					this.writeLine(lineString.getCoordinates(), jsonGenerator);
				}

				jsonGenerator.writeEndArray();
			}

		}
	}
}
