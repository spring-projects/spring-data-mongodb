/*
 * Copyright 2015-2023 the original author or authors.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.geo.Point;
import org.springframework.lang.Nullable;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * A Jackson {@link Module} to register custom {@link JsonDeserializer}s for GeoJSON types.
 * <br />
 * Use {@link #geoJsonModule()} to obtain a {@link Module} containing both {@link JsonSerializer serializers} and
 * {@link JsonDeserializer deserializers}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 * @since 1.7
 */
public class GeoJsonModule extends SimpleModule {

	private static final long serialVersionUID = -8723016728655643720L;

	public GeoJsonModule() {

		registerDeserializersIn(this);
		// TODO: add serializers as of next major version (4.0).
	}

	/**
	 * Obtain a {@link Module} containing {@link JsonDeserializer deserializers} for the following {@link GeoJson} types:
	 * <ul>
	 * <li>{@link GeoJsonPoint}</li>
	 * <li>{@link GeoJsonMultiPoint}</li>
	 * <li>{@link GeoJsonLineString}</li>
	 * <li>{@link GeoJsonMultiLineString}</li>
	 * <li>{@link GeoJsonPolygon}</li>
	 * <li>{@link GeoJsonMultiPolygon}</li>
	 * </ul>
	 *
	 * @return a {@link Module} containing {@link JsonDeserializer deserializers} for {@link GeoJson} types.
	 * @since 3.2
	 */
	public static Module deserializers() {

		SimpleModule module = new SimpleModule("Spring Data MongoDB GeoJson - Deserializers",
				new Version(3, 2, 0, null, "org.springframework.data", "spring-data-mongodb-geojson"));
		registerDeserializersIn(module);
		return module;
	}

	/**
	 * Obtain a {@link Module} containing {@link JsonSerializer serializers} for the following {@link GeoJson} types:
	 * <ul>
	 * <li>{@link GeoJsonPoint}</li>
	 * <li>{@link GeoJsonMultiPoint}</li>
	 * <li>{@link GeoJsonLineString}</li>
	 * <li>{@link GeoJsonMultiLineString}</li>
	 * <li>{@link GeoJsonPolygon}</li>
	 * <li>{@link GeoJsonMultiPolygon}</li>
	 * </ul>
	 *
	 * @return a {@link Module} containing {@link JsonSerializer serializers} for {@link GeoJson} types.
	 * @since 3.2
	 */
	public static Module serializers() {

		SimpleModule module = new SimpleModule("Spring Data MongoDB GeoJson - Serializers",
				new Version(3, 2, 0, null, "org.springframework.data", "spring-data-mongodb-geojson"));
		GeoJsonSerializersModule.registerSerializersIn(module);
		return module;
	}

	/**
	 * Obtain a {@link Module} containing {@link JsonSerializer serializers} and {@link JsonDeserializer deserializers}
	 * for the following {@link GeoJson} types:
	 * <ul>
	 * <li>{@link GeoJsonPoint}</li>
	 * <li>{@link GeoJsonMultiPoint}</li>
	 * <li>{@link GeoJsonLineString}</li>
	 * <li>{@link GeoJsonMultiLineString}</li>
	 * <li>{@link GeoJsonPolygon}</li>
	 * <li>{@link GeoJsonMultiPolygon}</li>
	 * </ul>
	 *
	 * @return a {@link Module} containing {@link JsonSerializer serializers} and {@link JsonDeserializer deserializers}
	 *         for {@link GeoJson} types.
	 * @since 3.2
	 */
	public static Module geoJsonModule() {

		SimpleModule module = new SimpleModule("Spring Data MongoDB GeoJson",
				new Version(3, 2, 0, null, "org.springframework.data", "spring-data-mongodb-geojson"));
		GeoJsonSerializersModule.registerSerializersIn(module);
		registerDeserializersIn(module);
		return module;
	}

	private static void registerDeserializersIn(SimpleModule module) {

		module.addDeserializer(GeoJsonPoint.class, new GeoJsonPointDeserializer());
		module.addDeserializer(GeoJsonMultiPoint.class, new GeoJsonMultiPointDeserializer());
		module.addDeserializer(GeoJsonLineString.class, new GeoJsonLineStringDeserializer());
		module.addDeserializer(GeoJsonMultiLineString.class, new GeoJsonMultiLineStringDeserializer());
		module.addDeserializer(GeoJsonPolygon.class, new GeoJsonPolygonDeserializer());
		module.addDeserializer(GeoJsonMultiPolygon.class, new GeoJsonMultiPolygonDeserializer());
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	private static abstract class GeoJsonDeserializer<T extends GeoJson<?>> extends JsonDeserializer<T> {

		@Nullable
		@Override
		public T deserialize(@Nullable JsonParser jp, @Nullable DeserializationContext ctxt) throws IOException {

			JsonNode node = jp.readValueAsTree();
			JsonNode coordinates = node.get("coordinates");

			if (coordinates != null && coordinates.isArray()) {
				return doDeserialize((ArrayNode) coordinates);
			}
			return null;
		}

		/**
		 * Perform the actual deserialization given the {@literal coordinates} as {@link ArrayNode}.
		 *
		 * @param coordinates
		 * @return
		 */
		@Nullable
		protected abstract T doDeserialize(ArrayNode coordinates);

		/**
		 * Get the {@link GeoJsonPoint} representation of given {@link ArrayNode} assuming {@code node.[0]} represents
		 * {@literal x - coordinate} and {@code node.[1]} is {@literal y}.
		 *
		 * @param node can be {@literal null}.
		 * @return {@literal null} when given a {@code null} value.
		 */
		@Nullable
		protected GeoJsonPoint toGeoJsonPoint(@Nullable ArrayNode node) {

			if (node == null) {
				return null;
			}

			return new GeoJsonPoint(node.get(0).asDouble(), node.get(1).asDouble());
		}

		/**
		 * Get the {@link Point} representation of given {@link ArrayNode} assuming {@code node.[0]} represents
		 * {@literal x - coordinate} and {@code node.[1]} is {@literal y}.
		 *
		 * @param node can be {@literal null}.
		 * @return {@literal null} when given a {@code null} value.
		 */
		@Nullable
		protected Point toPoint(@Nullable ArrayNode node) {

			if (node == null) {
				return null;
			}

			return new Point(node.get(0).asDouble(), node.get(1).asDouble());
		}

		/**
		 * Get the points nested within given {@link ArrayNode}.
		 *
		 * @param node can be {@literal null}.
		 * @return {@literal empty list} when given a {@code null} value.
		 */
		protected List<Point> toPoints(@Nullable ArrayNode node) {

			if (node == null) {
				return Collections.emptyList();
			}

			List<Point> points = new ArrayList<>(node.size());

			for (JsonNode coordinatePair : node) {
				if (coordinatePair.isArray()) {
					points.add(toPoint((ArrayNode) coordinatePair));
				}
			}
			return points;
		}

		protected GeoJsonLineString toLineString(ArrayNode node) {
			return new GeoJsonLineString(toPoints(node));
		}
	}

	/**
	 * {@link JsonDeserializer} converting GeoJSON representation of {@literal Point}.
	 *
	 * <pre>
	 * <code>
	 * { "type": "Point", "coordinates": [10.0, 20.0] }
	 * </code>
	 * </pre>
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	private static class GeoJsonPointDeserializer extends GeoJsonDeserializer<GeoJsonPoint> {

		@Nullable
		@Override
		protected GeoJsonPoint doDeserialize(ArrayNode coordinates) {
			return toGeoJsonPoint(coordinates);
		}
	}

	/**
	 * {@link JsonDeserializer} converting GeoJSON representation of {@literal LineString}.
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
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	private static class GeoJsonLineStringDeserializer extends GeoJsonDeserializer<GeoJsonLineString> {

		@Override
		protected GeoJsonLineString doDeserialize(ArrayNode coordinates) {
			return new GeoJsonLineString(toPoints(coordinates));
		}
	}

	/**
	 * {@link JsonDeserializer} converting GeoJSON representation of {@literal MultiPoint}.
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
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	private static class GeoJsonMultiPointDeserializer extends GeoJsonDeserializer<GeoJsonMultiPoint> {

		@Override
		protected GeoJsonMultiPoint doDeserialize(ArrayNode coordinates) {
			return new GeoJsonMultiPoint(toPoints(coordinates));
		}
	}

	/**
	 * {@link JsonDeserializer} converting GeoJSON representation of {@literal MultiLineString}.
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
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	private static class GeoJsonMultiLineStringDeserializer extends GeoJsonDeserializer<GeoJsonMultiLineString> {

		@Override
		protected GeoJsonMultiLineString doDeserialize(ArrayNode coordinates) {

			List<GeoJsonLineString> lines = new ArrayList<>(coordinates.size());

			for (JsonNode lineString : coordinates) {
				if (lineString.isArray()) {
					lines.add(toLineString((ArrayNode) lineString));
				}
			}

			return new GeoJsonMultiLineString(lines);
		}
	}

	/**
	 * {@link JsonDeserializer} converting GeoJSON representation of {@literal Polygon}.
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
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	private static class GeoJsonPolygonDeserializer extends GeoJsonDeserializer<GeoJsonPolygon> {

		@Nullable
		@Override
		protected GeoJsonPolygon doDeserialize(ArrayNode coordinates) {

			for (JsonNode ring : coordinates) {

				// currently we do not support holes in polygons.
				return new GeoJsonPolygon(toPoints((ArrayNode) ring));
			}

			return null;
		}
	}

	/**
	 * {@link JsonDeserializer} converting GeoJSON representation of {@literal MultiPolygon}.
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
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	private static class GeoJsonMultiPolygonDeserializer extends GeoJsonDeserializer<GeoJsonMultiPolygon> {

		@Override
		protected GeoJsonMultiPolygon doDeserialize(ArrayNode coordinates) {

			List<GeoJsonPolygon> polygones = new ArrayList<>(coordinates.size());

			for (JsonNode polygon : coordinates) {
				for (JsonNode ring : polygon) {
					polygones.add(new GeoJsonPolygon(toPoints((ArrayNode) ring)));
				}
			}

			return new GeoJsonMultiPolygon(polygones);
		}
	}
}
