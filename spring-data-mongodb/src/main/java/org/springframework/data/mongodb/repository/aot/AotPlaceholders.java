/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;
import org.springframework.data.mongodb.core.geo.GeoJson;
import org.springframework.data.mongodb.core.geo.Sphere;

/**
 * Placeholders for AOT processing of MongoDB queries.
 *
 * @author Mark Paluch
 * @since 5.0
 */
class AotPlaceholders {

	/**
	 * Create a new placeholder using positional binding markers.
	 *
	 * @param position the index of the parameter to bind.
	 * @return new instance of {@link Placeholder}.
	 */
	static Placeholder indexed(int position) {
		return new PlaceholderImpl("?" + position);
	}

	/**
	 * Create a placeholder for a GeoJSON object.
	 *
	 * @param index zero-based index referring to the bindable method parameter.
	 * @param type
	 * @return
	 */
	static Shape geoJson(int index, String type) {
		return new GeoJsonPlaceholder(index, type);
	}

	/**
	 * Create a placeholder for a {@link Point} object.
	 *
	 * @param index zero-based index referring to the bindable method parameter.
	 * @return
	 */
	static Point point(int index) {
		return new PointPlaceholder(index);
	}

	/**
	 * Create a placeholder for a {@link Circle} object.
	 *
	 * @param index zero-based index referring to the bindable method parameter.
	 * @return
	 */
	static Shape circle(int index) {
		return new CirclePlaceholder(index);
	}

	/**
	 * Create a placeholder for a {@link Box} object.
	 *
	 * @param index zero-based index referring to the bindable method parameter.
	 * @return
	 */
	static Shape box(int index) {
		return new BoxPlaceholder(index);
	}

	/**
	 * Create a placeholder for a {@link Sphere} object.
	 *
	 * @param index zero-based index referring to the bindable method parameter.
	 * @return
	 */
	static Shape sphere(int index) {
		return new SpherePlaceholder(index);
	}

	/**
	 * Create a placeholder for a {@link Polygon} object.
	 *
	 * @param index zero-based index referring to the bindable method parameter.
	 * @return
	 */
	static Shape polygon(int index) {
		return new PolygonPlaceholder(index);
	}

	static RegexPlaceholder regex(int index, @Nullable String options) {
		return new RegexPlaceholder(index, options);
	}

	/**
	 * Create a placeholder that indicates the value should be treated as list.
	 *
	 * @param index zero-based index referring to the bindable method parameter.
	 * @return new instance of {@link Placeholder}.
	 */
	static Placeholder asList(int index) {
		return asList(indexed(index));
	}

	/**
	 * Create a placeholder that indicates the wrapped placeholder should be treated as list.
	 *
	 * @param source the target placeholder
	 * @return new instance of {@link Placeholder}.
	 */
	static Placeholder asList(Placeholder source) {
		return new AsListPlaceholder(source);
	}

	/**
	 * A placeholder expression used when rending queries to JSON.
	 *
	 * @since 5.0
	 * @author Christoph Strobl
	 */
	interface Placeholder {

		String getValue();
	}

	/**
	 * @author Christoph Strobl
	 * @since 5.0
	 */
	record PlaceholderImpl(String expression) implements AotPlaceholders.Placeholder {

		@Override
		public String getValue() {
			return expression;
		}

		public String toString() {
			return getValue();
		}

	}

	private static class PointPlaceholder extends Point implements Placeholder {

		private final int index;

		PointPlaceholder(int index) {
			super(Double.NaN, Double.NaN);
			this.index = index;
		}

		@Override
		public String getValue() {
			return "?" + index;
		}

		@Override
		public String toString() {
			return getValue();
		}

	}

	private record GeoJsonPlaceholder(int index, String type) implements Placeholder, GeoJson<List<Placeholder>>, Shape {

		@Override
		public String getValue() {
			return "?" + index;
		}

		@Override
		public String getType() {
			return type();
		}

		@Override
		public String toString() {
			return getValue();
		}

		@Override
		public List<Placeholder> getCoordinates() {
			return List.of();
		}

	}

	private static class CirclePlaceholder extends Circle implements Placeholder {

		private final int index;

		CirclePlaceholder(int index) {
			super(new PointPlaceholder(index), Distance.of(1, Metrics.NEUTRAL)); //
			this.index = index;
		}

		@Override
		public String getValue() {
			return "?" + index;
		}

		@Override
		public String toString() {
			return getValue();
		}

	}

	private static class BoxPlaceholder extends Box implements Placeholder {

		private final int index;

		BoxPlaceholder(int index) {
			super(new PointPlaceholder(index), new PointPlaceholder(index));
			this.index = index;
		}

		@Override
		public String getValue() {
			return "?" + index;
		}

		@Override
		public String toString() {
			return getValue();
		}

	}

	private static class SpherePlaceholder extends Sphere implements Placeholder {

		private final int index;

		SpherePlaceholder(int index) {
			super(new PointPlaceholder(index), Distance.of(1, Metrics.NEUTRAL)); //
			this.index = index;
		}

		@Override
		public String getValue() {
			return "?" + index;
		}

		@Override
		public String toString() {
			return getValue();
		}

	}

	private static class PolygonPlaceholder extends Polygon implements Placeholder {

		private final int index;

		PolygonPlaceholder(int index) {
			super(new PointPlaceholder(index), new PointPlaceholder(index), new PointPlaceholder(index),
					new PointPlaceholder(index));
			this.index = index;
		}

		@Override
		public String getValue() {
			return "?" + index;
		}

		@Override
		public String toString() {
			return getValue();
		}

	}

	static class RegexPlaceholder implements Placeholder {

		private final int index;
		private final @Nullable String options;

		RegexPlaceholder(int index, @Nullable String options) {
			this.index = index;
			this.options = options;
		}

		@Nullable
		String regexOptions() {
			return options;
		}

		@Override
		public String getValue() {
			return "?" + index;
		}

		@Override
		public String toString() {
			return getValue();
		}
	}

	record AsListPlaceholder(Placeholder placeholder) implements Placeholder {

		@Override
		public String toString() {
			return getValue();
		}

		@Override
		public String getValue() {
			return "[" + placeholder.getValue() + "]";
		}
	}

}
