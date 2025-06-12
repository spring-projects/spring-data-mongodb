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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.conversions.Bson;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.geo.Shape;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.geo.GeoJson;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.CriteriaDefinition.Placeholder;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor;
import org.springframework.data.mongodb.repository.query.MongoParameterAccessor;
import org.springframework.data.mongodb.repository.query.MongoQueryCreator;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.ClassUtils;

import com.mongodb.DBRef;

/**
 * @author Christoph Strobl
 * @since 5.0
 */
class AotQueryCreator {

	private MongoMappingContext mappingContext;

	public AotQueryCreator() {

		MongoMappingContext mongoMappingContext = new MongoMappingContext();
		mongoMappingContext.setSimpleTypeHolder(
				MongoCustomConversions.create((cfg) -> cfg.useNativeDriverJavaTimeCodecs()).getSimpleTypeHolder());
		mongoMappingContext.setAutoIndexCreation(false);
		mongoMappingContext.afterPropertiesSet();

		this.mappingContext = mongoMappingContext;
	}

	@SuppressWarnings("NullAway")
	StringQuery createQuery(PartTree partTree, QueryMethod queryMethod) {


		boolean geoNear = queryMethod instanceof MongoQueryMethod mqm ? mqm.isGeoNearQuery() : false;

		Query query = new MongoQueryCreator(partTree,
				new PlaceholderConvertingParameterAccessor(new PlaceholderParameterAccessor(queryMethod)), mappingContext, geoNear, queryMethod.isSearchQuery())
				.createQuery();

		if (partTree.isLimiting()) {
			query.limit(partTree.getMaxResults());
		}
		return new StringQuery(query);
	}

	static class PlaceholderConvertingParameterAccessor extends ConvertingParameterAccessor {

		/**
		 * Creates a new {@link ConvertingParameterAccessor} with the given {@link MongoWriter} and delegate.
		 *
		 * @param delegate must not be {@literal null}.
		 */
		public PlaceholderConvertingParameterAccessor(PlaceholderParameterAccessor delegate) {
			super(PlaceholderWriter.INSTANCE, delegate);
		}
	}

	@NullUnmarked
	enum PlaceholderWriter implements MongoWriter<Object> {

		INSTANCE;

		@Override
		public @Nullable Object convertToMongoType(@Nullable Object obj, @Nullable TypeInformation<?> typeInformation) {
			return obj instanceof Placeholder p ? p.getValue() : obj;
		}

		@Override
		public DBRef toDBRef(Object object, @Nullable MongoPersistentProperty referringProperty) {
			return null;
		}

		@Override
		public void write(Object source, Bson sink) {

		}
	}

	@NullUnmarked
	static class PlaceholderParameterAccessor implements MongoParameterAccessor {

		private final List<Placeholder> placeholders;

		public PlaceholderParameterAccessor(QueryMethod queryMethod) {
			if (queryMethod.getParameters().getNumberOfParameters() == 0) {
				placeholders = List.of();
			} else {
				placeholders = new ArrayList<>();
				Parameters<?, ?> parameters = queryMethod.getParameters();
				for (Parameter parameter : parameters.toList()) {
					if (ClassUtils.isAssignable(GeoJson.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), new GeoJsonPlaceholder(parameter.getIndex(), ""));
					}
					else if (ClassUtils.isAssignable(Point.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), new PointPlaceholder(parameter.getIndex()));
					} else if (ClassUtils.isAssignable(Circle.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), new CirclePlaceholder(parameter.getIndex()));
					} else if (ClassUtils.isAssignable(Box.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), new BoxPlaceholder(parameter.getIndex()));
					} else if (ClassUtils.isAssignable(Sphere.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), new SpherePlaceholder(parameter.getIndex()));
					} else if (ClassUtils.isAssignable(Polygon.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), new PolygonPlaceholder(parameter.getIndex()));
					}
					else {
						placeholders.add(parameter.getIndex(), Placeholder.indexed(parameter.getIndex()));
					}
				}
			}
		}

		@Override
		public Range<Distance> getDistanceRange() {
			return Range.unbounded();
		}

		@Override
		public @Nullable Vector getVector() {
			return null;
		}

		@Override
		public @Nullable Score getScore() {
			return null;
		}

		@Override
		public @Nullable Range<Score> getScoreRange() {
			return null;
		}

		@Override
		public @Nullable Point getGeoNearLocation() {
			return null;
		}

		@Override
		public @Nullable TextCriteria getFullText() {
			return null;
		}

		@Override
		public @Nullable Collation getCollation() {
			return null;
		}

		@Override
		public Object[] getValues() {
			return placeholders.toArray();
		}

		@Override
		public @Nullable UpdateDefinition getUpdate() {
			return null;
		}

		@Override
		public @Nullable ScrollPosition getScrollPosition() {
			return null;
		}

		@Override
		public Pageable getPageable() {
			return null;
		}

		@Override
		public Sort getSort() {
			return null;
		}

		@Override
		public @Nullable Class<?> findDynamicProjection() {
			return null;
		}

		@Override
		public @Nullable Object getBindableValue(int index) {
			return placeholders.get(index).getValue();
		}

		@Override
		public boolean hasBindableNullValue() {
			return false;
		}

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public Iterator<Object> iterator() {
			return ((List) placeholders).iterator();
		}
	}

	static class CirclePlaceholder extends Circle implements Placeholder {

		int index;

		public CirclePlaceholder(int index) {
			super(new PointPlaceholder(index), Distance.of(1, Metrics.NEUTRAL)); //
			this.index = index;
		}

		@Override
		public Object getValue() {
			return "?%s".formatted(index);
		}

		@Override
		public String toString() {
			return getValue().toString();
		}
	}

	static class SpherePlaceholder extends Sphere implements Placeholder {

		int index;

		public SpherePlaceholder(int index) {
			super(new PointPlaceholder(index), Distance.of(1, Metrics.NEUTRAL)); //
			this.index = index;
		}

		@Override
		public Object getValue() {
			return "?%s".formatted(index);
		}

		@Override
		public String toString() {
			return getValue().toString();
		}
	}

	static class GeoJsonPlaceholder implements Placeholder, GeoJson<List<Placeholder>>, Shape {

		int index;
		String type;

		public GeoJsonPlaceholder(int index, String type) {
			this.index = index;
			this.type = type;
		}

		@Override
		public Object getValue() {
			return "?%s".formatted(index);
		}

		@Override
		public String toString() {
			return getValue().toString();
		}

		@Override
		public String getType() {
			return type;
		}

		@Override
		public List<Placeholder> getCoordinates() {
			return List.of();
		}
	}

	static class BoxPlaceholder extends Box implements Placeholder {
		int index;

		public BoxPlaceholder(int index) {
			super(new PointPlaceholder(index), new PointPlaceholder(index));
			this.index = index;
		}

		@Override
		public Object getValue() {
			return "?%s".formatted(index);
		}

		@Override
		public String toString() {
			return getValue().toString();
		}
	}

	static class PolygonPlaceholder extends Polygon implements Placeholder {
		int index;

		public PolygonPlaceholder(int index) {
			super(new PointPlaceholder(index), new PointPlaceholder(index), new PointPlaceholder(index),
					new PointPlaceholder(index));
			this.index = index;
		}

		@Override
		public Object getValue() {
			return "?%s".formatted(index);
		}

		@Override
		public String toString() {
			return getValue().toString();
		}
	}

	static class PointPlaceholder extends Point implements Placeholder {

		int index;

		public PointPlaceholder(int index) {
			super(Double.NaN, Double.NaN);
			this.index = index;
		}

		@Override
		public Object getValue() {
			return "?" + index;
		}

		@Override
		public String toString() {
			return getValue().toString();
		}
	}
}
