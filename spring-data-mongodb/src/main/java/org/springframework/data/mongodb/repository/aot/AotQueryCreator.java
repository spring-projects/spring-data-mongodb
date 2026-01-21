/*
 * Copyright 2025-present the original author or authors.
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.conversions.Bson;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.data.core.TypeInformation;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.geo.GeoJson;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.mongodb.repository.aot.AotPlaceholders.AsListPlaceholder;
import org.springframework.data.mongodb.repository.aot.AotPlaceholders.Placeholder;
import org.springframework.data.mongodb.repository.aot.AotPlaceholders.RegexPlaceholder;
import org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor;
import org.springframework.data.mongodb.repository.query.MongoParameterAccessor;
import org.springframework.data.mongodb.repository.query.MongoQueryCreator;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;

import com.mongodb.DBRef;

/**
 * @author Christoph Strobl
 * @since 5.0
 */
record AotQueryCreator(MappingContext<?, MongoPersistentProperty> mappingContext) {

	@SuppressWarnings("NullAway")
	AotStringQuery createQuery(PartTree partTree, QueryMethod queryMethod, Method source) {

		boolean geoNear = queryMethod instanceof MongoQueryMethod mqm && mqm.isGeoNearQuery();
		boolean searchQuery = queryMethod instanceof MongoQueryMethod mqm
				? mqm.isSearchQuery() || source.isAnnotationPresent(VectorSearch.class)
				: source.isAnnotationPresent(VectorSearch.class);

		PlaceholderParameterAccessor placeholderAccessor = new PlaceholderParameterAccessor(partTree, queryMethod);
		Query query = new AotMongoQueryCreator(partTree, new PlaceholderConvertingParameterAccessor(placeholderAccessor),
				mappingContext, geoNear, searchQuery).createQuery();

		if (partTree.isLimiting()) {
			query.limit(partTree.getMaxResults());
		}
		return new AotStringQuery(query, placeholderAccessor.getPlaceholders());
	}

	static class AotMongoQueryCreator extends MongoQueryCreator {

		public AotMongoQueryCreator(PartTree tree, MongoParameterAccessor accessor,
				MappingContext<?, MongoPersistentProperty> context, boolean isGeoNearQuery, boolean isSearchQuery) {
			super(tree, accessor, context, isGeoNearQuery, isSearchQuery);
		}

		@Override
		protected Criteria in(Criteria criteria, Part part, Object param) {
			return param instanceof Placeholder p ? criteria.raw("$in", p) : super.in(criteria, part, param);
		}

		@Override
		protected Criteria nin(Criteria criteria, Part part, Object param) {
			return param instanceof Placeholder p ? criteria.raw("$nin", p) : super.nin(criteria, part, param);
		}

		@Override
		protected Criteria regex(Criteria criteria, Object param) {
			return param instanceof Placeholder p ? criteria.raw("$regex", p) : super.regex(criteria, param);
		}

		@Override
		protected Criteria exists(Criteria criteria, Object param) {
			return param instanceof Placeholder p ? criteria.raw("$exists", p) : super.exists(criteria, param);
		}

		@Override
		protected Criteria createContainingCriteria(Part part, MongoPersistentProperty property, Criteria criteria,
				Object param) {

			if (part.getType().equals(Type.LIKE)) {
				return criteria.is(param);
			}

			if (part.getType().equals(Type.NOT_LIKE)) {
				return criteria.raw("$not", param);
			}

			if (param instanceof RegexPlaceholder) {
				return criteria.raw("$regex", param);
			}

			if (param instanceof AsListPlaceholder asList && !property.isCollectionLike()) {
				return super.createContainingCriteria(part, property, criteria, asList.placeholder());
			}

			return super.createContainingCriteria(part, property, criteria, param);
		}
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

		@Nullable
		Part getPartForIndex(PartTree partTree, Parameter parameter) {

			if (!parameter.isBindable()) {
				return null;
			}

			List<Part> parts = partTree.getParts().stream().toList();
			int counter = 0;
			for (Part part : parts) {
				if (counter == parameter.getIndex()) {
					return part;
				}
				counter += part.getNumberOfArguments();
			}
			return null;
		}

		public PlaceholderParameterAccessor(PartTree partTree, QueryMethod queryMethod) {

			Parameters<?, ?> parameters = queryMethod.getParameters();
			if (parameters.getNumberOfParameters() == 0) {
				placeholders = List.of();
			} else {

				placeholders = new ArrayList<>(parameters.getNumberOfParameters());
				for (Parameter parameter : parameters.toList()) {

					int index = parameter.getIndex();
					placeholders.add(index, getPlaceholder(index, parameter, partTree));
				}
			}
		}

		private Placeholder getPlaceholder(int index, Parameter parameter, PartTree partTree) {

			Class<?> type = parameter.getType();

			if (GeoJson.class.isAssignableFrom(type)) {
				return AotPlaceholders.geoJson(index, "");
			} else if (Point.class.isAssignableFrom(type)) {
				return AotPlaceholders.point(index);
			} else if (Circle.class.isAssignableFrom(type)) {
				return AotPlaceholders.circle(index);
			} else if (Box.class.isAssignableFrom(type)) {
				return AotPlaceholders.box(index);
			} else if (Sphere.class.isAssignableFrom(type)) {
				return AotPlaceholders.sphere(index);
			} else if (Polygon.class.isAssignableFrom(type)) {
				return AotPlaceholders.polygon(index);
			} else if (Pattern.class.isAssignableFrom(type)) {
				return AotPlaceholders.regex(index, null);
			}

			Part partForIndex = getPartForIndex(partTree, parameter);
			if (partForIndex != null) {

				IgnoreCaseType ignoreCaseType = partForIndex.shouldIgnoreCase();
				if (isLike(partForIndex.getType())) {

					boolean ignoreCase = !ignoreCaseType.equals(IgnoreCaseType.NEVER);
					return AotPlaceholders.regex(index, ignoreCase ? "i" : null);
				}

				if (isContaining(partForIndex.getType())) {

					if (partForIndex.getProperty().isCollection() && !TypeInformation.of(type).isCollectionLike()) {
						if (ignoreCaseType.equals(IgnoreCaseType.ALWAYS)) {
							return AotPlaceholders.asList(AotPlaceholders.regex(index, "i"));
						} else {
							return AotPlaceholders.asList(index);
						}
					}

					return AotPlaceholders.indexed(index);
				}
			}

			return AotPlaceholders.indexed(index);
		}

		private static boolean isContaining(Part.Type type) {
			return type.equals(Type.IN) || type.equals(Type.NOT_IN) //
					|| type.equals(Type.CONTAINING) || type.equals(Type.NOT_CONTAINING);
		}

		private static boolean isLike(Part.Type type) {
			return type.equals(Type.LIKE) || type.equals(Type.NOT_LIKE);
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

		public List<Placeholder> getPlaceholders() {
			return placeholders;
		}
	}

}
