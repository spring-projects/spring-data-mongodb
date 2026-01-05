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
import org.springframework.util.ClassUtils;

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

		private final List<Object> placeholders;

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

			if (queryMethod.getParameters().getNumberOfParameters() == 0) {
				placeholders = List.of();
			} else {

				placeholders = new ArrayList<>();
				Parameters<?, ?> parameters = queryMethod.getParameters();

				for (Parameter parameter : parameters.toList()) {
					if (ClassUtils.isAssignable(GeoJson.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), AotPlaceholders.geoJson(parameter.getIndex(), ""));
					} else if (ClassUtils.isAssignable(Point.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), AotPlaceholders.point(parameter.getIndex()));
					} else if (ClassUtils.isAssignable(Circle.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), AotPlaceholders.circle(parameter.getIndex()));
					} else if (ClassUtils.isAssignable(Box.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), AotPlaceholders.box(parameter.getIndex()));
					} else if (ClassUtils.isAssignable(Sphere.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), AotPlaceholders.sphere(parameter.getIndex()));
					} else if (ClassUtils.isAssignable(Polygon.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), AotPlaceholders.polygon(parameter.getIndex()));
					} else if (ClassUtils.isAssignable(Pattern.class, parameter.getType())) {
						placeholders.add(parameter.getIndex(), AotPlaceholders.regex(parameter.getIndex(), null));
					} else {
						Part partForIndex = getPartForIndex(partTree, parameter);
						if (partForIndex != null
								&& (partForIndex.getType().equals(Type.LIKE) || partForIndex.getType().equals(Type.NOT_LIKE))) {
							placeholders
									.add(parameter.getIndex(),
											AotPlaceholders
													.regex(parameter.getIndex(),
															partForIndex.shouldIgnoreCase().equals(IgnoreCaseType.ALWAYS)
																	|| partForIndex.shouldIgnoreCase().equals(IgnoreCaseType.WHEN_POSSIBLE) ? "i"
																			: null));
						} else {
							placeholders.add(parameter.getIndex(), AotPlaceholders.indexed(parameter.getIndex()));
						}
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
			return placeholders.get(index) instanceof Placeholder placeholder ? placeholder.getValue()
					: placeholders.get(index);
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

		public List<Object> getPlaceholders() {
			return placeholders;
		}
	}

}
