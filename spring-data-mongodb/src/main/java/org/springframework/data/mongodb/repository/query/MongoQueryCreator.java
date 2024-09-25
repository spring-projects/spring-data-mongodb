/*
 * Copyright 2010-2024 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BsonRegularExpression;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Shape;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.geo.GeoJson;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.MetricConversion;
import org.springframework.data.mongodb.core.query.MongoRegexCreator;
import org.springframework.data.mongodb.core.query.MongoRegexCreator.MatchMode;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.Streamable;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * Custom query creator to create Mongo criterias.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Edward Prentice
 */
class MongoQueryCreator extends AbstractQueryCreator<Query, Criteria> {

	private static final Log LOG = LogFactory.getLog(MongoQueryCreator.class);

	private final MongoParameterAccessor accessor;
	private final MappingContext<?, MongoPersistentProperty> context;
	private final boolean isGeoNearQuery;

	/**
	 * Creates a new {@link MongoQueryCreator} from the given {@link PartTree}, {@link ConvertingParameterAccessor} and
	 * {@link MappingContext}.
	 *
	 * @param tree
	 * @param accessor
	 * @param context
	 */
	public MongoQueryCreator(PartTree tree, ConvertingParameterAccessor accessor,
			MappingContext<?, MongoPersistentProperty> context) {
		this(tree, accessor, context, false);
	}

	/**
	 * Creates a new {@link MongoQueryCreator} from the given {@link PartTree}, {@link ConvertingParameterAccessor} and
	 * {@link MappingContext}.
	 *
	 * @param tree
	 * @param accessor
	 * @param context
	 * @param isGeoNearQuery
	 */
	public MongoQueryCreator(PartTree tree, ConvertingParameterAccessor accessor,
			MappingContext<?, MongoPersistentProperty> context, boolean isGeoNearQuery) {

		super(tree, accessor);

		Assert.notNull(context, "MappingContext must not be null");

		this.accessor = accessor;
		this.isGeoNearQuery = isGeoNearQuery;
		this.context = context;
	}

	@Override
	protected Criteria create(Part part, Iterator<Object> iterator) {

		if (isGeoNearQuery && part.getType().equals(Type.NEAR)) {
			return null;
		}

		PersistentPropertyPath<MongoPersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		MongoPersistentProperty property = path.getLeafProperty();

		return from(part, property, where(path.toDotPath()), iterator);
	}

	@Override
	protected Criteria and(Part part, Criteria base, Iterator<Object> iterator) {

		if (base == null) {
			return create(part, iterator);
		}

		PersistentPropertyPath<MongoPersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		MongoPersistentProperty property = path.getLeafProperty();

		return from(part, property, base.and(path.toDotPath()), iterator);
	}

	@Override
	protected Criteria or(Criteria base, Criteria criteria) {

		Criteria result = new Criteria();
		return result.orOperator(base, criteria);
	}

	@Override
	protected Query complete(Criteria criteria, Sort sort) {

		Query query = (criteria == null ? new Query() : new Query(criteria)).with(sort);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created query " + query);
		}

		return query;
	}

	/**
	 * Populates the given {@link CriteriaDefinition} depending on the {@link Part} given.
	 *
	 * @param part
	 * @param property
	 * @param criteria
	 * @param parameters
	 * @return
	 */
	private Criteria from(Part part, MongoPersistentProperty property, Criteria criteria, Iterator<Object> parameters) {

		Type type = part.getType();

		switch (type) {
			case AFTER:
			case GREATER_THAN:
				return criteria.gt(parameters.next());
			case GREATER_THAN_EQUAL:
				return criteria.gte(parameters.next());
			case BEFORE:
			case LESS_THAN:
				return criteria.lt(parameters.next());
			case LESS_THAN_EQUAL:
				return criteria.lte(parameters.next());
			case BETWEEN:
				return computeBetweenPart(criteria, parameters);
			case IS_NOT_NULL:
				return criteria.ne(null);
			case IS_NULL:
				return criteria.is(null);
			case NOT_IN:
				return criteria.nin(nextAsList(parameters, part));
			case IN:
				return criteria.in(nextAsList(parameters, part));
			case LIKE:
			case STARTING_WITH:
			case ENDING_WITH:
			case CONTAINING:
				return createContainingCriteria(part, property, criteria, parameters);
			case NOT_LIKE:
				return createContainingCriteria(part, property, criteria.not(), parameters);
			case NOT_CONTAINING:
				return createContainingCriteria(part, property, criteria.not(), parameters);
			case REGEX:

				Object param = parameters.next();
				return param instanceof Pattern pattern ? criteria.regex(pattern) : criteria.regex(param.toString());
			case EXISTS:
				return criteria.exists((Boolean) parameters.next());
			case TRUE:
				return criteria.is(true);
			case FALSE:
				return criteria.is(false);
			case NEAR:

				Range<Distance> range = accessor.getDistanceRange();
				Optional<Distance> distance = range.getUpperBound().getValue();
				Optional<Distance> minDistance = range.getLowerBound().getValue();

				Point point = accessor.getGeoNearLocation();
				Point pointToUse = point == null ? nextAs(parameters, Point.class) : point;

				boolean isSpherical = isSpherical(property);

				return distance.map(it -> {

					if (isSpherical || !Metrics.NEUTRAL.equals(it.getMetric())) {
						criteria.nearSphere(pointToUse);
					} else {
						criteria.near(pointToUse);
					}

					if (pointToUse instanceof GeoJson) { // using GeoJson distance is in meters.

						criteria.maxDistance(MetricConversion.getDistanceInMeters(it));
						minDistance.map(MetricConversion::getDistanceInMeters).ifPresent(criteria::minDistance);
					} else {
						criteria.maxDistance(it.getNormalizedValue());
						minDistance.map(Distance::getNormalizedValue).ifPresent(criteria::minDistance);
					}

					return criteria;

				}).orElseGet(() -> isSpherical ? criteria.nearSphere(pointToUse) : criteria.near(pointToUse));

			case WITHIN:

				Object parameter = parameters.next();
				return criteria.within((Shape) parameter);
			case SIMPLE_PROPERTY:

				return isSimpleComparisonPossible(part) ? criteria.is(parameters.next())
						: createLikeRegexCriteriaOrThrow(part, property, criteria, parameters, false);

			case NEGATING_SIMPLE_PROPERTY:

				return isSimpleComparisonPossible(part) ? criteria.ne(parameters.next())
						: createLikeRegexCriteriaOrThrow(part, property, criteria, parameters, true);
			default:
				throw new IllegalArgumentException("Unsupported keyword");
		}
	}

	private boolean isSimpleComparisonPossible(Part part) {

		return switch (part.shouldIgnoreCase()) {
			case NEVER -> true;
			case WHEN_POSSIBLE -> part.getProperty().getType() != String.class;
			case ALWAYS -> false;
		};
	}

	/**
	 * Creates and extends the given criteria with a like-regex if necessary.
	 *
	 * @param part
	 * @param property
	 * @param criteria
	 * @param parameters
	 * @param shouldNegateExpression
	 * @return the criteria extended with the like-regex.
	 */
	private Criteria createLikeRegexCriteriaOrThrow(Part part, MongoPersistentProperty property, Criteria criteria,
			Iterator<Object> parameters, boolean shouldNegateExpression) {

		PropertyPath path = part.getProperty().getLeafProperty();

		switch (part.shouldIgnoreCase()) {

			case ALWAYS:
				if (path.getType() != String.class) {
					throw new IllegalArgumentException(
							String.format("Part %s must be of type String but was %s", path, path.getType()));
				}
				// fall-through

			case WHEN_POSSIBLE:

				if (shouldNegateExpression) {
					criteria = criteria.not();
				}

				return addAppropriateLikeRegexTo(criteria, part, parameters.next());

			case NEVER:
				// intentional no-op
		}

		throw new IllegalArgumentException(String.format("part.shouldCaseIgnore must be one of %s, but was %s",
				Arrays.asList(IgnoreCaseType.ALWAYS, IgnoreCaseType.WHEN_POSSIBLE), part.shouldIgnoreCase()));
	}

	/**
	 * If the target property of the comparison is of type String, then the operator checks for match using regular
	 * expression. If the target property of the comparison is a {@link Collection} then the operator evaluates to true if
	 * it finds an exact match within any member of the {@link Collection}.
	 *
	 * @param part
	 * @param property
	 * @param criteria
	 * @param parameters
	 * @return
	 */
	private Criteria createContainingCriteria(Part part, MongoPersistentProperty property, Criteria criteria,
			Iterator<Object> parameters) {

		if (property.isCollectionLike()) {
			return criteria.in(nextAsList(parameters, part));
		}

		return addAppropriateLikeRegexTo(criteria, part, parameters.next());
	}

	/**
	 * Creates an appropriate like-regex and appends it to the given criteria.
	 *
	 * @param criteria
	 * @param part
	 * @param value
	 * @return the criteria extended with the regex.
	 */
	private Criteria addAppropriateLikeRegexTo(Criteria criteria, Part part, Object value) {

		if (value == null) {

			throw new IllegalArgumentException(String.format(
					"Argument for creating $regex pattern for property '%s' must not be null", part.getProperty().getSegment()));
		}

		return criteria.regex(toLikeRegex(value.toString(), part), toRegexOptions(part));
	}

	/**
	 * @param part
	 * @return the regex options or {@literal null}.
	 */
	@Nullable
	private String toRegexOptions(Part part) {

		String regexOptions = null;
		switch (part.shouldIgnoreCase()) {
			case WHEN_POSSIBLE:
			case ALWAYS:
				regexOptions = "i";
			case NEVER:
		}
		return regexOptions;
	}

	/**
	 * Returns the next element from the given {@link Iterator} expecting it to be of a certain type.
	 *
	 * @param <T>
	 * @param iterator
	 * @param type
	 * @throws IllegalArgumentException in case the next element in the iterator is not of the given type.
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private <T> T nextAs(Iterator<Object> iterator, Class<T> type) {

		Object parameter = iterator.next();

		if (ClassUtils.isAssignable(type, parameter.getClass())) {
			return (T) parameter;
		}

		throw new IllegalArgumentException(
				String.format("Expected parameter type of %s but got %s", type, parameter.getClass()));
	}

	private java.util.List<?> nextAsList(Iterator<Object> iterator, Part part) {

		Streamable<?> streamable = asStreamable(iterator.next());
		if (!isSimpleComparisonPossible(part)) {

			MatchMode matchMode = toMatchMode(part.getType());
			String regexOptions = toRegexOptions(part);

			streamable = streamable.map(it -> {
				if (it instanceof String value) {

					return new BsonRegularExpression(MongoRegexCreator.INSTANCE.toRegularExpression(value, matchMode),
							regexOptions);
				}
				return it;
			});
		}

		return streamable.toList();
	}

	private Streamable<?> asStreamable(Object value) {

		if (value instanceof Collection<?> collection) {
			return Streamable.of(collection);
		} else if (ObjectUtils.isArray(value)) {
			return Streamable.of((Object[]) value);
		}
		return Streamable.of(value);
	}

	private String toLikeRegex(String source, Part part) {
		return MongoRegexCreator.INSTANCE.toRegularExpression(source, toMatchMode(part.getType()));
	}

	private boolean isSpherical(MongoPersistentProperty property) {

		if (property.isAnnotationPresent(GeoSpatialIndexed.class)) {
			GeoSpatialIndexed index = property.findAnnotation(GeoSpatialIndexed.class);
			return index.type().equals(GeoSpatialIndexType.GEO_2DSPHERE);
		}

		return false;
	}

	/**
	 * Compute a {@link Type#BETWEEN} typed {@link Part} using {@link Criteria#gt(Object) $gt},
	 * {@link Criteria#gte(Object) $gte}, {@link Criteria#lt(Object) $lt} and {@link Criteria#lte(Object) $lte}.
	 * <br />
	 * In case the first {@literal value} is actually a {@link Range} the lower and upper bounds of the {@link Range} are
	 * used according to their {@link Bound#isInclusive() inclusion} definition. Otherwise the {@literal value} is used
	 * for {@literal $gt} and {@link Iterator#next() parameters.next()} as {@literal $lt}.
	 *
	 * @param criteria must not be {@literal null}.
	 * @param parameters must not be {@literal null}.
	 * @return
	 * @since 2.2
	 */
	private static Criteria computeBetweenPart(Criteria criteria, Iterator<Object> parameters) {

		Object value = parameters.next();
		if (!(value instanceof Range<?> range)) {
			return criteria.gt(value).lt(parameters.next());
		}

		Optional<?> min = range.getLowerBound().getValue();
		Optional<?> max = range.getUpperBound().getValue();

		min.ifPresent(it -> {

			if (range.getLowerBound().isInclusive()) {
				criteria.gte(it);
			} else {
				criteria.gt(it);
			}
		});

		max.ifPresent(it -> {

			if (range.getUpperBound().isInclusive()) {
				criteria.lte(it);
			} else {
				criteria.lt(it);
			}
		});

		return criteria;
	}

	private static MatchMode toMatchMode(Type type) {

		return switch (type) {
			case NOT_CONTAINING, CONTAINING -> MatchMode.CONTAINING;
			case STARTING_WITH -> MatchMode.STARTING_WITH;
			case ENDING_WITH -> MatchMode.ENDING_WITH;
			case LIKE, NOT_LIKE -> MatchMode.LIKE;
			case REGEX -> MatchMode.REGEX;
			case NEGATING_SIMPLE_PROPERTY, SIMPLE_PROPERTY, IN -> MatchMode.EXACT;
			default -> MatchMode.DEFAULT;
		};
	}
}
