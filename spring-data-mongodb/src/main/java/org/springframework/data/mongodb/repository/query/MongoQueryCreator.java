/*
 * Copyright 2010-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Shape;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.MongoRegexCreator;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor.PotentiallyConvertingIterator;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Custom query creator to create Mongo criterias.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
class MongoQueryCreator extends AbstractQueryCreator<Query, Criteria> {

	private static final Logger LOG = LoggerFactory.getLogger(MongoQueryCreator.class);
	private static final Pattern PUNCTATION_PATTERN = Pattern.compile("\\p{Punct}");
	private final MongoParameterAccessor accessor;
	private final boolean isGeoNearQuery;

	private final MappingContext<?, MongoPersistentProperty> context;

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

		Assert.notNull(context, "MappingContext must not be null!");

		this.accessor = accessor;
		this.isGeoNearQuery = isGeoNearQuery;
		this.context = context;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#create(org.springframework.data.repository.query.parser.Part, java.util.Iterator)
	 */
	@Override
	protected Criteria create(Part part, Iterator<Object> iterator) {

		if (isGeoNearQuery && part.getType().equals(Type.NEAR)) {
			return null;
		}

		PersistentPropertyPath<MongoPersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		MongoPersistentProperty property = path.getLeafProperty();
		Criteria criteria = from(part, property, where(path.toDotPath()), (PotentiallyConvertingIterator) iterator);

		return criteria;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#and(org.springframework.data.repository.query.parser.Part, java.lang.Object, java.util.Iterator)
	 */
	@Override
	protected Criteria and(Part part, Criteria base, Iterator<Object> iterator) {

		if (base == null) {
			return create(part, iterator);
		}

		PersistentPropertyPath<MongoPersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		MongoPersistentProperty property = path.getLeafProperty();

		return from(part, property, base.and(path.toDotPath()), (PotentiallyConvertingIterator) iterator);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#or(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected Criteria or(Criteria base, Criteria criteria) {

		Criteria result = new Criteria();
		return result.orOperator(base, criteria);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#complete(java.lang.Object, org.springframework.data.domain.Sort)
	 */
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
				return criteria.gt(parameters.next()).lt(parameters.next());
			case IS_NOT_NULL:
				return criteria.ne(null);
			case IS_NULL:
				return criteria.is(null);
			case NOT_IN:
				return criteria.nin(nextAsArray(parameters));
			case IN:
				return criteria.in(nextAsArray(parameters));
			case LIKE:
			case STARTING_WITH:
			case ENDING_WITH:
			case CONTAINING:
				return createContainingCriteria(part, property, criteria, parameters);
			case NOT_CONTAINING:
				return createContainingCriteria(part, property, criteria.not(), parameters);
			case REGEX:
				return criteria.regex(parameters.next().toString());
			case EXISTS:
				return criteria.exists((Boolean) parameters.next());
			case TRUE:
				return criteria.is(true);
			case FALSE:
				return criteria.is(false);
			case NEAR:

				Range<Distance> range = accessor.getDistanceRange();
				Distance distance = range.getUpperBound();
				Distance minDistance = range.getLowerBound();

				Point point = accessor.getGeoNearLocation();
				point = point == null ? nextAs(parameters, Point.class) : point;

				boolean isSpherical = isSpherical(property);

				if (distance == null) {
					return isSpherical ? criteria.nearSphere(point) : criteria.near(point);
				} else {
					if (isSpherical || !Metrics.NEUTRAL.equals(distance.getMetric())) {
						criteria.nearSphere(point);
					} else {
						criteria.near(point);
					}
					criteria.maxDistance(distance.getNormalizedValue());
					if (minDistance != null) {
						criteria.minDistance(minDistance.getNormalizedValue());
					}
				}
				return criteria;
			case WITHIN:

				Object parameter = parameters.next();
				return criteria.within((Shape) parameter);
			case SIMPLE_PROPERTY:

				return isSimpleComparisionPossible(part) ? criteria.is(parameters.next())
						: createLikeRegexCriteriaOrThrow(part, property, criteria, parameters, false);

			case NEGATING_SIMPLE_PROPERTY:

				return isSimpleComparisionPossible(part) ? criteria.ne(parameters.next())
						: createLikeRegexCriteriaOrThrow(part, property, criteria, parameters, true);
			default:
				throw new IllegalArgumentException("Unsupported keyword!");
		}
	}

	private boolean isSimpleComparisionPossible(Part part) {

		switch (part.shouldIgnoreCase()) {
			case NEVER:
				return true;
			case WHEN_POSSIBLE:
				return part.getProperty().getType() != String.class;
			case ALWAYS:
				return false;
			default:
				return true;
		}
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

				return addAppropriateLikeRegexTo(criteria, part, parameters.next().toString());

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
			return criteria.in(nextAsArray(parameters));
		}

		return addAppropriateLikeRegexTo(criteria, part, parameters.next().toString());
	}

	/**
	 * Creates an appropriate like-regex and appends it to the given criteria.
	 * 
	 * @param criteria
	 * @param part
	 * @param value
	 * @return the criteria extended with the regex.
	 */
	private Criteria addAppropriateLikeRegexTo(Criteria criteria, Part part, String value) {

		return criteria.regex(toLikeRegex(value, part), toRegexOptions(part));
	}

	/**
	 * @param part
	 * @return the regex options or {@literal null}.
	 */
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
				String.format("Expected parameter type of %s but got %s!", type, parameter.getClass()));
	}

	private Object[] nextAsArray(Iterator<Object> iterator) {

		Object next = iterator.next();

		if (next instanceof Collection) {
			return ((Collection<?>) next).toArray();
		} else if (next != null && next.getClass().isArray()) {
			return (Object[]) next;
		}

		return new Object[] { next };
	}

	private String toLikeRegex(String source, Part part) {
		return MongoRegexCreator.INSTANCE.toRegularExpression(source, part.getType());
	}

	private boolean isSpherical(MongoPersistentProperty property) {

		GeoSpatialIndexed index = property.findAnnotation(GeoSpatialIndexed.class);
		return index != null && index.type().equals(GeoSpatialIndexType.GEO_2DSPHERE);
	}
}
