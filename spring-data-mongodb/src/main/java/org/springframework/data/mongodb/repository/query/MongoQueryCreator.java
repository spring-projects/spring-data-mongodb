/*
 * Copyright 2002-2010 the original author or authors.
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

import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.mongodb.core.geo.Shape;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor.PotentiallyConvertingIterator;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

/**
 * Custom query creator to create Mongo criterias.
 * 
 * @author Oliver Gierke
 */
class MongoQueryCreator extends AbstractQueryCreator<Query, Criteria> {

	private static final Logger LOG = LoggerFactory.getLogger(MongoQueryCreator.class);
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

		Assert.notNull(context);

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
		Criteria criteria = from(part.getType(), property,
				where(path.toDotPath(MongoPersistentProperty.PropertyToFieldNameConverter.INSTANCE)),
				(PotentiallyConvertingIterator) iterator);

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

		Criteria criteria = from(part.getType(), property,
				where(path.toDotPath(MongoPersistentProperty.PropertyToFieldNameConverter.INSTANCE)),
				(PotentiallyConvertingIterator) iterator);

		return criteria.andOperator(criteria);
	}

	/*
	* (non-Javadoc)
	*
	* @see
	* org.springframework.data.repository.query.parser.AbstractQueryCreator
	* #or(java.lang.Object, java.lang.Object)
	*/
	@Override
	protected Criteria or(Criteria base, Criteria criteria) {

		Criteria result = new Criteria();
		return result.orOperator(base, criteria);
	}

	/*
	* (non-Javadoc)
	*
	* @see
	* org.springframework.data.repository.query.parser.AbstractQueryCreator
	* #complete(java.lang.Object, org.springframework.data.domain.Sort)
	*/
	@Override
	protected Query complete(Criteria criteria, Sort sort) {

		if (criteria == null) {
			return null;
		}

		Query query = new Query(criteria);
		QueryUtils.applySorting(query, sort);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created query " + query.getQueryObject());
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
	private Criteria from(Type type, MongoPersistentProperty property, Criteria criteria,
			PotentiallyConvertingIterator parameters) {

		switch (type) {
		case AFTER:
		case GREATER_THAN:
			return criteria.gt(parameters.nextConverted(property));
		case GREATER_THAN_EQUAL:
			return criteria.gte(parameters.nextConverted(property));
		case BEFORE:
		case LESS_THAN:
			return criteria.lt(parameters.nextConverted(property));
		case LESS_THAN_EQUAL:
			return criteria.lte(parameters.nextConverted(property));
		case BETWEEN:
			return criteria.gt(parameters.nextConverted(property)).lt(parameters.nextConverted(property));
		case IS_NOT_NULL:
			return criteria.ne(null);
		case IS_NULL:
			return criteria.is(null);
		case NOT_IN:
			return criteria.nin(nextAsArray(parameters, property));
		case IN:
			return criteria.in(nextAsArray(parameters, property));
		case LIKE:
		case STARTING_WITH:
		case ENDING_WITH:
		case CONTAINING:
			String value = parameters.next().toString();
			return criteria.regex(toLikeRegex(value, type));
		case REGEX:
			return criteria.regex(parameters.next().toString());
		case EXISTS:
			return criteria.exists((Boolean) parameters.next());
		case TRUE:
			return criteria.is(true);
		case FALSE:
			return criteria.is(false);
		case NEAR:

			Distance distance = accessor.getMaxDistance();
			Point point = accessor.getGeoNearLocation();
			point = point == null ? nextAs(parameters, Point.class) : point;

			if (distance == null) {
				return criteria.near(point);
			} else {
				if (distance.getMetric() != null) {
					criteria.nearSphere(point);
				} else {
					criteria.near(point);
				}
				criteria.maxDistance(distance.getNormalizedValue());
			}
			return criteria;

		case WITHIN:
			Object parameter = parameters.next();
			return criteria.within((Shape) parameter);
		case SIMPLE_PROPERTY:
			return criteria.is(parameters.nextConverted(property));
		case NEGATING_SIMPLE_PROPERTY:
			return criteria.not().is(parameters.nextConverted(property));
		}

		throw new IllegalArgumentException("Unsupported keyword!");
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
		if (parameter.getClass().isAssignableFrom(type)) {
			return (T) parameter;
		}

		throw new IllegalArgumentException(String.format("Expected parameter type of %s but got %s!", type,
				parameter.getClass()));
	}

	private Object[] nextAsArray(PotentiallyConvertingIterator iterator, MongoPersistentProperty property) {
		Object next = iterator.nextConverted(property);

		if (next instanceof Collection) {
			return ((Collection<?>) next).toArray();
		} else if (next.getClass().isArray()) {
			return (Object[]) next;
		}

		return new Object[] { next };
	}

	private String toLikeRegex(String source, Type type) {

		switch (type) {
		case STARTING_WITH:
			source = source + "*";
			break;
		case ENDING_WITH:
			source = "*" + source;
			break;
		case CONTAINING:
			source = "*" + source + "*";
			break;
		}

		return source.replaceAll("\\*", ".*");
	}
}