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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.geo.Box;
import org.springframework.data.mongodb.core.geo.Circle;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor.PotentiallyConvertingIterator;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Custom query creator to create Mongo criterias.
 * 
 * @author Oliver Gierke
 */
class MongoQueryCreator extends AbstractQueryCreator<Query, Query> {

	private static final Log LOG = LogFactory.getLog(MongoQueryCreator.class);
	private final MongoParameterAccessor accessor;
	private final boolean isGeoNearQuery;

	public MongoQueryCreator(PartTree tree, ConvertingParameterAccessor accessor) {
		this(tree, accessor, false);
	}
	
	/**
	 * Creates a new {@link MongoQueryCreator} from the given {@link PartTree} and {@link ParametersParameterAccessor}.
	 * 
	 * @param tree
	 * @param accessor
	 * @param isGeoNearQuery
	 */
	public MongoQueryCreator(PartTree tree, ConvertingParameterAccessor accessor, boolean isGeoNearQuery) {

		super(tree, accessor);
		this.accessor = accessor;
		this.isGeoNearQuery = isGeoNearQuery;
	}
	

	/*
	* (non-Javadoc)
	* @see org.springframework.data.repository.query.parser.AbstractQueryCreator#create(org.springframework.data.repository.query.parser.Part, java.util.Iterator)
	*/
	@Override
	protected Query create(Part part, Iterator<Object> iterator) {
		
		if (isGeoNearQuery && part.getType().equals(Type.NEAR)) {
			return null;
		}

		Criteria criteria = from(part.getType(), where(part.getProperty().toDotPath()),
				(PotentiallyConvertingIterator) iterator);

		return new Query(criteria);
	}

	/*
	* (non-Javadoc)
	* @see org.springframework.data.repository.query.parser.AbstractQueryCreator#and(org.springframework.data.repository.query.parser.Part, java.lang.Object, java.util.Iterator)
	*/
	@Override
	protected Query and(Part part, Query base, Iterator<Object> iterator) {
		
		if (base == null) {
			return create(part, iterator);
		}

		Criteria criteria = from(part.getType(), where(part.getProperty().toDotPath()),
				(PotentiallyConvertingIterator) iterator);
		return base.addCriteria(criteria);
	}

	/*
	* (non-Javadoc)
	*
	* @see
	* org.springframework.data.repository.query.parser.AbstractQueryCreator
	* #or(java.lang.Object, java.lang.Object)
	*/
	@Override
	protected Query or(Query base, Query query) {

		return new Query().or(base, query);
	}

	/*
	* (non-Javadoc)
	*
	* @see
	* org.springframework.data.repository.query.parser.AbstractQueryCreator
	* #complete(java.lang.Object, org.springframework.data.domain.Sort)
	*/
	@Override
	protected Query complete(Query query, Sort sort) {
		
		if (query == null) {
			return null;
		}

		QueryUtils.applySorting(query, sort);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created query " + query.getQueryObject());
		}

		return query;
	}

	/**
	 * Populates the given {@link CriteriaDefinition} depending on the {@link Type} given.
	 * 
	 * @param type
	 * @param criteria
	 * @param parameters
	 * @return
	 */
	private Criteria from(Type type, Criteria criteria, PotentiallyConvertingIterator parameters) {

		switch (type) {
		case GREATER_THAN:
			return criteria.gt(parameters.nextConverted());
		case GREATER_THAN_EQUAL:
			return criteria.gte(parameters.nextConverted());
		case LESS_THAN:
			return criteria.lt(parameters.nextConverted());
		case LESS_THAN_EQUAL:
			return criteria.lte(parameters.nextConverted());
		case BETWEEN:
			return criteria.gt(parameters.nextConverted()).lt(parameters.nextConverted());
		case IS_NOT_NULL:
			return criteria.ne(null);
		case IS_NULL:
			return criteria.is(null);
		case NOT_IN:
			return criteria.nin(nextAsArray(parameters));
		case IN:
			return criteria.in(nextAsArray(parameters));
		case LIKE:
			String value = parameters.next().toString();
			return criteria.regex(toLikeRegex(value));
		case NEAR:

			Distance distance = accessor.getMaxDistance();
			Point point = nextAs(parameters, Point.class);

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
			if (parameter instanceof Box) {
				return criteria.withinBox((Box) parameter);
			} else if (parameter instanceof Circle) {
				return criteria.withinCenter((Circle) parameter);
			}
			throw new IllegalArgumentException("Parameter has to be either Box or Circle!");
		case SIMPLE_PROPERTY:
			return criteria.is(parameters.nextConverted());
		case NEGATING_SIMPLE_PROPERTY:
			return criteria.not().is(parameters.nextConverted());
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

	private Object[] nextAsArray(PotentiallyConvertingIterator iterator) {
		Object next = iterator.nextConverted();

		if (next instanceof Collection) {
			return ((Collection<?>) next).toArray();
		} else if (next.getClass().isArray()) {
			return (Object[]) next;
		}

		return new Object[] { next };
	}

	private String toLikeRegex(String source) {

		return source.replaceAll("\\*", ".*");
	}
}