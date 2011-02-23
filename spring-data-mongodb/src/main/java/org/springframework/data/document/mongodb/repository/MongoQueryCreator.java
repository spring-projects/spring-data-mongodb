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
package org.springframework.data.document.mongodb.repository;

import static org.springframework.data.document.mongodb.query.Criteria.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.document.mongodb.query.Criteria;
import org.springframework.data.document.mongodb.query.CriteriaDefinition;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;
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
class MongoQueryCreator extends AbstractQueryCreator<Query, Criteria> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoQueryCreator.class);


    /**
     * Creates a new {@link MongoQueryCreator} from the given {@link PartTree}
     * and {@link ParametersParameterAccessor}.
     * 
     * @param tree
     * @param accessor
     */
    public MongoQueryCreator(PartTree tree, ParameterAccessor accessor) {

        super(tree, accessor);
    }


    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#create(org.springframework.data.repository.query.parser.Part, java.util.Iterator)
     */
    @Override
    protected Criteria create(Part part, Iterator<Object> iterator) {

        return from(part.getType(),
                where(part.getProperty().toDotPath()), iterator);
    }


    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#and(org.springframework.data.repository.query.parser.Part, java.lang.Object, java.util.Iterator)
     */
    @Override
    protected Criteria and(Part part, Criteria base,
            Iterator<Object> iterator) {

        return from(part.getType(), where(part.getProperty().toDotPath()),
                iterator);
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

        base.or(Collections.singletonList(new Query(criteria)));
        return base;
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

        Query query = new Query(criteria);

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
    private Criteria from(Type type, Criteria criteria,
            Iterator<Object> parameters) {

        switch (type) {
        case GREATER_THAN:
            return criteria.gt(parameters.next());
        case LESS_THAN:
            return criteria.lt(parameters.next());
        case BETWEEN:
            return criteria.gt(parameters.next()).lt(
            		parameters.next());
        case IS_NOT_NULL:
            return criteria.not().is(null);
        case IS_NULL:
            return criteria.is(null);
        case NOT_IN:
        	return criteria.nin(nextAsArray(parameters));
        case IN:
        	return criteria.in(nextAsArray(parameters));
        case LIKE:
            String value = parameters.next().toString();
            return criteria.is(toLikeRegex(value));
        case SIMPLE_PROPERTY:
            return criteria.is(parameters.next());
        case NEGATING_SIMPLE_PROPERTY:
            return criteria.not().is(parameters.next());
        }

        throw new IllegalArgumentException("Unsupported keyword!");
    }


    private Object[] nextAsArray(Iterator<Object> iterator) {
    	Object next = iterator.next();
    	
    	if (next instanceof Collection) {
    		return ((Collection<?>) next).toArray();
    	} else if (next.getClass().isArray()) {
    		return (Object[]) next;
    	}
    	
    	return new Object[] { next };
    }

    private Pattern toLikeRegex(String source) {

        String regex = source.replaceAll("\\*", ".*");
        return Pattern.compile(regex);
    }

    
}