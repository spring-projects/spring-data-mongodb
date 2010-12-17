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

import java.util.regex.Pattern;

import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.SimpleParameterAccessor;
import org.springframework.data.repository.query.SimpleParameterAccessor.BindableParameterIterator;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;

import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;


/**
 * Custom query creator to create Mongo criterias.
 * 
 * @author Oliver Gierke
 */
class MongoQueryCreator extends AbstractQueryCreator<DBObject, QueryBuilder> {

    /**
     * Creates a new {@link MongoQueryCreator} from the given {@link PartTree}
     * and {@link SimpleParameterAccessor}.
     * 
     * @param tree
     * @param accessor
     */
    public MongoQueryCreator(PartTree tree, SimpleParameterAccessor accessor) {

        super(tree, accessor);
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.document.mongodb.repository.AbstractQueryCreator
     * #create(org.springframework.data.repository.query.parser.Part,
     * org.springframework
     * .data.repository.query.SimpleParameterAccessor.BindableParameterIterator)
     */
    @Override
    protected QueryBuilder create(Part part, BindableParameterIterator iterator) {

        return from(part.getType(), QueryBuilder.start(part.getProperty()),
                iterator);
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.document.mongodb.repository.AbstractQueryCreator
     * #handlePart(org.springframework.data.repository.query.parser.Part,
     * org.springframework
     * .data.repository.query.SimpleParameterAccessor.BindableParameterIterator)
     */
    @Override
    protected QueryBuilder and(Part part, QueryBuilder base,
            BindableParameterIterator iterator) {

        return from(part.getType(), base.and(part.getProperty()), iterator);
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.document.mongodb.repository.AbstractQueryCreator
     * #reduceCriterias(java.lang.Object, java.lang.Object)
     */
    @Override
    protected QueryBuilder or(QueryBuilder base, QueryBuilder criteria) {

        base.or(criteria.get());
        return base;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.document.mongodb.repository.AbstractQueryCreator
     * #finalize(java.lang.Object)
     */
    @Override
    protected DBObject finalize(QueryBuilder criteria, Sort sort) {

        return criteria.get();
    }


    /**
     * Populates the given {@link Criteria} depending on the {@link Type} given.
     * 
     * @param type
     * @param criteria
     * @param parameters
     * @return
     */
    private QueryBuilder from(Type type, QueryBuilder criteria,
            BindableParameterIterator parameters) {

        switch (type) {
        case GREATER_THAN:
            return criteria.greaterThan(parameters.next());
        case LESS_THAN:
            return criteria.lessThan(parameters.next());
        case BETWEEN:
            return criteria.greaterThan(parameters.next()).lessThan(parameters.next());
        case IS_NOT_NULL:
            return criteria.notEquals(null);
        case IS_NULL:
            return criteria.is(null);
        case LIKE:
            String value = parameters.next().toString();
            return criteria.regex(toLikeRegex(value));
        case SIMPLE_PROPERTY:
            return criteria.is(parameters.next());
        case NEGATING_SIMPLE_PROPERTY:
            return criteria.notEquals(parameters.next());
        }

        throw new IllegalArgumentException("Unsupported keyword!");
    }


    private Pattern toLikeRegex(String source) {

        String regex = source.replaceAll("\\*", ".*");
        return Pattern.compile(regex);
    }
}