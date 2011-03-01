/*
 * Copyright 2002-2011 the original author or authors.
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

import static org.springframework.data.document.mongodb.repository.QueryUtils.*;

import java.util.List;

import org.springframework.data.document.mongodb.CollectionCallback;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.util.Assert;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


/**
 * Base class for {@link RepositoryQuery} implementations for Mongo.
 * 
 * @author Oliver Gierke
 */
public abstract class AbstractMongoQuery implements RepositoryQuery {

    private final MongoQueryMethod method;
    private final MongoTemplate template;


    /**
     * Creates a new {@link AbstractMongoQuery} from the given {@link QueryMethod} and
     * {@link MongoTemplate}.
     * 
     * @param method
     * @param template
     */
    public AbstractMongoQuery(MongoQueryMethod method, MongoTemplate template) {

        Assert.notNull(template);
        Assert.notNull(method);

        this.method = method;
        this.template = template;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.query.RepositoryQuery#execute(java
     * .lang.Object[])
     */
    public Object execute(Object[] parameters) {

        ParameterAccessor accessor =
                new ParametersParameterAccessor(method.getParameters(), parameters);
        Query query = createQuery(new ConvertingParameterAccessor(template.getConverter(), accessor));

        if (method.isCollectionQuery()) {
            return new CollectionExecution().execute(query);
        } else if (method.isPageQuery()) {
            return new PagedExecution(accessor.getPageable()).execute(query);
        } else {
            return new SingleEntityExecution().execute(query);
        }
    }
    
    /**
     * Create a {@link Query} instance using the given {@link ParameterAccessor}
     * @param accessor
     * @param converter
     * @return
     */
    protected abstract Query createQuery(ConvertingParameterAccessor accessor);
    	

    private abstract class Execution {

        abstract Object execute(Query query);


        protected List<?> readCollection(Query query) {

            String collectionName = getCollectionName(method.getDomainClass());
            return template
                    .find(collectionName, query, method.getReturnedDomainClass());
        }
    }

    /**
     * {@link Execution} for collection returning queries.
     * 
     * @author Oliver Gierke
     */
    class CollectionExecution extends Execution {

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.springframework.data.document.mongodb.repository.MongoQuery.Execution
         * #execute(com.mongodb.DBObject)
         */
        @Override
        public Object execute(Query query) {

            return readCollection(query);
        }
    }

    /**
     * {@link Execution} for pagination queries.
     * 
     * @author Oliver Gierke
     */
    class PagedExecution extends Execution {

        private final Pageable pageable;


        /**
         * Creates a new {@link PagedExecution}.
         * 
         * @param pageable
         */
        public PagedExecution(Pageable pageable) {

            Assert.notNull(pageable);
            this.pageable = pageable;
        }


        /*
         * (non-Javadoc)
         * 
         * @see
         * org.springframework.data.document.mongodb.repository.MongoQuery.Execution
         * #execute(com.mongodb.DBObject)
         */
        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Object execute(Query query) {

            String collectionName = getCollectionName(method.getDomainClass());
            int count = getCollectionCursor(collectionName, query.getQueryObject()).count();

            List<?> result =
                    template.find(collectionName, applyPagination(query, pageable),
                            method.getReturnedDomainClass());

            return new PageImpl(result, pageable, count);
        }


        private DBCursor getCollectionCursor(String collectionName, final DBObject query) {

            return template.execute(collectionName, new CollectionCallback<DBCursor>() {

                public DBCursor doInCollection(DBCollection collection) {

                    return collection.find(query);
                }
            });
        }
    }

    /**
     * {@link Execution} to return a single entity.
     * 
     * @author Oliver Gierke
     */
    class SingleEntityExecution extends Execution {

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.springframework.data.document.mongodb.repository.MongoQuery.Execution
         * #execute(com.mongodb.DBObject)
         */
        @Override
        Object execute(Query query) {

            List<?> result = readCollection(query);
            return result.isEmpty() ? null : result.get(0);
        }
    }
}
