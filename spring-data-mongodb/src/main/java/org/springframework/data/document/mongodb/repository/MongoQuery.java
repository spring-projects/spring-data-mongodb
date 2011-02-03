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

import static org.springframework.data.document.mongodb.repository.QueryUtils.*;

import java.util.List;

import org.springframework.data.document.mongodb.CollectionCallback;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.builder.QuerySpec;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.SimpleParameterAccessor;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


/**
 * {@link RepositoryQuery} implementation for Mongo.
 * 
 * @author Oliver Gierke
 */
public class MongoQuery implements RepositoryQuery {

    private final QueryMethod method;
    private final MongoTemplate template;
    private final PartTree tree;


    /**
     * Creates a new {@link MongoQuery} from the given {@link QueryMethod} and
     * {@link MongoTemplate}.
     * 
     * @param method
     * @param template
     */
    public MongoQuery(QueryMethod method, MongoTemplate template) {

        Assert.notNull(template);
        Assert.notNull(method);

        this.method = method;
        this.template = template;
        this.tree = new PartTree(method.getName(), method.getDomainClass());
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.query.RepositoryQuery#execute(java
     * .lang.Object[])
     */
    public Object execute(Object[] parameters) {

        SimpleParameterAccessor accessor =
                new SimpleParameterAccessor(method.getParameters(), parameters);
        QuerySpec spec = new QuerySpec();

        MongoQueryCreator creator =
                new MongoQueryCreator(spec, tree, accessor,
                        template.getConverter());
        creator.createQuery();

        if (method.isCollectionQuery()) {
            return new CollectionExecution().execute(spec);
        } else if (method.isPageQuery()) {
            return new PagedExecution(creator, accessor.getPageable())
                    .execute(spec);
        } else {
            return new SingleEntityExecution().execute(spec);
        }
    }

    private abstract class Execution {

        abstract Object execute(QuerySpec query);


        protected List<?> readCollection(QuerySpec query) {

            return template.query(query.build(), method.getDomainClass());
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
        public Object execute(QuerySpec query) {

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
        private final MongoQueryCreator creator;


        /**
         * Creates a new {@link PagedExecution}.
         * 
         * @param pageable
         */
        public PagedExecution(MongoQueryCreator creator, Pageable pageable) {

            Assert.notNull(creator);
            Assert.notNull(pageable);
            this.creator = creator;
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
        Object execute(QuerySpec query) {

            int count = getCollectionCursor(query.getQueryObject()).count();

            List<?> result =
                    template.query(applyPagination(query, pageable),
                            method.getDomainClass());

            return new PageImpl(result, pageable, count);
        }


        private DBCursor getCollectionCursor(final DBObject query) {

            return template.execute(new CollectionCallback<DBCursor>() {

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
        Object execute(QuerySpec query) {

            List<?> result = readCollection(query);
            return result.isEmpty() ? null : result.get(0);
        }
    }
}
