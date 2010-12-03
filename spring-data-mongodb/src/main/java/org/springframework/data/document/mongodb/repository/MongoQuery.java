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

import java.util.List;

import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.SimpleParameterAccessor;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

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
        Assert.isTrue(!method.isPageQuery(),
                "Pagination queries not supported!");

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
        MongoQueryCreator creator = new MongoQueryCreator(tree, accessor);
        DBObject query = creator.createQuery();

        List<?> result =
                template.query(template.getDefaultCollectionName(), query,
                        method.getDomainClass());

        if (method.isCollectionQuery()) {
            return result;
        } else {
            return result.isEmpty() ? null : result.get(0);
        }
    }
}
