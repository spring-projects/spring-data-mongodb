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

import java.io.Serializable;
import java.lang.reflect.Method;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.support.RepositoryFactorySupport;
import org.springframework.data.repository.support.RepositorySupport;
import org.springframework.util.Assert;


/**
 * {@link FactoryBean} to create {@link MongoRepository} instances.
 * 
 * @author Oliver Gierke
 */
public class MongoRepositoryFactoryBean extends
        RepositoryFactoryBeanSupport<Repository<?, ?>> {

    private MongoTemplate template;


    /**
     * Configures the {@link MongoTemplate} to be used.
     * 
     * @param template the template to set
     */
    public void setTemplate(MongoTemplate template) {

        this.template = template;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.support.RepositoryFactoryBeanSupport
     * #createRepositoryFactory()
     */
    @Override
    protected RepositoryFactorySupport createRepositoryFactory() {

        return new MongoRepositoryFactory();
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.support.RepositoryFactoryBeanSupport
     * #afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() {

        super.afterPropertiesSet();
        Assert.notNull(template, "MongoTemplate must not be null!");
    }

    /**
     * Repository to create {@link MongoRepository} instances.
     * 
     * @author Oliver Gierke
     */
    private class MongoRepositoryFactory extends RepositoryFactorySupport {

        @Override
        protected <T, ID extends Serializable> RepositorySupport<T, ID> getTargetRepository(
                Class<T> domainClass) {

            return new SimpleMongoRepository<T, ID>(domainClass, template);
        }


        @Override
        @SuppressWarnings("rawtypes")
        protected Class<? extends RepositorySupport> getRepositoryClass() {

            return SimpleMongoRepository.class;
        }


        @Override
        protected QueryLookupStrategy getQueryLookupStrategy(Key key) {

            return new MongoQueryLookupStrategy();
        }

        /**
         * {@link QueryLookupStrategy} to create {@link MongoQuery} instances.
         * 
         * @author Oliver Gierke
         */
        private class MongoQueryLookupStrategy implements QueryLookupStrategy {

            public RepositoryQuery resolveQuery(Method method) {

                return new MongoQuery(new QueryMethod(method), template);
            }
        }
    }
}
