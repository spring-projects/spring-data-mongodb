/*
 * Copyright 2010-2011 the original author or authors.
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

import static org.springframework.data.document.mongodb.repository.MongoCursorUtils.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.springframework.data.document.mongodb.MongoConverter;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.support.IsNewAware;
import org.springframework.data.repository.support.RepositorySupport;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.QueryBuilder;


/**
 * Repository base implementation for Mongo.
 * 
 * @author Oliver Gierke
 */
public class SimpleMongoRepository<T, ID extends Serializable> extends
        RepositorySupport<T, ID> implements PagingAndSortingRepository<T, ID> {

    private final MongoTemplate template;
    private MongoEntityInformation entityInformation;


    /**
     * Creates a ew {@link SimpleMongoRepository} for the given domain class and
     * {@link MongoTemplate}.
     * 
     * @param domainClass
     * @param template
     */
    public SimpleMongoRepository(Class<T> domainClass,
            MongoTemplate template) {

        super(domainClass);

        Assert.notNull(template);
        this.template = template;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.Repository#save(java.lang.Object)
     */
    public T save(T entity) {

        template.save(entity);
        return entity;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.Repository#save(java.lang.Iterable)
     */
    public List<T> save(Iterable<? extends T> entities) {

        List<T> result = new ArrayList<T>();

        for (T entity : entities) {
            template.save(entity);
            result.add(entity);
        }

        return result;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.Repository#findById(java.io.Serializable
     * )
     */
    public T findById(ID id) {
    	
    	MongoConverter converter = template.getConverter();
    	
		List<T> result = template.find(QueryBuilder.start("_id").is(converter.convertObjectId(id)).get(),
				getDomainClass());
		return result.isEmpty() ? null : result.get(0);
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.Repository#exists(java.io.Serializable
     * )
     */
    public boolean exists(ID id) {

        return findById(id) == null;
    }


    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.Repository#findAll()
     */
    public List<T> findAll() {

        return template.getCollection(getDomainClass());
    }


    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.Repository#count()
     */
    public Long count() {

        return template.getCollection(template.getDefaultCollectionName())
                .count();
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.Repository#delete(java.lang.Object)
     */
    public void delete(T entity) {

        QueryBuilder builder =
                QueryBuilder.start(entityInformation.getFieldName()).is(
                        entityInformation.getId(entity));
        template.remove(builder.get());
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.Repository#delete(java.lang.Iterable)
     */
    public void delete(Iterable<? extends T> entities) {

        for (T entity : entities) {
            delete(entity);
        }
    }


    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.Repository#deleteAll()
     */
    public void deleteAll() {

        template.dropCollection(template.getDefaultCollectionName());
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.PagingAndSortingRepository#findAll
     * (org.springframework.data.domain.Pageable)
     */
    public Page<T> findAll(final Pageable pageable) {

        Long count = count();

        List<T> list =
                template.find(new BasicDBObject(), getDomainClass(),
                        withPagination(pageable));

        return new PageImpl<T>(list, pageable, count);
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.PagingAndSortingRepository#findAll
     * (org.springframework.data.domain.Sort)
     */
    public List<T> findAll(final Sort sort) {

        return template.find(new BasicDBObject(), getDomainClass(),
                withSorting(sort));
    }


    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.support.RepositorySupport#
     * createIsNewStrategy(java.lang.Class)
     */
    @Override
    protected IsNewAware createIsNewStrategy(Class<?> domainClass) {

        if (entityInformation == null) {
            this.entityInformation = new MongoEntityInformation(domainClass);
        }

        return entityInformation;
    }
}
