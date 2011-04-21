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

import static org.springframework.data.document.mongodb.query.Criteria.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.document.mongodb.MongoOperations;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.query.Criteria;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.util.Assert;

/**
 * Repository base implementation for Mongo.
 * 
 * @author Oliver Gierke
 */
public class SimpleMongoRepository<T, ID extends Serializable> implements PagingAndSortingRepository<T, ID> {

  private final MongoTemplate template;
  private final MongoEntityInformation<T, ID> entityInformation;

  /**
   * Creates a ew {@link SimpleMongoRepository} for the given {@link MongoEntityInformation} and {@link MongoTemplate}.
   * 
   * @param metadata
   * @param template
   */
  public SimpleMongoRepository(MongoEntityInformation<T, ID> metadata, MongoTemplate template) {

    Assert.notNull(template);
    Assert.notNull(metadata);
    this.entityInformation = metadata;
    this.template = template;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.springframework.data.repository.Repository#save(java.lang.Object)
   */
  public T save(T entity) {

    template.save(entityInformation.getCollectionName(), entity);
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
      save(entity);
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
  public T findOne(ID id) {

    return template.findOne(entityInformation.getCollectionName(), getIdQuery(id), entityInformation.getJavaType());
  }

  private Query getIdQuery(Object id) {
    return new Query(getIdCriteria(id));
  }

  private Criteria getIdCriteria(Object id) {
    return where(entityInformation.getIdAttribute()).is(id);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.springframework.data.repository.Repository#exists(java.io.Serializable
   * )
   */
  public boolean exists(ID id) {

    return findOne(id) != null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.springframework.data.repository.Repository#count()
   */
  public long count() {

    return template.getCollection(entityInformation.getCollectionName()).count();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.springframework.data.repository.Repository#delete(java.lang.Object)
   */
  public void delete(T entity) {

    template.remove(entityInformation.getCollectionName(), getIdQuery(entityInformation.getId(entity)), entity.getClass());
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

    template.dropCollection(entityInformation.getCollectionName());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.springframework.data.repository.Repository#findAll()
   */
  public List<T> findAll() {

    return findAll(new Query());
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
    List<T> list = findAll(QueryUtils.applyPagination(new Query(), pageable));

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

    return findAll(QueryUtils.applySorting(new Query(), sort));
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.springframework.data.repository.Repository#findAll(java.lang.Iterable
   * )
   */
  public List<T> findAll(Iterable<ID> ids) {

    Query query = null;

    for (ID id : ids) {
      if (query == null) {
        query = getIdQuery(id);
      } else {
        query = new Query().or(getIdQuery(id));
      }
    }

    return findAll(query);
  }

  private List<T> findAll(Query query) {

    if (query == null) {
      return Collections.emptyList();
    }

    return template.find(entityInformation.getCollectionName(), query, entityInformation.getJavaType());
  }

  /**
   * Returns the underlying {@link MongoOperations} instance.
   * 
   * @return
   */
  protected MongoOperations getMongoOperations() {

    return this.template;
  }

  /**
   * @return the entityInformation
   */
  protected MongoEntityInformation<T, ID> getEntityInformation() {

    return entityInformation;
  }
}
