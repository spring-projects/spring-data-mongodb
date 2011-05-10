/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.document.mongodb.repository;

import java.io.Serializable;
import org.springframework.data.document.mongodb.mapping.BasicMongoPersistentEntity;
import org.springframework.data.document.mongodb.mapping.MongoPersistentEntity;
import org.springframework.data.document.mongodb.mapping.MongoPersistentProperty;
import org.springframework.data.mapping.MappingBeanHelper;
import org.springframework.data.repository.support.AbstractEntityInformation;

/**
 * {@link MongoEntityInformation} implementation using a {@link BasicMongoPersistentEntity} instance to lookup the necessary
 * information.
 * 
 * @author Oliver Gierke
 */
public class MappingMongoEntityInformation<T, ID extends Serializable> extends AbstractEntityInformation<T, ID>
    implements MongoEntityInformation<T, ID> {

  private final MongoPersistentEntity<T> entityMetadata;

  /**
   * Creates a new {@link MappingMongoEntityInformation} for the given {@link MongoPersistentEntity}.
   * 
   * @param domainClass
   * @param entity
   */
  public MappingMongoEntityInformation(MongoPersistentEntity<T> entity) {
    super(entity.getType());
    this.entityMetadata = entity;
  }

  /* (non-Javadoc)
   * @see org.springframework.data.repository.support.EntityInformation#getId(java.lang.Object)
   */
  @SuppressWarnings("unchecked")
  public ID getId(T entity) {

    MongoPersistentProperty idProperty = entityMetadata.getIdProperty();

    try {
      return (ID) MappingBeanHelper.getProperty(entity, idProperty, idProperty.getType(), false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.springframework.data.repository.support.EntityInformation#getIdType()
   */
  @SuppressWarnings("unchecked")
  public Class<ID> getIdType() {
    return (Class<ID>) entityMetadata.getIdProperty().getType();
  }

  /* (non-Javadoc)
   * @see org.springframework.data.document.mongodb.repository.MongoEntityInformation#getCollectionName()
   */
  public String getCollectionName() {
    return entityMetadata.getCollection();
  }

  /* (non-Javadoc)
   * @see org.springframework.data.document.mongodb.repository.MongoEntityInformation#getIdAttribute()
   */
  public String getIdAttribute() {
    return entityMetadata.getIdProperty().getName();
  }
}
