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

package org.springframework.data.document.mongodb.mapping;

import org.springframework.data.document.mongodb.mapping.index.IndexCreationHelper;
import org.springframework.data.mapping.BasicMappingContext;
import org.springframework.data.mapping.model.PersistentEntity;
import org.springframework.data.util.TypeInformation;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MongoMappingContext extends BasicMappingContext {

  protected IndexCreationHelper indexCreationHelper;

  public MongoMappingContext() {
    builder = new MongoMappingConfigurationBuilder();
  }

  public IndexCreationHelper getIndexCreationHelper() {
    return indexCreationHelper;
  }

  public void setIndexCreationHelper(IndexCreationHelper indexCreationHelper) {
    this.indexCreationHelper = indexCreationHelper;
  }

  @Override
  public <T> PersistentEntity<T> addPersistentEntity(TypeInformation typeInformation) {
    PersistentEntity<T> entity = super.addPersistentEntity(typeInformation);
    if (entity instanceof MongoPersistentEntity && null != indexCreationHelper) {
      indexCreationHelper.checkForIndexes((MongoPersistentEntity<?>) entity);
    }
    return entity;
  }
}
