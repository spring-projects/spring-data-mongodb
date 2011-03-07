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

import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.mapping.model.AbstractMappingContext;
import org.springframework.data.mapping.model.MappingConfigurationStrategy;
import org.springframework.data.mapping.model.MappingFactory;
import org.springframework.data.mapping.model.PersistentEntity;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MongoMappingContext extends AbstractMappingContext {

  private MongoTemplate mongo;
  private MongoMappingFactory mappingFactory = null;
  private MappingConfigurationStrategy strategy = null;

  public MongoMappingContext(MongoTemplate mongo) {
    this.mongo = mongo;
    this.mappingFactory = new MongoMappingFactory();
    strategy = new MongoMappingConfigurationStrategy(mongo, mappingFactory);
  }

  public MongoMappingContext(MongoTemplate mongo, MongoMappingFactory mappingFactory) {
    this.mongo = mongo;
    this.mappingFactory = mappingFactory;
    strategy = new MongoMappingConfigurationStrategy(mongo, mappingFactory);
  }

  @Override
  protected PersistentEntity createPersistentEntity(Class javaClass) {
    MongoPersistentEntity entity = new MongoPersistentEntity(javaClass, this);
    return entity;
  }

  public MappingConfigurationStrategy getMappingSyntaxStrategy() {
    return strategy;
  }

  public MappingFactory getMappingFactory() {
    return mappingFactory;
  }
}
