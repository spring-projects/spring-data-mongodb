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

package org.springframework.data.document.mongodb.mapping.event;

import java.util.Set;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.data.document.mongodb.mapping.MongoMappingContext;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MappingConfigurationListener implements ApplicationListener<ContextRefreshedEvent> {

  private MongoMappingContext mappingContext;
  private Set<String> initialEntitySet;

  public MappingConfigurationListener() {
  }

  public MongoMappingContext getMappingContext() {
    return mappingContext;
  }

  public void setMappingContext(MongoMappingContext mappingContext) {
    this.mappingContext = mappingContext;
  }

  public Set<String> getInitialEntitySet() {
    return initialEntitySet;
  }

  public void setInitialEntitySet(Set<String> initialEntitySet) {
    this.initialEntitySet = initialEntitySet;
  }

  public void onApplicationEvent(ContextRefreshedEvent event) {
    for (String className : initialEntitySet) {
      try {
        Class<?> clazz = Class.forName(className);
        if (null == mappingContext.getPersistentEntity(clazz)) {
          mappingContext.addPersistentEntity(clazz);
        }
      } catch (ClassNotFoundException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }
}
