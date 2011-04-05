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

import org.springframework.data.mapping.BasicPersistentEntity;
import org.springframework.data.mapping.model.MappingContext;
import org.springframework.data.mapping.model.PersistentEntity;
import org.springframework.data.util.TypeInformation;

/**
 * Mongo specific {@link PersistentEntity} implementation that adds Mongo specific meta-data such as the collection name
 * and the like.
 *
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke
 */
public class MongoPersistentEntity<T> extends BasicPersistentEntity<T> {

  protected String collection;

  /**
   * Creates a new {@link MongoPersistentEntity} with the given {@link MappingContext} and {@link TypeInformation}. Will
   * default the collection name to the entities simple type name.
   *
   * @param mappingContext
   * @param typeInformation
   */
  public MongoPersistentEntity(MappingContext mappingContext, TypeInformation typeInformation) {
    super(mappingContext, typeInformation);
    this.collection = typeInformation.getType().getSimpleName().toLowerCase();
    if (typeInformation.getType().isAnnotationPresent(Document.class)) {
      Document d = typeInformation.getType().getAnnotation(Document.class);
      if (!"".equals(d.collection())) {
        this.collection = d.collection();
      }
    }
  }

  /**
   * Returns the collection the entity should be stored in.
   *
   * @return
   */
  public String getCollection() {
    if (null == collection) {
      this.collection = type.getSimpleName().toLowerCase();
    }
    return collection;
  }


  public void setCollection(String collection) {
    this.collection = collection;
  }
}
