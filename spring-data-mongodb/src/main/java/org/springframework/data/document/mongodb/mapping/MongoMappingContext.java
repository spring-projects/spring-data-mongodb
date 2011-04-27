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

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.Set;

import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;
import org.springframework.data.mapping.AbstractMappingContext;
import org.springframework.data.mapping.MappingBeanHelper;
import org.springframework.data.util.TypeInformation;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MongoMappingContext extends AbstractMappingContext<BasicMongoPersistentEntity<?>, MongoPersistentProperty> {

  public MongoMappingContext() {
    augmentSimpleTypes();
  }

  protected void augmentSimpleTypes() {
    // Augment simpleTypes with MongoDB-specific classes
    Set<Class<?>> simpleTypes = MappingBeanHelper.getSimpleTypes();
    simpleTypes.add(com.mongodb.DBRef.class);
    simpleTypes.add(ObjectId.class);
    simpleTypes.add(CodeWScope.class);
    simpleTypes.add(Character.class);
  }

  @Override
  public MongoPersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor, BasicMongoPersistentEntity<?> owner) {
    return new MongoPersistentProperty(field, descriptor, owner);
  }
  
  /* (non-Javadoc)
   * @see org.springframework.data.mapping.BasicMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation, org.springframework.data.mapping.model.MappingContext)
   */
  @Override
  @SuppressWarnings("rawtypes")
  protected BasicMongoPersistentEntity<?> createPersistentEntity(TypeInformation typeInformation) {
    return new BasicMongoPersistentEntity(typeInformation);
  }
}
