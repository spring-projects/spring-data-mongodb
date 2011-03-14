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
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import org.bson.types.ObjectId;
import org.springframework.data.mapping.BasicPersistentProperty;

/**
 * Mongo specific
 * {@link org.springframework.data.mapping.model.PersistentProperty}
 * implementation.
 * 
 * @author Oliver Gierke
 */
public class MongoPersistentProperty<T> extends BasicPersistentProperty<T> {

  private static final Set<Class<?>> SUPPORTED_ID_TYPES = new HashSet<Class<?>>();
  private static final Set<String> SUPPORTED_ID_PROPERTY_NAMES = new HashSet<String>();

  static {
    SUPPORTED_ID_TYPES.add(ObjectId.class);
    SUPPORTED_ID_TYPES.add(String.class);
    SUPPORTED_ID_TYPES.add(BigInteger.class);

    SUPPORTED_ID_PROPERTY_NAMES.add("id");
    SUPPORTED_ID_PROPERTY_NAMES.add("_id");
  }

  /**
   * Creates a new {@link MongoPersistentProperty}.
   * 
   * @param name
   * @param type
   * @param field
   * @param propertyDescriptor
   */
  public MongoPersistentProperty(String name, Class<T> type, Field field,
      PropertyDescriptor propertyDescriptor) {
    super(name, type, field, propertyDescriptor);
  }

  /**
   * Also considers fields as id that are of supported id type and name.
   * 
   * @see #SUPPORTED_ID_PROPERTY_NAMES
   * @see #SUPPORTED_ID_TYPES
   */
  @Override
  public boolean isIdProperty() {
    if (super.isIdProperty()) {
      return true;
    }

    return SUPPORTED_ID_TYPES.contains(field.getType())
        && SUPPORTED_ID_PROPERTY_NAMES.contains(field.getName());
  }
}
