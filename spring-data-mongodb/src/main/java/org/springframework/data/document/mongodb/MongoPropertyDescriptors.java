/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.document.mongodb;

import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.util.Assert;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.*;

/**
 * An iterable of {@link MongoPropertyDescriptor}s that allows dedicated access to the {@link MongoPropertyDescriptor}
 * that captures the id-property.
 *
 * @author Oliver Gierke
 */
public class MongoPropertyDescriptors implements Iterable<MongoPropertyDescriptors.MongoPropertyDescriptor> {

  private final Collection<MongoPropertyDescriptors.MongoPropertyDescriptor> descriptors;
  private final MongoPropertyDescriptors.MongoPropertyDescriptor idDescriptor;

  /**
   * Creates the {@link MongoPropertyDescriptors} for the given type.
   *
   * @param type
   */
  public MongoPropertyDescriptors(Class<?> type) {

    Assert.notNull(type);
    Set<MongoPropertyDescriptors.MongoPropertyDescriptor> descriptors = new HashSet<MongoPropertyDescriptors.MongoPropertyDescriptor>();
    MongoPropertyDescriptors.MongoPropertyDescriptor idDesciptor = null;

    for (PropertyDescriptor candidates : BeanUtils.getPropertyDescriptors(type)) {
      MongoPropertyDescriptor descriptor = new MongoPropertyDescriptors.MongoPropertyDescriptor(candidates);
      descriptors.add(descriptor);
      if (descriptor.isIdProperty()) {
        idDesciptor = descriptor;
      }
    }

    this.descriptors = Collections.unmodifiableSet(descriptors);
    this.idDescriptor = idDesciptor;
  }

  /**
   * Returns the {@link MongoPropertyDescriptor} for the id property.
   *
   * @return the idDescriptor
   */
  public MongoPropertyDescriptors.MongoPropertyDescriptor getIdDescriptor() {
    return idDescriptor;
  }

  /*
    * (non-Javadoc)
    *
    * @see java.lang.Iterable#iterator()
    */
  public Iterator<MongoPropertyDescriptors.MongoPropertyDescriptor> iterator() {
    return descriptors.iterator();
  }

  /**
   * Simple value object to have a more suitable abstraction for MongoDB specific property handling.
   *
   * @author Oliver Gierke
   */
  public static class MongoPropertyDescriptor {

    public static Collection<Class<?>> SUPPORTED_ID_CLASSES;

    static {
      Set<Class<?>> classes = new HashSet<Class<?>>();
      classes.add(ObjectId.class);
      classes.add(String.class);
      classes.add(BigInteger.class);
      SUPPORTED_ID_CLASSES = Collections.unmodifiableCollection(classes);
    }

    private static final String ID_PROPERTY = "id";
    static final String ID_KEY = "_id";

    private final PropertyDescriptor delegate;

    /**
     * Creates a new {@link MongoPropertyDescriptor} for the given {@link PropertyDescriptor}.
     *
     * @param descriptor
     */
    public MongoPropertyDescriptor(PropertyDescriptor descriptor) {
      Assert.notNull(descriptor);
      this.delegate = descriptor;
    }

    /**
     * Returns whether the property is the id-property. Will be identified by name for now ({@value #ID_PROPERTY}).
     *
     * @return
     */
    public boolean isIdProperty() {
      return ID_PROPERTY.equals(delegate.getName()) || ID_KEY.equals(delegate.getName());
    }

    /**
     * Returns whether the property is of one of the supported id types. Currently we support {@link String},
     * {@link ObjectId} and {@link BigInteger}.
     *
     * @return
     */
    public boolean isOfIdType() {
      return SUPPORTED_ID_CLASSES.contains(delegate.getPropertyType());
    }

    /**
     * Returns the key that shall be used for mapping. Will return {@value #ID_KEY} for the id property and the
     * plain name for all other ones.
     *
     * @return
     */
    public String getKeyToMap() {
      return isIdProperty() ? ID_KEY : delegate.getName();
    }

    /**
     * Returns the name of the property.
     *
     * @return
     */
    public String getName() {
      return delegate.getName();
    }

    /**
     * Returns whether the underlying property is actually mappable. By default this will exclude the
     * {@literal class} property and only include properties with a getter.
     *
     * @return
     */
    public boolean isMappable() {
      return !delegate.getName().equals("class") && delegate.getReadMethod() != null;
    }

    /**
     * Returns the plain property type.
     *
     * @return
     */
    public Class<?> getPropertyType() {
      return delegate.getPropertyType();
    }

    /**
     * Returns the type type to be set. Will return the setter method's type and fall back to the getter method's
     * return type in case no setter is available. Useful for further (generics) inspection.
     *
     * @return
     */
    public Type getTypeToSet() {

      Method method = delegate.getWriteMethod();
      return method == null ? delegate.getReadMethod().getGenericReturnType()
          : method.getGenericParameterTypes()[0];
    }

    /**
     * Returns whther we describe a {@link Map}.
     *
     * @return
     */
    public boolean isMap() {
      return Map.class.isAssignableFrom(getPropertyType());
    }

    /**
     * Returns whether the descriptor is for a collection.
     *
     * @return
     */
    public boolean isCollection() {
      return Collection.class.isAssignableFrom(getPropertyType());
    }

    /**
     * Returns whether the descriptor is for an {@link Enum}.
     *
     * @return
     */
    public boolean isEnum() {
      return Enum.class.isAssignableFrom(getPropertyType());
    }

    /*
       * (non-Javadoc)
       *
       * @see java.lang.Object#equals(java.lang.Object)
       */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || !getClass().equals(obj.getClass())) {
        return false;
      }
      MongoPropertyDescriptor that = (MongoPropertyDescriptor) obj;
      return that.delegate.equals(this.delegate);
    }

    /*
       * (non-Javadoc)
       *
       * @see java.lang.Object#hashCode()
       */
    @Override
    public int hashCode() {
      return delegate.hashCode();
    }
  }
}