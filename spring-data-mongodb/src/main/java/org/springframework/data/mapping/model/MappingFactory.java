/* Copyright 2004-2005 the original author or authors.
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
package org.springframework.data.mapping.model;

import org.springframework.data.mapping.model.types.*;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

/**
 * <p>An abstract factory for creating persistent property instances.</p>
 * <p/>
 * <p>Subclasses should implement the createMappedForm method in order to
 * provide a mechanisms for representing the property in a form appropriate
 * for mapping to the underlying datastore. Example:</p>
 * <p/>
 * <pre>
 *  <code>
 *      class RelationalPropertyFactory<Column> extends PropertyFactory {
 *            public Column createMappedForm(PersistentProperty mpp) {
 *                return new Column(mpp)
 *            }
 *      }
 *  </code>
 * </pre>
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public abstract class MappingFactory<R, T> {
  public static final Set<String> SIMPLE_TYPES;

  static {
    Set<String> basics = new HashSet<String>();
    basics.add(boolean.class.getName());
    basics.add(long.class.getName());
    basics.add(short.class.getName());
    basics.add(int.class.getName());
    basics.add(byte.class.getName());
    basics.add(float.class.getName());
    basics.add(double.class.getName());
    basics.add(char.class.getName());
    basics.add(Boolean.class.getName());
    basics.add(Long.class.getName());
    basics.add(Short.class.getName());
    basics.add(Integer.class.getName());
    basics.add(Byte.class.getName());
    basics.add(Float.class.getName());
    basics.add(Double.class.getName());
    basics.add(Character.class.getName());
    basics.add(String.class.getName());
    basics.add(java.util.Date.class.getName());
    basics.add(Time.class.getName());
    basics.add(Timestamp.class.getName());
    basics.add(java.sql.Date.class.getName());
    basics.add(BigDecimal.class.getName());
    basics.add(BigInteger.class.getName());
    basics.add(Locale.class.getName());
    basics.add(Calendar.class.getName());
    basics.add(GregorianCalendar.class.getName());
    basics.add(java.util.Currency.class.getName());
    basics.add(TimeZone.class.getName());
    basics.add(Object.class.getName());
    basics.add(Class.class.getName());
    basics.add(byte[].class.getName());
    basics.add(Byte[].class.getName());
    basics.add(char[].class.getName());
    basics.add(Character[].class.getName());
    basics.add(Blob.class.getName());
    basics.add(Clob.class.getName());
    basics.add(Serializable.class.getName());
    basics.add(URI.class.getName());
    basics.add(URL.class.getName());

    SIMPLE_TYPES = Collections.unmodifiableSet(basics);
  }


  public MappingFactory() {
    super();
  }

  public static boolean isSimpleType(Class propType) {
    if (propType == null) return false;
    if (propType.isArray()) {
      return isSimpleType(propType.getComponentType());
    }
    final String typeName = propType.getName();
    return isSimpleType(typeName);
  }

  public static boolean isSimpleType(final String typeName) {
    return SIMPLE_TYPES.contains(typeName);
  }

  /**
   * Creates the mapped form of a persistent entity
   *
   * @param entity The entity
   * @return The mapped form
   */
  public abstract R createMappedForm(PersistentEntity entity);

  /**
   * Creates the mapped form of a PersistentProperty instance
   *
   * @param mpp The PersistentProperty instance
   * @return The mapped form
   */
  public abstract T createMappedForm(PersistentProperty mpp);

  /**
   * Creates an identifier property
   *
   * @param owner   The owner
   * @param context The context
   * @param pd      The PropertyDescriptor
   * @return An Identity instance
   */
  public Identity<T> createIdentity(PersistentEntity owner, MappingContext context, PropertyDescriptor pd) {
    return new Identity<T>(owner, context, pd) {
      public PropertyMapping<T> getMapping() {
        return createPropertyMapping(this, owner);
      }
    };
  }

  /**
   * Creates a simple property type used for mapping basic types such as String, long, integer etc.
   *
   * @param owner   The owner
   * @param context The MappingContext
   * @param pd      The PropertyDescriptor
   * @return A Simple property type
   */
  public Simple<T> createSimple(PersistentEntity owner, MappingContext context, PropertyDescriptor pd) {
    return new Simple<T>(owner, context, pd) {
      public PropertyMapping<T> getMapping() {
        return createPropertyMapping(this, owner);
      }
    };
  }

  protected PropertyMapping<T> createPropertyMapping(final PersistentProperty<T> property, final PersistentEntity owner) {
    return new PropertyMapping<T>() {
      public ClassMapping getClassMapping() {
        return owner.getMapping();
      }

      public T getMappedForm() {
        return createMappedForm(property);
      }
    };
  }

  /**
   * Creates a one-to-one association type used for mapping a one-to-one association between entities
   *
   * @param entity   The entity
   * @param context  The context
   * @param property The property
   * @return The ToOne instance
   */
  public ToOne createOneToOne(PersistentEntity entity, MappingContext context, PropertyDescriptor property) {
    return new OneToOne<T>(entity, context, property) {
      public PropertyMapping getMapping() {
        return createPropertyMapping(this, owner);
      }
    };
  }

  /**
   * Creates a many-to-one association type used for a mapping a many-to-one association between entities
   *
   * @param entity   The entity
   * @param context  The context
   * @param property The property
   * @return The ToOne instance
   */
  public ToOne createManyToOne(PersistentEntity entity, MappingContext context, PropertyDescriptor property) {
    return new ManyToOne<T>(entity, context, property) {
      public PropertyMapping getMapping() {
        return createPropertyMapping(this, owner);
      }
    };

  }

  /**
   * Creates a {@link OneToMany} type used to model a one-to-many association between entities
   *
   * @param entity   The entity
   * @param context  The context
   * @param property The property
   * @return The {@link OneToMany} instance
   */
  public OneToMany createOneToMany(PersistentEntity entity, MappingContext context, PropertyDescriptor property) {
    return new OneToMany<T>(entity, context, property) {
      public PropertyMapping getMapping() {
        return createPropertyMapping(this, owner);
      }
    };

  }

  /**
   * Creates a {@link ManyToMany} type used to model a many-to-many association between entities
   *
   * @param entity   The entity
   * @param context  The context
   * @param property The property
   * @return The {@link ManyToMany} instance
   */
  public ManyToMany createManyToMany(PersistentEntity entity, MappingContext context, PropertyDescriptor property) {
    return new ManyToMany<T>(entity, context, property) {
      public PropertyMapping getMapping() {
        return createPropertyMapping(this, owner);
      }
    };
  }

  /**
   * Creates an {@link Embedded} type used to model an embedded association (composition)
   *
   * @param entity   The entity
   * @param context  The context
   * @param property The property
   * @return The {@link Embedded} instance
   */
  public Embedded createEmbedded(PersistentEntity entity,
                                 MappingContext context, PropertyDescriptor property) {
    return new Embedded<T>(entity, context, property) {
      public PropertyMapping getMapping() {
        return createPropertyMapping(this, owner);
      }
    };
  }

  /**
   * Creates a {@link Basic} collection type
   *
   * @param entity   The entity
   * @param context  The context
   * @param property The property
   * @return The Basic collection type
   */
  public Basic createBasicCollection(PersistentEntity entity,
                                     MappingContext context, PropertyDescriptor property) {
    return new Basic(entity, context, property) {
      public PropertyMapping getMapping() {
        return createPropertyMapping(this, owner);
      }

    };
  }
}

