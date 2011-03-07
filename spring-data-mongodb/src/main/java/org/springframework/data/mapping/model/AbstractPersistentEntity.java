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

import org.springframework.beans.BeanUtils;
import org.springframework.data.mapping.model.types.Association;
import org.springframework.data.mapping.model.types.OneToMany;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * Abstract implementation to be subclasses on a per datastore basis
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public abstract class AbstractPersistentEntity<T> implements PersistentEntity<T> {

  protected Class<T> javaClass;
  protected List<PersistentProperty> persistentProperties;
  protected List<Association> associations;
  protected Map<String, PersistentProperty> propertiesByName = new HashMap<String, PersistentProperty>();
  protected MappingContext context;
  protected PersistentProperty identity;
  protected List<String> persistentPropertyNames;
  private String decapitalizedName;
  protected Set owners;
  private PersistentEntity parentEntity = null;
  private boolean external;

  public AbstractPersistentEntity(Class<T> javaClass, MappingContext context) {
    if (javaClass == null) throw new IllegalArgumentException("The argument [javaClass] cannot be null");
    this.javaClass = javaClass;
    this.context = context;
    this.decapitalizedName = Introspector.decapitalize(javaClass.getSimpleName());
  }

  public boolean isExternal() {
    return external;
  }


  public void setExternal(boolean external) {
    this.external = external;
  }

  public MappingContext getMappingContext() {
    return this.context;
  }

  public void afterPropertiesSet() throws Exception {
    this.identity = context.getMappingSyntaxStrategy().getIdentity(javaClass, context);
    this.owners = context.getMappingSyntaxStrategy().getOwningEntities(javaClass, context);
    this.persistentProperties = context.getMappingSyntaxStrategy().getPersistentProperties(javaClass, context);
    persistentPropertyNames = new ArrayList<String>();
    associations = new ArrayList();
    for (PersistentProperty persistentProperty : persistentProperties) {
      if (!(persistentProperty instanceof OneToMany))
        persistentPropertyNames.add(persistentProperty.getName());
      if (persistentProperty instanceof Association) {
        associations.add((Association) persistentProperty);
      }
    }
    for (PersistentProperty persistentProperty : persistentProperties) {
      propertiesByName.put(persistentProperty.getName(), persistentProperty);
    }

    Class<?> superClass = javaClass.getSuperclass();
    if (superClass != null
        && !superClass.equals(Object.class)
        && !Modifier.isAbstract(superClass.getModifiers())) {
      this.parentEntity = context.addPersistentEntity(superClass);
    }


    getMapping().getMappedForm(); // initialize mapping
  }

  public boolean hasProperty(String name, Class type) {
    final PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(getJavaClass(), name);
    return pd != null && pd.getPropertyType().equals(type);
  }

  public boolean isIdentityName(String propertyName) {
    return getIdentity().getName().equals(propertyName);
  }

  public PersistentEntity getParentEntity() {
    return parentEntity;
  }

  public String getDiscriminator() {
    return getJavaClass().getSimpleName();
  }

  public PersistentEntity getRootEntity() {
    PersistentEntity root = this;
    PersistentEntity parent = getParentEntity();
    while (parent != null) {
      root = parent;
      parent = parent.getParentEntity();
    }
    return root;
  }

  public boolean isRoot() {
    return getParentEntity() == null;
  }

  public boolean isOwningEntity(PersistentEntity owner) {
    return owner != null && owners.contains(owner.getJavaClass());
  }

  public String getDecapitalizedName() {
    return this.decapitalizedName;
  }

  public List<String> getPersistentPropertyNames() {
    return this.persistentPropertyNames;
  }

  public T newInstance() {
    try {
      return getJavaClass().newInstance();
    } catch (InstantiationException e) {
      throw new EntityInstantiationException("Unable to create entity of type [" + getJavaClass() + "]: " + e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new EntityInstantiationException("Unable to create entity of type [" + getJavaClass() + "]: " + e.getMessage(), e);
    }
  }

  public String getName() {
    return javaClass.getName();
  }

  public PersistentProperty getIdentity() {
    return this.identity;
  }

  public Class<T> getJavaClass() {
    return this.javaClass;
  }

  public boolean isInstance(Object obj) {
    return getJavaClass().isInstance(obj);
  }

  public List<PersistentProperty> getPersistentProperties() {
    return persistentProperties;
  }

  public List<Association> getAssociations() {
    return associations;
  }

  public PersistentProperty getPropertyByName(String name) {
    return propertiesByName.get(name);
  }

  @Override
  public int hashCode() {
    return javaClass.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof PersistentEntity)) return false;
    if (this == o) return true;

    PersistentEntity other = (PersistentEntity) o;
    return javaClass.equals(other.getJavaClass());
  }

  @Override
  public String toString() {
    return "PersistentEntity[" + javaClass.getName() + "]";
  }
}
