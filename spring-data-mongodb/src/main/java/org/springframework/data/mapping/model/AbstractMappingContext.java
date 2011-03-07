/* Copyright (C) 2010 SpringSource
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

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.validation.Validator;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Abstract implementation of the MappingContext interface
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public abstract class AbstractMappingContext implements MappingContext {

  protected Collection<PersistentEntity> persistentEntities = new ConcurrentLinkedQueue<PersistentEntity>();
  protected Map<String, PersistentEntity> persistentEntitiesByName = new ConcurrentHashMap<String, PersistentEntity>();
  protected Map<PersistentEntity, Map<String, PersistentEntity>> persistentEntitiesByDiscriminator = new ConcurrentHashMap<PersistentEntity, Map<String, PersistentEntity>>();
  protected Map<PersistentEntity, Validator> entityValidators = new ConcurrentHashMap<PersistentEntity, Validator>();
  protected Collection<Listener> eventListeners = new ConcurrentLinkedQueue<Listener>();
  protected GenericConversionService conversionService = new GenericConversionService();

  public ConversionService getConversionService() {
    return conversionService;
  }

  public ConverterRegistry getConverterRegistry() {
    return conversionService;
  }

  public void addMappingContextListener(Listener listener) {
    if (listener != null)
      eventListeners.add(listener);
  }

  public void addTypeConverter(Converter converter) {
    conversionService.addConverter(converter);
  }

  public Validator getEntityValidator(PersistentEntity entity) {
    if (entity != null) {
      return entityValidators.get(entity);
    }
    return null;
  }

  public void addEntityValidator(PersistentEntity entity, Validator validator) {
    if (entity != null && validator != null) {
      entityValidators.put(entity, validator);
    }
  }

  public PersistentEntity addExternalPersistentEntity(Class javaClass) {
    if (javaClass == null) throw new IllegalArgumentException("PersistentEntity class cannot be null");

    PersistentEntity entity = persistentEntitiesByName.get(javaClass.getName());

    if (entity == null) {
      entity = addPersistentEntityInternal(javaClass, true);
    }

    return entity;
  }

  public final PersistentEntity addPersistentEntity(Class javaClass) {
    if (javaClass == null) throw new IllegalArgumentException("PersistentEntity class cannot be null");

    PersistentEntity entity = persistentEntitiesByName.get(javaClass.getName());

    if (entity == null) {
      entity = addPersistentEntityInternal(javaClass, false);
    }

    return entity;
  }

  private PersistentEntity addPersistentEntityInternal(Class javaClass, boolean isExternal) {
    PersistentEntity entity;
    entity = createPersistentEntity(javaClass);
    entity.setExternal(isExternal);

    persistentEntities.remove(entity);
    persistentEntities.add(entity);
    persistentEntitiesByName.put(entity.getName(), entity);
    try {
      entity.afterPropertiesSet();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error initializing PersistentEntity: %s", entity), e);
    }
    if (!entity.isRoot()) {
      PersistentEntity root = entity.getRootEntity();
      Map<String, PersistentEntity> children = persistentEntitiesByDiscriminator.get(root);
      if (children == null) {
        children = new ConcurrentHashMap<String, PersistentEntity>();
        persistentEntitiesByDiscriminator.put(root, children);
      }
      children.put(entity.getDiscriminator(), entity);
    }
    for (Listener eventListener : eventListeners) {
      eventListener.persistentEntityAdded(entity);
    }
    return entity;
  }

  public PersistentEntity getChildEntityByDiscriminator(PersistentEntity root, String discriminator) {
    final Map<String, PersistentEntity> children = persistentEntitiesByDiscriminator.get(root);
    if (children != null) {
      return children.get(discriminator);
    }
    return null;
  }

  protected abstract PersistentEntity createPersistentEntity(Class javaClass);

  public Collection<PersistentEntity> getPersistentEntities() {
    return persistentEntities;
  }

  public boolean isPersistentEntity(Class type) {
    return type != null && getPersistentEntity(type.getName()) != null;

  }

  public boolean isPersistentEntity(Object value) {
    return value != null && isPersistentEntity(value.getClass());
  }

  public PersistentEntity getPersistentEntity(String name) {
    final int proxyIndicator = name.indexOf("_$$_");
    if (proxyIndicator > -1) {
      name = name.substring(0, proxyIndicator);
    }

    return persistentEntitiesByName.get(name);
  }
}
