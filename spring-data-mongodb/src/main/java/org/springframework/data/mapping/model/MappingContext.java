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

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.validation.Validator;

import java.util.Collection;

/**
 * <p>This interface defines the overall context including all known
 * PersistentEntity instances and methods to obtain instances on demand</p>
 * <p/>
 * <p>This interface is used internally to establish associations
 * between entities and also at runtime to obtain entities by name</p>
 * <p/>
 * <p>The generic type parameters T & R are used to specify the
 * mapped form of a class (example Table) and property (example Column) respectively.</p>
 * <p/>
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public interface MappingContext {


  /**
   * Obtains a list of PersistentEntity instances
   *
   * @return A list of PersistentEntity instances
   */
  Collection<PersistentEntity> getPersistentEntities();

  /**
   * Obtains a PersistentEntity by name
   *
   * @param name The name of the entity
   * @return The entity or null
   */
  PersistentEntity getPersistentEntity(String name);

  /**
   * Obtains a child of the given root entity using the given discriminator
   *
   * @param root          The root entity
   * @param discriminator The discriminator
   * @return The child entity or null if non exists
   */
  PersistentEntity getChildEntityByDiscriminator(PersistentEntity root, String discriminator);

  /**
   * Adds a PersistentEntity instance
   *
   * @param javaClass The Java class representing the entity
   * @return The PersistentEntity instance
   */
  PersistentEntity addPersistentEntity(Class javaClass);

  /**
   * Adds a persistent entity that is not mapped by this MappingContext instance.
   * Used for cross store persistence
   *
   * @param javaClass The Java class
   * @return The persistent entity
   */
  PersistentEntity addExternalPersistentEntity(Class javaClass);

  /**
   * Adds a validator to be used by the entity for validation
   *
   * @param entity    The PersistentEntity
   * @param validator The validator
   */
  void addEntityValidator(PersistentEntity entity, Validator validator);

  /**
   * Add a converter used to convert property values to and from the datastore
   *
   * @param converter The converter to add
   */
  void addTypeConverter(Converter converter);

  /**
   * Obtains the ConversionService instance to use for type conversion
   *
   * @return The conversion service instance
   */
  ConversionService getConversionService();

  /**
   * Obtains the converter registry
   *
   * @return The converter registry used for type conversion
   */
  ConverterRegistry getConverterRegistry();

  /**
   * Obtains a validator for the given entity
   *
   * @param entity The entity
   * @return A validator or null if none exists for the given entity
   */
  Validator getEntityValidator(PersistentEntity entity);

  /**
   * Returns the syntax reader used to interpret the entity
   * mapping syntax
   *
   * @return The SyntaxReader
   */
  MappingConfigurationStrategy getMappingSyntaxStrategy();

  /**
   * Obtains the MappingFactory instance
   *
   * @return The mapping factory instance
   */
  MappingFactory getMappingFactory();

  /**
   * Returns whether the specified class is a persistent entity
   *
   * @param type The type to check
   * @return True if it is
   */
  boolean isPersistentEntity(Class<?> type);

  /**
   * Returns whether the specified value is a persistent entity
   *
   * @param value The value to check
   * @return True if it is
   */
  boolean isPersistentEntity(Object value);

  /**
   * Adds a new mapping context listener instance
   *
   * @param listener The listener
   */
  void addMappingContextListener(Listener listener);

  /**
   * Implementors can register for events when the mapping context changes
   */
  public static interface Listener {

    /**
     * Fired when a new entity is added
     *
     * @param entity The entity
     */
    void persistentEntityAdded(PersistentEntity entity);
  }
}
