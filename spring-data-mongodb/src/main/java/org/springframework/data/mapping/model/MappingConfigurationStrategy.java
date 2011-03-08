package org.springframework.data.mapping.model;

import org.springframework.data.document.mongodb.mapping.MappingException;

import java.util.List;
import java.util.Set;

/**
 * <p>This interface defines a strategy for reading how
 * persistent properties are defined in a persistent entity.</p>
 * <p/>
 * <p>Subclasses can implement this interface in order to provide
 * a different mechanism for mapping entities such as annotations or XML.</p>
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public interface MappingConfigurationStrategy {

  /**
   * Tests whether the given class is a persistent entity
   *
   * @param javaClass The java class
   * @return true if it is a persistent entity
   */
  <T> boolean isPersistentEntity(Class<T> javaClass);

  /**
   * @see #getPersistentProperties(Class, MappingContext, ClassMapping)
   */
  <T> List<PersistentProperty> getPersistentProperties(Class<T> javaClass, MappingContext context);

  /**
   * Obtains a List of PersistentProperty instances for the given Mapped class
   *
   * @param javaClass The Java class
   * @param context   The MappingContext instance
   * @param mapping   The mapping for this class
   * @return The PersistentProperty instances
   */
  <T> List<PersistentProperty> getPersistentProperties(Class<T> javaClass, MappingContext context, ClassMapping mapping);

  /**
   * Obtains the identity of a persistent entity
   *
   * @param javaClass The Java class
   * @param context   The MappingContext
   * @return A PersistentProperty instance
   */
  <T> PersistentProperty getIdentity(Class<T> javaClass, MappingContext context);

  /**
   * Obtains the default manner in which identifiers are mapped. In GORM
   * this is just using a property called 'id', but in other frameworks this
   * may differ. For example JPA expects an annotated @Id property
   *
   * @param classMapping The ClassMapping instance
   * @return The default identifier mapping
   */
  IdentityMapping getDefaultIdentityMapping(ClassMapping classMapping) throws MappingException;

  /**
   * Returns a set of entities that "own" the given entity. Ownership
   * dictates default cascade strategies. So if entity A owns entity B
   * then saves, updates and deletes will cascade from A to B
   *
   * @param javaClass The Java class
   * @param context   The MappingContext
   * @return A Set of owning classes
   */
  <T> Set getOwningEntities(Class<T> javaClass, MappingContext context);
}
