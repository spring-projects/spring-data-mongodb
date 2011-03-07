package org.springframework.data.mapping.model;

/**
 * A class mapping is a mapping between a class and some external
 * form such as a table, column family, or document (depending on the underlying data store)
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public interface ClassMapping<T> {
  /**
   * Obtains the PersistentEntity for this class mapping
   *
   * @return The PersistentEntity
   */
  PersistentEntity getEntity();

  /**
   * Returns the mapped form of the class such as a Table, a Key Space, Document etc.
   *
   * @return The mapped representation
   */
  T getMappedForm();

  /**
   * Returns details of the identifier used for this class
   *
   * @return The Identity
   */
  IdentityMapping getIdentifier();
}
