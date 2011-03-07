package org.springframework.data.mapping.model;

/**
 * A marker interface for a property mapping which specifies
 * what or where a particular property is mapped to
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public interface PropertyMapping<T> {

    /**
     * Retrieves the ClassMapping instance of the owning class
     *
     * @return The ClassMapping instance
     */
    ClassMapping getClassMapping();

    /**
     * Returns the mapped form of the property such as a Column, a Key/Value pair, attribute etc.
     * @return The mapped representation
     */
    T getMappedForm();

}
