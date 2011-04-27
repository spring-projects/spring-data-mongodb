package org.springframework.data.document.mongodb.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import org.springframework.data.mapping.AbstractMappingContext;
import org.springframework.data.mapping.BasicPersistentEntity;
import org.springframework.data.mapping.AbstractPersistentProperty;
import org.springframework.data.util.TypeInformation;

/**
 *
 * @author Oliver Gierke
 */
public class SimpleMongoMappingContext extends AbstractMappingContext<SimpleMongoMappingContext.SimpleMongoPersistentEntity<?>, SimpleMongoMappingContext.SimplePersistentProperty> {

  /* (non-Javadoc)
   * @see org.springframework.data.mapping.BasicMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation)
   */
  @Override
  @SuppressWarnings("rawtypes")
  protected SimpleMongoPersistentEntity<?> createPersistentEntity(TypeInformation typeInformation) {
    return new SimpleMongoPersistentEntity(typeInformation);
  }

  /* (non-Javadoc)
   * @see org.springframework.data.mapping.BasicMappingContext#createPersistentProperty(java.lang.reflect.Field, java.beans.PropertyDescriptor, org.springframework.data.util.TypeInformation, org.springframework.data.mapping.BasicPersistentEntity)
   */
  @Override
  protected SimplePersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor, SimpleMongoPersistentEntity<?> owner) {
    return new SimplePersistentProperty(field, descriptor, owner);
  }
  
  static class SimplePersistentProperty extends AbstractPersistentProperty {
    
    private static final List<String> ID_FIELD_NAMES = Arrays.asList("id", "_id");

    /**
     * Creates a new {@link SimplePersistentProperty}.
     * 
     * @param field
     * @param propertyDescriptor
     * @param information
     */
    public SimplePersistentProperty(Field field, PropertyDescriptor propertyDescriptor, SimpleMongoPersistentEntity<?> owner) {
      super(field, propertyDescriptor, owner);
    }
    
    /* (non-Javadoc)
     * @see org.springframework.data.mapping.BasicPersistentProperty#isIdProperty()
     */
    public boolean isIdProperty() {
      return ID_FIELD_NAMES.contains(field.getName());
    }
  }
  
  static class SimpleMongoPersistentEntity<T> extends BasicPersistentEntity<T> implements MongoPersistentEntity<T> {

    /**
     * @param information
     */
    public SimpleMongoPersistentEntity(TypeInformation information) {
      super(information);
    }

    /* (non-Javadoc)
     * @see org.springframework.data.document.mongodb.mapping.MongoPersistentEntity#getCollection()
     */
    public String getCollection() {
      return type.getSimpleName();
    }
  }
}
