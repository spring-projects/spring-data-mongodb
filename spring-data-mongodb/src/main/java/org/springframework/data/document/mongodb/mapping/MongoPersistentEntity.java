package org.springframework.data.document.mongodb.mapping;

import org.springframework.data.mapping.model.PersistentEntity;

/**
 *
 * @author Oliver Gierke
 */
public interface MongoPersistentEntity<T> extends PersistentEntity<T> {

  String getCollection();
}
