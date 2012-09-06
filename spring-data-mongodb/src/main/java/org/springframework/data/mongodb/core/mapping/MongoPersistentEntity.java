package org.springframework.data.mongodb.core.mapping;

import org.springframework.data.mapping.PersistentEntity;

/**
 * 
 * @author Oliver Gierke
 */
public interface MongoPersistentEntity<T> extends PersistentEntity<T, MongoPersistentProperty> {

	String getCollection();
	
	MongoPersistentProperty getVersionProperty();
	
	boolean hasVersion();
}
