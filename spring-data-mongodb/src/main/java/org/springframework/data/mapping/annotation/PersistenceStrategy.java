package org.springframework.data.mapping.annotation;

/**
 * @author J. Brisbin <jbrisbin@vmware.com>
 */
public enum PersistenceStrategy {
	DEFAULT,
	KEY_VALUE,
	DOCUMENT,
	SERIALIZED
}
