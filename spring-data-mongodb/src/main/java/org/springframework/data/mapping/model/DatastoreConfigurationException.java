package org.springframework.data.mapping.model;

/**
 * Exception thrown when something goes wrong configuring a datastore
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public class DatastoreConfigurationException extends RuntimeException{

    public DatastoreConfigurationException(String message) {
        super(message);
    }

    public DatastoreConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
