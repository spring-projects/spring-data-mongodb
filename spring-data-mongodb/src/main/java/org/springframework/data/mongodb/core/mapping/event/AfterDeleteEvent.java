package org.springframework.data.mongodb.core.mapping.event;

/**
 * Event invoked after document is deleted with methods:
 * <ul>
 *     <li>{@link org.springframework.data.mongodb.core.MongoOperations#remove(Object)}</li>
 *     <li>{@link org.springframework.data.mongodb.core.MongoOperations#remove(Object, String)}</li>
 * </ul>
 *
 * @param <T> removed object
 *
 * @author Maciej Walkowiak
 */
public class AfterDeleteEvent<T> extends MongoMappingEvent<T> {
	public AfterDeleteEvent(T source) {
		super(source, null);
	}
}
