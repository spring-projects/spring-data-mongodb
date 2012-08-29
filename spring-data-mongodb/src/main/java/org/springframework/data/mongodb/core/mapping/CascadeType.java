package org.springframework.data.mongodb.core.mapping;

/**
 * Used to define cascade type in {@link DBRef} annotation
 *
 * @author Maciej Walkowiak
 */
public enum CascadeType {
	/**
	 * Includes {@link CascadeType#SAVE} amd {@link CascadeType#DELETE}
	 */
	ALL,

	/**
	 * Cascades save operation
	 */
	SAVE,

	/**
	 * Cascades delete operation
	 */
	DELETE,

	/**
	 * Default option. No operation is cascaded
	 */
	NONE;

	public boolean isSave() {
		return this == ALL || this == SAVE;
	}

	public boolean isDelete() {
		return this == ALL || this == DELETE;
	}
}
