package org.springframework.data.mongodb.repository;

/**
 * Annotation used by {@link ReadPreference} for define {@link com.mongodb.Tag}
 *
 * @author Jorge Rodríguez
 * @since 4.2
 */
public @interface ReadPreferenceTag {

	/**
	 * Set the name of tag
	 * @return name of tag
	 */
	String name();

	/**
	 * Set the value of tag
	 * @return value of tag
	 */
	String value();
}
