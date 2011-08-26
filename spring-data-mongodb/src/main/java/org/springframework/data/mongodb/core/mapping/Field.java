package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to define custom metadata for document fields.
 * 
 * @author Oliver Gierke
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Field {

	/**
	 * The key to be used to store the field inside the document.
	 * 
	 * @return
	 */
	String value() default "";

	/**
	 * The order in which various fields shall be stored. Has to be a positive integer.
	 * 
	 * @return the order the field shall have in the document or -1 if undefined.
	 */
	int order() default Integer.MAX_VALUE;
}
