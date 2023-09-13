package org.springframework.data.mongodb.repository;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to declare read preference for repository and query.
 *
 * @author Jorge Rodríguez
 * @since 4.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
public @interface ReadPreference {

	/**
	 * Configure read preference mode
	 * @return read preference mode
	 */
	String value() default "";

	/**
	 * Set read preference tags
	 * @return read preference tags
	 */
	ReadPreferenceTag[] tags() default {};

	/**
	 * Set read preference maxStalenessSeconds
	 * @return read preference maxStalenessSeconds
	 */
	long maxStalenessSeconds() default -1;
}

