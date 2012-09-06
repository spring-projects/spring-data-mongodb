package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Specifies the version field that serves as its optimistic lock value.
 *  
 */
@Documented
@Target({ FIELD })
@Retention(RUNTIME)
public @interface Version {

}
