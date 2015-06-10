package org.springframework.data.mongodb.core.mapping.event;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 *
 * Call after the document is saved in datastore
 *
 * @author Regis Leray
 *
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface OnAfterSave {
}
