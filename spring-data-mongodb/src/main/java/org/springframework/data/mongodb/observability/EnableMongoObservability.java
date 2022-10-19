package org.springframework.data.mongodb.observability;

import java.lang.annotation.*;

import org.springframework.context.annotation.Import;

/**
 * Annotation to active Spring Data MongoDB's usage of Micrometer's Observation API.
 */
@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(MongoMetricsConfiguration.class)
public @interface EnableMongoObservability {
}
