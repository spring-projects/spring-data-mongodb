package org.springframework.data.mongodb.repository;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.lang.annotation.Retention;

import org.springframework.data.mongodb.core.geo.Distance;

/**
 * Annotation to be used for disambiguing method parameters that shall be used to trigger geo near queries. By default
 * those parameters are found without the need for additional annotation if they are the only parameters of the
 * according type (e.g. {@link Point}, {@code double[]}, {@link Distance}).
 * 
 * @author Oliver Gierke
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Near {

}
