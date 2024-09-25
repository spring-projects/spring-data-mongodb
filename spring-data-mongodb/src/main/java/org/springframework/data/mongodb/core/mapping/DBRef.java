/*
 * Copyright 2011-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.Reference;

/**
 * An annotation that indicates the annotated field is to be stored using a {@link com.mongodb.DBRef}.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
@Reference
public @interface DBRef {

	/**
	 * The database the referred entity resides in.
	 *
	 * @return empty String by default.
	 */
	String db() default "";

	/**
	 * Controls whether the referenced entity should be loaded lazily. This defaults to {@literal false}.
	 *
	 * @return {@literal false} by default.
	 */
	boolean lazy() default false;
}
