/*
 * Copyright 2014-2019 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.QueryAnnotation;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.6
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
@QueryAnnotation
public @interface Meta {

	/**
	 * Set the maximum time limit in milliseconds for processing operations.
	 *
	 * @return
	 * @since 1.10
	 */
	long maxExecutionTimeMs() default -1;

	/**
	 * Sets the number of documents to return per batch. <br />
	 * Use {@literal 0 (zero)} for no limit. A <strong>negative limit</strong> closes the cursor after returning a single
	 * batch indicating to the server that the client will not ask for a subsequent one.
	 * 
	 * @return {@literal 0 (zero)} by default.
	 * @since 2.1
	 */
	int cursorBatchSize() default 0;

	/**
	 * Add a comment to the query.
	 *
	 * @return
	 */
	String comment() default "";

	/**
	 * Set {@link org.springframework.data.mongodb.core.query.Meta.CursorOption} to be used when executing query.
	 *
	 * @return never {@literal null}.
	 * @since 1.10
	 */
	org.springframework.data.mongodb.core.query.Meta.CursorOption[] flags() default {};

}
