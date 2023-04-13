/*
 * Copyright 2023 the original author or authors.
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

import org.springframework.core.annotation.AliasFor;

/**
 * Annotation to declare index hints for repository query, update and aggregate operations. The index is specified by
 * its name.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
public @interface Hint {

	/**
	 * The name of the index to use. In case of an {@literal aggregation} the index is evaluated against the initial
	 * collection or view.
	 *
	 * @return the index name.
	 */
	String value() default "";

	/**
	 * The name of the index to use. In case of an {@literal aggregation} the index is evaluated against the initial
	 * collection or view.
	 *
	 * @return the index name.
	 */
	@AliasFor("value")
	String indexName() default "";
}
