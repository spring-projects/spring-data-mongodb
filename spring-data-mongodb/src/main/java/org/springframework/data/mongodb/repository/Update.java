/*
 * Copyright 2022-2023 the original author or authors.
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
 * Annotation to declare update operators directly on repository methods. Both attributes allow using a placeholder
 * notation of {@code ?0}, {@code ?1} and so on. The update will be applied to documents matching the either method name
 * derived or annotated query, but not to any custom implementation methods.
 *
 * @author Christoph Strobl
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
public @interface Update {

	/**
	 * Takes a MongoDB JSON string to define the actual update to be executed.
	 *
	 * @return the MongoDB JSON string representation of the update. Empty string by default.
	 * @see #update()
	 */
	@AliasFor("update")
	String value() default "";

	/**
	 * Takes a MongoDB JSON string to define the actual update to be executed.
	 *
	 * @return the MongoDB JSON string representation of the update. Empty string by default.
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/tutorial/update-documents/">https://docs.mongodb.com/manual/tutorial/update-documents/</a>
	 */
	@AliasFor("value")
	String update() default "";

	/**
	 * Takes a MongoDB JSON string representation of an aggregation pipeline to define the update stages to be executed.
	 * <p>
	 * This allows to e.g. define update statement that can evaluate conditionals based on a field value, etc.
	 *
	 * @return the MongoDB JSON string representation of the update pipeline. Empty array by default.
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/tutorial/update-documents-with-aggregation-pipeline/">https://docs.mongodb.com/manual/tutorial/update-documents-with-aggregation-pipeline</a>
	 */
	String[] pipeline() default {};
}
