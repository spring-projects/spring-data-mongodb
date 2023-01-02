/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Annotation to inject {@link org.springframework.data.mongodb.core.MongoOperations} and
 * {@link org.springframework.data.mongodb.core.ReactiveMongoOperations} parameters as method arguments and into
 * {@code static} fields.
 *
 * @author Christoph Strobl
 * @since 3.0
 * @see MongoTemplateExtension
 */
@Target({ ElementType.FIELD, ElementType.PARAMETER })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ExtendWith(MongoTemplateExtension.class)
public @interface Template {

	/**
	 * @return name of the database to use. Use empty String to generate the database name for the
	 *         {@link ExtensionContext#getTestClass() test class}.
	 */
	String database() default "";

	/**
	 * Pre-initialize the {@link org.springframework.data.mapping.context.MappingContext} with the given entities.
	 *
	 * @return empty by default.
	 */
	Class<?>[] initialEntitySet() default {};

	/**
	 * Use a {@link ReplSetClient} if {@literal true}.
	 *
	 * @return false by default.
	 */
	boolean replicaSet() default false;
}
