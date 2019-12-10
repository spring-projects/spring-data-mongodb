/*
 * Copyright 2019 the original author or authors.
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

import org.junit.jupiter.api.Tag;

/**
 * @author Christoph Strobl
 * @since 3.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
@Documented
@Tag("version-specific")
public @interface EnableIfMongoServerVersion {

	/**
	 * Inclusive lower bound of MongoDB server range.
	 *
	 * @return {@code 0.0.0} by default.
	 */
	String isGreaterThanEqual() default "0.0.0";

	/**
	 * Exclusive upper bound of MongoDB server range.
	 *
	 * @return {@code 9999.9999.9999} by default.
	 */
	String isLessThan() default "9999.9999.9999";
}
