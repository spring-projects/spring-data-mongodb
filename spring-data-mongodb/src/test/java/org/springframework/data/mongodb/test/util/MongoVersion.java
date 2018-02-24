/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * {@link MongoVersion} allows specifying an version range of mongodb that is applicable for a specific test method. To
 * be used along with {@link MongoVersionRule}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface MongoVersion {

	/**
	 * Inclusive lower bound of MongoDB server range.
	 *
	 * @return {@code 0.0.0} by default.
	 */
	String asOf() default "0.0.0";

	/**
	 * Exclusive upper bound of MongoDB server range.
	 *
	 * @return {@code 9999.9999.9999} by default.
	 */
	String until() default "9999.9999.9999";
}
