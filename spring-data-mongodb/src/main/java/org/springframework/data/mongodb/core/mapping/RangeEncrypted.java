/*
 * Copyright 2025 the original author or authors.
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;

/**
 * @author Christoph Strobl
 * @author Ross Lawley
 * @since 4.5
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Encrypted(algorithm = "Range")
@Queryable(queryType = "range")
public @interface RangeEncrypted {

	/**
	 * Set the contention factor.
	 *
	 * @return the contention factor
	 */
	@AliasFor(annotation = Queryable.class, value = "contentionFactor")
	long contentionFactor() default -1;

	/**
	 * Set the {@literal range} options.
	 * <p>
	 * Should be valid extended {@link org.bson.Document#parse(String) JSON} representing the range options and including
	 * the following values: {@code min}, {@code max}, {@code trimFactor} and {@code sparsity}.
	 * <p>
	 * Please note that values are data type sensitive and may require proper identification via eg. {@code $numberLong}.
	 *
	 * @return the {@link org.bson.Document#parse(String) JSON} representation of range options.
	 */
	@AliasFor(annotation = Queryable.class, value = "queryAttributes")
	String rangeOptions() default "";

}
