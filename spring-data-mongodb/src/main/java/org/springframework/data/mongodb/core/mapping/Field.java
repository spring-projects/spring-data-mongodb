/*
 * Copyright 2011-2021 the original author or authors.
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

import org.springframework.core.annotation.AliasFor;

/**
 * Annotation to define custom metadata for document fields.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
public @interface Field {

	/**
	 * Enumeration of write strategies for a field with null value.It decides whether a field with null value has to be
	 * written to the resulting document to be saved to the database.
	 */
	enum Write{
		/*
		 * The field will always be written to the database irrespective of null value.
		 */
		ALWAYS,
		/*
		 * The field will only be written to the database if it has a non null value.
		 */
		NON_NULL
	}

	/**
	 * The key to be used to store the field inside the document. Alias for {@link #name()}.
	 *
	 * @return an empty {@link String} by default.
	 */
	@AliasFor("name")
	String value() default "";

	/**
	 * The key to be used to store the field inside the document. Alias for {@link #value()}.
	 *
	 * @return an empty {@link String} by default.
	 * @since 2.2
	 */
	@AliasFor("value")
	String name() default "";

	/**
	 * The order in which various fields shall be stored. Has to be a positive integer.
	 *
	 * @return the order the field shall have in the document or -1 if undefined.
	 */
	int order() default Integer.MAX_VALUE;

	/**
	 * The actual desired target type the field should be stored as.
	 *
	 * @return {@link FieldType#IMPLICIT} by default.
	 * @since 2.2
	 */
	FieldType targetType() default FieldType.IMPLICIT;

	/**
	 * If set to {@link Write#NON_NULL} {@literal null} values will be omitted.
	 * Setting the value to {@link Write#ALWAYS} explicitly adds an entry for the given field
	 * holding {@literal null} as a value {@code 'fieldName' : null }.
	 * <p />
	 * <strong>NOTE</strong> Setting the value to {@link Write#ALWAYS} may lead to increased document size.
	 * @return {@link Write#NON_NULL} by default.
	 */
	Write write() default Write.NON_NULL;
}
