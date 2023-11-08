/*
 * Copyright 2011-2023 the original author or authors.
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
import org.springframework.data.mongodb.core.mapping.FieldName.Type;

/**
 * Annotation to define custom metadata for document fields.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Divya Srivastava
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
public @interface Field {

	/**
	 * The key to be used to store the field inside the document. Alias for {@link #name()}.
	 *
	 * @return an empty {@link String} by default.
	 * @see #name()
	 */
	@AliasFor("name")
	String value() default "";

	/**
	 * The key to be used to store the field inside the document. Alias for {@link #value()}. The name may contain MongoDB
	 * special characters like dot ({@literal .}). In this case the name is by default treated as a {@link Type#PATH
	 * path}. To preserve dots within the name set the {@link #nameType()} attribute to {@link Type#KEY}.
	 *
	 * @return an empty {@link String} by default.
	 * @since 2.2
	 */
	@AliasFor("value")
	String name() default "";

	/**
	 * The used {@link Type type} has impact on how a given {@link #name()} is treated if it contains dot ({@literal .})
	 * characters.
	 *
	 * @return {@link Type#PATH} by default.
	 * @since 4.2
	 */
	Type nameType() default Type.PATH;

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
	 * Write rules when to include a property value upon conversion. If set to {@link Write#NON_NULL} (default)
	 * {@literal null} values are not written to the target {@code Document}. Setting the value to {@link Write#ALWAYS}
	 * explicitly adds an entry for the given field holding {@literal null} as a value {@code 'fieldName' : null }. <br />
	 * <strong>NOTE:</strong> Setting the value to {@link Write#ALWAYS} may lead to increased document size.
	 *
	 * @return {@link Write#NON_NULL} by default.
	 * @since 3.3
	 */
	Write write() default Write.NON_NULL;

	/**
	 * Enumeration of write strategies to define when a property is included for write conversion.
	 *
	 * @since 3.3
	 */
	enum Write {

		/**
		 * Value that indicates that property is to be always included, independent of value of the property.
		 */
		ALWAYS,

		/**
		 * Value that indicates that only properties with non-{@literal null} values are to be included.
		 */
		NON_NULL
	}
}
