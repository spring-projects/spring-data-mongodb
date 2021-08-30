/*
 * Copyright 2021. the original author or authors.
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

/*
 * Copyright 2021 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;

/**
 * @author Christoph Strobl
 * @since 2021/08
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.ANNOTATION_TYPE })
@Encrypted
@Field
public @interface EncryptedField {

	/**
	 * The key to be used to store the field inside the document. Alias for {@link #name()}.
	 *
	 * @return an empty {@link String} by default.
	 */
	@AliasFor(annotation = Field.class, attribute = "value")
	String value() default "";

	/**
	 * The key to be used to store the field inside the document. Alias for {@link #value()}.
	 *
	 * @return an empty {@link String} by default.
	 * @since 2.2
	 */
	@AliasFor(annotation = Field.class, attribute = "name")
	String name() default "";

	/**
	 * The order in which various fields shall be stored. Has to be a positive integer.
	 *
	 * @return the order the field shall have in the document or -1 if undefined.
	 */
	@AliasFor(annotation = Field.class, attribute = "order")
	int order() default Integer.MAX_VALUE;

	/**
	 * The actual desired target type the field should be stored as.
	 *
	 * @return {@link FieldType#IMPLICIT} by default.
	 * @since 2.2
	 */
	@AliasFor(annotation = Field.class, attribute = "targetType")
	FieldType targetType() default FieldType.IMPLICIT;

	@AliasFor(annotation = Encrypted.class, attribute = "keyId")
	String[] keyId() default {};

	@AliasFor(annotation = Encrypted.class, attribute = "algorithm")
	String algorithm() default "";
}
